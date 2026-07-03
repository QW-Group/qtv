package qtv

import (
	"crypto/sha1"
	"crypto/subtle"
	"fmt"
	"math"
	"net"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

//
// Connectionless "rcon" over the existing UDP socket, matching mvdsv's scheme so
// existing QW tooling (ezQuake's `rcon`, rcon utilities) can drive QTV the same
// way they drive a server. This replaces owning QTV's stdin for automation:
//
//   \xff\xff\xff\xff rcon <auth> <command...>
//
// With rcon_crypt 1 (default) <auth> is SHA1("rcon" + " " + password + timestamp
// + " " + args...) as uppercase hex immediately followed by the request's hex
// timestamp — identical to mvdsv (sv_crypt_rcon), so the password never travels
// on the wire, stale requests are rejected by a freshness window, and each
// authenticator is accepted only once (replay cache). With rcon_crypt 0 <auth>
// is the plaintext password (only advisable on a trusted network).
//
// The reply is a connectionless print packet (\xff\xff\xff\xff n <output>) with
// whatever the command wrote to the console, like mvdsv. As on mvdsv, a small set
// of commands is refused over rcon (see rconDenied).
//
// rcon is disabled unless rcon_password is set.
//

const (
	rconDigestHexLen = sha1.Size * 2  // 40 uppercase hex chars
	rconTimeHexLen   = 8 * 2          // mvdsv sends a 64-bit time_t as 16 hex chars
	rconMaxOutput    = msgBufSize - 6 // leave room for the 5-byte connectionless header (+slack)
)

// rconDenied lists commands refused over rcon, by first token (case-insensitive).
// Mirrors the spirit of mvdsv's normal-rcon denylist: no shutting the proxy down,
// no reading arbitrary files via exec, and no reading back or downgrading the
// rcon auth itself (which would let one captured/leaked request persist access).
var rconDenied = map[string]bool{
	"quit":              true,
	"exec":              true,
	"rcon_password":     true,
	"rcon_crypt":        true,
	"rcon_timestamplen": true,
	"rcon_limit":        true,
}

// rconFirstDenied reports the first denied command token in a command string, or
// "" if none are denied. The console executor runs ';'- and newline-separated
// commands (see qCmd.Exec), so every segment must be inspected — checking only
// the first token would let a denied command ride in behind an allowed one, e.g.
// "list; quit". Splitting fails closed: a ';' inside a quoted argument is treated
// as a separator here even though the executor would not, which can only reject
// (never wrongly allow) a request.
func rconFirstDenied(command string) string {
	segments := strings.FieldsFunc(command, func(r rune) bool { return r == ';' || r == '\n' })
	for _, seg := range segments {
		fields := strings.Fields(seg)
		if len(fields) == 0 {
			continue
		}
		if first := strings.ToLower(fields[0]); rconDenied[first] {
			return first
		}
	}
	return ""
}

func (sv *udpSv) regRconVars(qtv *QTV) {
	qtv.qvs.Reg("rcon_password", "")       // empty = rcon disabled
	qtv.qvs.Reg("rcon_crypt", "1")         // 1 = require SHA1+timestamp (like mvdsv sv_crypt_rcon)
	qtv.qvs.Reg("rcon_timestamplen", "60") // seconds a crypted request stays valid
	qtv.qvs.Reg("rcon_limit", "10")        // max BAD rcon attempts per second, per source
}

func (sv *udpSv) rconPassword() string { return sv.qtv.qvs.Get("rcon_password").Str }
func (sv *udpSv) rconCrypt() bool      { return sv.qtv.qvs.Get("rcon_crypt").Bool }
func (sv *udpSv) rconTimestampLen() int {
	return iBound(0, sv.qtv.qvs.Get("rcon_timestamplen").Int, 3600)
}
func (sv *udpSv) rconLimit() int { return sv.qtv.qvs.Get("rcon_limit").Int }

// rconCmd handles a connectionless "rcon" packet: validate, reject replays and
// denied commands, run the command on mainLoop, and reply with its console
// output. Bad authenticators are dropped silently (no reply, so an attacker can't
// tell rcon is enabled and can't use it as an amplifier) and rate-limited per
// source; valid requests are never rate-limited, so bogus traffic can't starve
// legitimate automation.
func (sv *udpSv) rconCmd(addr net.Addr, tr *tokenizerResult) {
	password := sv.rconPassword()
	if password == "" {
		return // rcon not enabled — stay silent, like an unknown command
	}

	ok, replayKey, reqTime := sv.rconValidate(tr, password)
	if !ok {
		if sv.rconFailAllowed(addr) {
			log.Warn().Str("ctx", "udpSv").Str("event", "rcon-bad").Str("remote", addr.String()).Msg("")
		}
		return // silent drop
	}

	// Replay protection (crypt mode only): each authenticator is single-use for
	// its freshness window, so a captured non-idempotent command (close, etc.)
	// cannot be replayed.
	if replayKey != "" && !sv.rconReplayOK(replayKey, reqTime) {
		log.Warn().Str("ctx", "udpSv").Str("event", "rcon-replay").Str("remote", addr.String()).Msg("")
		return
	}

	// Everything from Argv(2) on is the command to run.
	var b strings.Builder
	for i := 2; i < tr.Argc(); i++ {
		if i > 2 {
			b.WriteByte(' ')
		}
		b.WriteString(tr.Argv(i))
	}
	command := strings.TrimSpace(b.String())
	if command == "" {
		sv.rconReply(addr, "")
		return
	}

	if denied := rconFirstDenied(command); denied != "" {
		log.Warn().Str("ctx", "udpSv").Str("event", "rcon-denied").Str("remote", addr.String()).Str("cmd", denied).Msg("")
		sv.rconReply(addr, fmt.Sprintf("command %q is not allowed over rcon\n", denied))
		return
	}

	log.Info().Str("ctx", "udpSv").Str("event", "rcon").Str("remote", addr.String()).Str("cmd", command).Msg("")
	out, ok := sv.qtv.Rcon(command)
	if !ok {
		return // shutting down
	}
	sv.rconReply(addr, out)
}

// rconValidate checks the auth token in Argv(1). On success it returns a replay
// key (the digest) for crypt mode, or "" for plaintext (no replay protection),
// plus the request timestamp (the end of whose freshness window bounds how long
// the replay key must be remembered); reqTime is 0 when there is no timestamp.
func (sv *udpSv) rconValidate(tr *tokenizerResult, password string) (ok bool, replayKey string, reqTime int64) {
	auth := tr.Argv(1)
	if auth == "" {
		return false, "", 0
	}

	if !sv.rconCrypt() {
		return subtle.ConstantTimeCompare([]byte(auth), []byte(password)) == 1, "", 0
	}

	// auth = <40 hex digest><>=16 hex timestamp>. mvdsv requires at least
	// digest+timestamp and parses exactly rconTimeHexLen timestamp chars.
	if len(auth) < rconDigestHexLen+rconTimeHexLen {
		return false, "", 0
	}
	digest := auth[:rconDigestHexLen]
	timeHex := auth[rconDigestHexLen:] // hashed in full, like mvdsv's time_start

	if tl := sv.rconTimestampLen(); tl > 0 {
		t, ok := parseRconTime(timeHex[:rconTimeHexLen])
		if !ok {
			return false, "", 0
		}
		if delta := time.Now().Unix() - t; delta > int64(tl) || delta < -int64(tl) {
			return false, "", 0 // stale or from the future
		}
		reqTime = t
	}

	// SHA1("rcon" + " " + password + timeHex + " " + each arg + " ")
	h := sha1.New()
	h.Write([]byte("rcon "))
	h.Write([]byte(password))
	h.Write([]byte(timeHex))
	h.Write([]byte(" "))
	for i := 2; i < tr.Argc(); i++ {
		h.Write([]byte(tr.Argv(i)))
		h.Write([]byte(" "))
	}
	want := fmt.Sprintf("%X", h.Sum(nil)) // uppercase, exactly like mvdsv
	if subtle.ConstantTimeCompare([]byte(digest), []byte(want)) != 1 {
		return false, "", 0
	}
	return true, digest, reqTime
}

// parseRconTime decodes the little-endian hex time_t that mvdsv clients append,
// e.g. the first hex byte-pair is the least significant byte.
func parseRconTime(hex string) (int64, bool) {
	if len(hex) == 0 || len(hex)%2 != 0 {
		return 0, false
	}
	var t int64
	for i := 0; i < len(hex); i += 2 {
		hi, ok1 := hexNibble(hex[i])
		lo, ok2 := hexNibble(hex[i+1])
		if !ok1 || !ok2 {
			return 0, false
		}
		t += int64(hi<<4|lo) << (uint(i) * 4)
	}
	return t, true
}

func hexNibble(c byte) (int, bool) {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0'), true
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10, true
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10, true
	}
	return 0, false
}

// rconReply sends a connectionless print packet with (bounded) output. It sets a
// fresh write deadline because the serve loop's short (10ms) deadline may have
// elapsed while the command ran on mainLoop, which would otherwise drop the reply.
func (sv *udpSv) rconReply(addr net.Addr, output string) {
	if len(output) > rconMaxOutput {
		output = output[:rconMaxOutput]
	}
	msg := sv.msg.Clear()
	msg.PutUint32(math.MaxUint32) // -1 sequence: out of band
	msg.PutByte(a2c_print[0])
	msg.PutString2f("%s", output)
	if msg.error {
		return
	}
	_ = sv.conn.SetWriteDeadline(time.Now().Add(time.Second))
	sv.conn.WriteTo(msg.Bytes(), addr)
}

// rconFailAllowed rate-limits BAD rcon attempts per source (bounding brute force
// and log spam) without ever throttling valid requests. Returns whether this
// failure is under the per-second budget (used only to gate logging).
func (sv *udpSv) rconFailAllowed(addr net.Addr) bool {
	limit := sv.rconLimit()
	if limit <= 0 {
		return true
	}
	host := hostOf(addr)
	now := time.Now().Unix()

	sv.rconFail.mu.Lock()
	defer sv.rconFail.mu.Unlock()
	if now != sv.rconFail.second {
		sv.rconFail.second = now
		sv.rconFail.perHost = make(map[string]int, len(sv.rconFail.perHost))
	}
	sv.rconFail.perHost[host]++
	return sv.rconFail.perHost[host] <= limit
}

// rconReplayOK records a crypt authenticator and reports whether it is new (not a
// replay). Only valid (authenticated) requests reach here, so the cache cannot be
// flooded by an attacker. Expired entries are swept lazily.
//
// The entry must live until the authenticator can no longer pass validation,
// which is the end of its freshness window (reqTime + tl), not now + tl. When the
// request's timestamp is ahead of our clock (client skew) it is first seen before
// reqTime, so now + tl would expire the entry while the same authenticator is
// still acceptable — reopening a replay gap.
func (sv *udpSv) rconReplayOK(key string, reqTime int64) bool {
	tl := sv.rconTimestampLen()
	if tl <= 0 {
		return true // freshness window disabled → replay protection off
	}
	now := time.Now().Unix()
	exp := reqTime + int64(tl)

	sv.rconSeen.mu.Lock()
	defer sv.rconSeen.mu.Unlock()
	if sv.rconSeen.m == nil {
		sv.rconSeen.m = map[string]int64{}
	}
	// Sweep expired entries when the map has grown.
	if len(sv.rconSeen.m) > 256 {
		for k, e := range sv.rconSeen.m {
			if e <= now {
				delete(sv.rconSeen.m, k)
			}
		}
	}
	if e, ok := sv.rconSeen.m[key]; ok && e > now {
		return false // already used within its window
	}
	sv.rconSeen.m[key] = exp
	return true
}

// hostOf returns the source host (without port) for per-source accounting.
func hostOf(addr net.Addr) string {
	if host, _, err := net.SplitHostPort(addr.String()); err == nil {
		return host
	}
	return addr.String()
}
