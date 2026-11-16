package qtv

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/qw-group/qtv/pkg/info"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/sha3"
)

const (
	// Classification and authentication share the time since TCP accept.
	reverseHandshakeTimeout = 5 * time.Second
	// Start a separate deadline after REVERSED so a confirmed source must begin streaming.
	reverseBeginTimeout = 5 * time.Second
	// The first viewer header has this cap too because the connection is still unclassified.
	reverseHeaderLimit = 32 * 1024
	reverseAuthMethod  = "HMAC-SHA3-256"
)

// readQTVHeader consumes one text block, leaving subsequent bytes in the
// original reader. The caller shares its byte budget across handshake phases.
func readQTVHeader(r *bufio.Reader, remaining *int) ([]byte, error) {
	var block []byte
	lineStart := 0
	for {
		part, err := r.ReadSlice('\n')
		*remaining -= len(part)
		if *remaining < 0 {
			return nil, errors.New("QTV handshake exceeds size limit")
		}
		block = append(block, part...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			return nil, err
		}
		line := block[lineStart:]
		if bytes.Equal(line, []byte("\n")) || bytes.Equal(line, []byte("\r\n")) {
			return block, nil
		}
		lineStart = len(block)
	}
}

// Reverse-specific headers also identify incomplete or mixed reverse requests so
// they cannot fall through to the viewer parser.
func isReverseRequest(block []byte) bool {
	for line := range bytes.SplitSeq(block, []byte("\n")) {
		key, _, _ := bytes.Cut(line, []byte(":"))
		key = bytes.TrimSpace(key)
		if bytes.Equal(key, []byte("REVERSE")) || bytes.Equal(key, []byte("RESPONSE")) {
			return true
		}
	}
	return false
}

// reverseRequestFields parses both the initial REVERSE and the proof. Reject
// duplicate and unrelated fields so each phase has one unambiguous meaning.
func reverseRequestFields(block []byte) map[string]string {
	lines := strings.Split(strings.TrimSuffix(strings.ReplaceAll(string(block), "\r\n", "\n"), "\n\n"), "\n")
	if lines[0] != "QTV" {
		return nil
	}
	fields := make(map[string]string, 4)
	for _, line := range lines[1:] {
		name, value, hasValue := strings.Cut(line, ":")
		name = strings.TrimSpace(name)
		if _, duplicate := fields[name]; duplicate {
			return nil
		}
		switch name {
		case "REVERSE":
			if hasValue {
				return nil
			}
		case "VERSION", "AUTH", "RESPONSE":
			if !hasValue {
				return nil
			}
		default:
			return nil
		}
		fields[name] = strings.TrimSpace(value)
	}
	if _, ok := fields["REVERSE"]; !ok || fields["VERSION"] != "1" {
		return nil
	}
	return fields
}

// bufferedQTVConn replays the viewer's header and any read-ahead to the normal
// viewer parser while leaving deadlines and close on the original connection.
type bufferedQTVConn struct {
	net.Conn
	reader io.Reader
}

func (c *bufferedQTVConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

// classifyTCP routes a shared QTV TCP connection by its first header. It owns
// the socket and pending slot until handoff so a failed handshake frees both.
func (dss *dStreamStorage) classifyTCP(ctx context.Context, conn net.Conn, writer io.Writer, deadline time.Time) {
	stopCancel := context.AfterFunc(ctx, func() { conn.Close() })
	defer stopCancel()

	handedOff, pending := false, true
	defer func() {
		if !handedOff {
			conn.Close()
		}
		if pending {
			dss.finishPending()
		}
	}()

	if err := conn.SetDeadline(deadline); err != nil {
		return
	}
	reader := bufio.NewReader(conn)
	remaining := reverseHeaderLimit
	first, err := readQTVHeader(reader, &remaining)
	if err != nil {
		return
	}
	if isReverseRequest(first) {
		if us := dss.acceptReverse(conn, reader, first, &remaining); us != nil {
			// Release the slot so an immediate viewer can connect after REVERSED.
			dss.finishPending()
			pending, handedOff = false, true
			us.run()
		}
		return
	}

	// Replay the viewer's request and any following bytes, in order, through
	// the original downstream parser. It owns its normal authentication flow.
	replayed := &bufferedQTVConn{Conn: conn, reader: io.MultiReader(bytes.NewReader(first), reader)}
	dss.mu.Lock()
	defer dss.mu.Unlock()
	dss.pending--
	pending = false
	if dss.closing || ctx.Err() != nil {
		return
	}
	if err := dss.openLocked(replayed, writer); err != nil {
		log.Err(err).Msg("open TCP viewer")
		return
	}
	handedOff = true
}

// acceptReverse authenticates the sender with a fresh challenge on this socket.
// Only a valid proof can claim an upstream slot. The upstream sends REVERSED
// after registration and waits for BEGIN before parsing MVD.
func (dss *dStreamStorage) acceptReverse(conn net.Conn, reader *bufio.Reader, first []byte, remaining *int) *uStream {
	deny := func(message string) *uStream {
		_ = writeQTVHeader(conn, "QTVSV 1\nPERROR: "+message+"\n\n")
		return nil
	}
	if len(reverseRequestFields(first)) != 2 {
		return deny("invalid reverse request")
	}

	secret := dss.qtv.qvs.Get("qtv_reverse_secret").Str
	if len(secret) != 64 {
		return deny("reverse connections disabled")
	}
	key, err := hex.DecodeString(secret)
	if err != nil {
		return deny("reverse connections disabled")
	}
	address, name := dss.qtv.qvs.Get("address").Str, dss.qtv.hostName()
	ui, err := reverseUserInfo(name, address)
	if err != nil {
		return deny("invalid reverse proxy configuration")
	}

	var challenge [32]byte
	if _, err := rand.Read(challenge[:]); err != nil {
		return deny("could not create reverse challenge")
	}
	if err := writeQTVHeader(conn, fmt.Sprintf("QTVSV 1\nAUTH: %s\nCHALLENGE: %x\n\n", reverseAuthMethod, challenge)); err != nil {
		return nil
	}

	second, err := readQTVHeader(reader, remaining)
	if err != nil {
		return nil
	}
	fields := reverseRequestFields(second)
	if len(fields) != 4 || fields["AUTH"] != reverseAuthMethod {
		return deny("invalid reverse response")
	}
	proof, err := hex.DecodeString(fields["RESPONSE"])
	if err != nil || len(proof) != 32 {
		return deny("invalid reverse response")
	}
	mac := hmac.New(sha3.New256, key)
	mac.Write(challenge[:])
	if !hmac.Equal(mac.Sum(nil), proof) {
		return deny("reverse authentication failed")
	}

	us, err := dss.qtv.uss.registerReverse(conn, reader, ui)
	if err != nil {
		return deny("reverse upstream unavailable")
	}
	return us
}

// reverseUserInfo validates the proxy details the game server will advertise
// to spectators. They come from public configuration because the socket
// endpoint may only be reachable from the game server.
func reverseUserInfo(name, address string) (*info.Info, error) {
	// MVDSV rejects ASCII control bytes in reverse USERINFO values.
	isControl := func(r rune) bool { return r < ' ' || r == 0x7f }
	if name == "" || strings.ContainsFunc(name, isControl) {
		return nil, errors.New("invalid reverse proxy name")
	}
	host, port, err := net.SplitHostPort(address)
	number, portErr := strconv.ParseUint(port, 10, 16)
	if err != nil || host == "" || strings.ContainsAny(host, "@/?# ") || strings.ContainsFunc(address, isControl) || portErr != nil || number == 0 {
		return nil, errors.New("invalid reverse proxy address")
	}
	ui := info.NewInfo()
	if err := ui.Set("name", name); err != nil {
		return nil, err
	}
	if err := ui.Set("address", address); err != nil {
		return nil, err
	}
	return ui, nil
}

// writeQTVHeader requires a complete reply before the handshake can advance.
func writeQTVHeader(w io.Writer, header string) error {
	n, err := io.WriteString(w, header)
	if err == nil && n != len(header) {
		return io.ErrShortWrite
	}
	return err
}
