package qtv

import (
	"encoding/binary"
	"errors"
	"math"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/rs/zerolog/log"
)

//
// Upstream and MVD related stuff like parsing and writing.
// With parsing here implied MVD "container" parsing, QW related protocol parsing is outside of the scope of this file.
//

const (
	// qqshka:
	// ezQuake (and FTE?) expects maximum message size is msgBufSize == 8192 with MVD header
	// which does not have fixed size. MVD header max size is 10 bytes (at the moment).
	// In order to be safely processed by majority QW clients we reduce msgBufSize by 100 bytes.
	// Value 100 is completely random, besides it should cover MVD header with huge margin for error
	// and it should allow extending MVD header size in future if required.
	//
	// maxMvdSize - max size of single MVD message WITHOUT MVD header.
	maxMvdSize = (msgBufSize - 100)
)

type mvdMsgType byte

const (
	// mvdMsgCmd    mvdMsgType = 0 // Unused.
	mvdMsgRead   mvdMsgType = 1
	mvdMsgSet    mvdMsgType = 2
	mvdMsgMulti  mvdMsgType = 3
	mvdMsgSingle mvdMsgType = 4
	mvdMsgStats  mvdMsgType = 5
	mvdMsgAll    mvdMsgType = 6
	mvdMsgMask   mvdMsgType = 7
)

func isKnownMvdMsg(messageType mvdMsgType) bool {
	switch messageType {
	case mvdMsgRead, mvdMsgSet, mvdMsgMulti, mvdMsgSingle, mvdMsgStats, mvdMsgAll:
		return true
	default:
		return false
	}
}

const (
	playersMaskAll = math.MaxUint32
)

const (
	dfOrigin      = (1 << 0)
	dfAngles      = (1 << 3)
	dfEffects     = (1 << 6)
	dfSkinNum     = (1 << 7)
	dfDead        = (1 << 8)
	dfGib         = (1 << 9)
	dfWeaponFrame = (1 << 10)
	dfModel       = (1 << 11)
)

// MVD protocol extensions.
const (
	mvdPext1HiddenMessages uint32 = (1 << 5) // dem_multiple(0) packets are in format (<length> <type-id>+ <packet-data>)*
)

// meag:
// hidden messages inserted into .mvd files
// embedded in dem_multiple(0) - should be safely skipped in clients
// format is <int:length> <short:type>*   where <type> is duplicated if 0xFFFF.  <length> is length of the data packet, not the header
const (
	// mvdhidden_antilag_position_header_t mvdhidden_antilag_position_t*
	mvdhidden_antilag_position uint16 = 0x0000
	// <byte: playernum> <byte:dropnum> <byte: msec, vec3_t: angles, short[3]: forward side up> <byte: buttons> <byte: impulse>
	mvdhidden_usercmd uint16 = 0x0001
	// <byte: source playernum> <int: items> <byte[4]: ammo> <byte: result> <byte*: weapon priority (nul terminated)>
	mvdhidden_usercmd_weapons uint16 = 0x0002
	// <short: block#> <byte[] content>
	mvdhidden_demoinfo uint16 = 0x0003
	// <byte: track#> [todo... <byte: audioformat> <string: short-name> <string: author(s)> <float: start-offset>?]
	mvdhidden_commentary_track uint16 = 0x0004
	// <byte: track#> [todo... format-specific]
	mvdhidden_commentary_data uint16 = 0x0005
	// <byte: track#> [todo... <float: duration> <string: text (utf8)>]
	mvdhidden_commentary_text_segment uint16 = 0x0006
	// <byte: type-flags> <short: damaged ent#> <short: damaged ent#> <short: damage>
	mvdhidden_dmgdone uint16 = 0x0007
	// (same format as mvdhidden_usercmd_weapons)
	mvdhidden_usercmd_weapons_ss uint16 = 0x0008
	// <byte: playernum> <byte: flags> <int: sequence#> <int: mode> <byte[10]: weaponlist>
	mvdhidden_usercmd_weapon_instruction uint16 = 0x0009
	// <byte: msec> ... actual time elapsed, not gametime (can be used to keep stream running) ... expected to be QTV only
	mvdhidden_paused_duration uint16 = 0x000A
	// doubt we'll ever get here: read next short...
	mvdhidden_extended uint16 = 0xFFFF
)

// mvdRecordStatus distinguishes a partial tail from invalid container framing.
type mvdRecordStatus byte

const (
	mvdRecordIncomplete mvdRecordStatus = iota
	mvdRecordComplete
	mvdRecordInvalid
)

// mvdRecordHeader describes a record before its timestamp is scaled for playback.
type mvdRecordHeader struct {
	size          int
	payloadOffset int
	playersMask   uint32
	kind          mvdMsgType
	playbackMS    byte // Timestamp used for scheduling before speed adjustment.
}

// quakePayload returns nil for container-only and negotiated hidden records.
func (h mvdRecordHeader) quakePayload(record []byte, hidden bool) []byte {
	if h.kind == mvdMsgSet || (hidden && h.kind == mvdMsgMulti && h.playersMask == 0) {
		return nil
	}
	return record[h.payloadOffset:h.size]
}

// inspectMVDRecord validates framing without advancing or normalizing time.
func inspectMVDRecord(c *byteCursor, elapsed bool) (mvdRecordHeader, mvdRecordStatus) {
	var scratch [13]byte
	if c.left < 2 {
		return mvdRecordHeader{}, mvdRecordIncomplete
	}
	b := c.peek(2, scratch[:])
	h := mvdRecordHeader{kind: mvdMsgType(b[1]) & mvdMsgMask, playbackMS: b[0]}
	if !isKnownMvdMsg(h.kind) {
		return mvdRecordHeader{}, mvdRecordInvalid
	}
	if h.kind == mvdMsgSet {
		if c.left < 10 {
			return mvdRecordHeader{}, mvdRecordIncomplete
		}
		h.size = 10
		return h, mvdRecordComplete
	}
	h.payloadOffset = 6
	if h.kind == mvdMsgMulti {
		h.payloadOffset = 10
	}
	if c.left < h.payloadOffset {
		return mvdRecordHeader{}, mvdRecordIncomplete
	}
	b = c.peek(h.payloadOffset, scratch[:])
	size := binary.LittleEndian.Uint32(b[h.payloadOffset-4:])
	switch h.kind {
	case mvdMsgMulti:
		h.playersMask = binary.LittleEndian.Uint32(b[2:6])
		if elapsed && h.playersMask == 0 {
			// Hidden elapsed-time detection retains its 13-byte lookahead.
			if c.left < 13 {
				return mvdRecordHeader{}, mvdRecordIncomplete
			}
			if size == 3 {
				payload := c.peek(13, scratch[:])[10:]
				if binary.LittleEndian.Uint16(payload[:2]) == mvdhidden_paused_duration {
					h.playbackMS = payload[2]
				}
			}
		}
	case mvdMsgSingle, mvdMsgStats:
		h.playersMask = uint32(1) << (b[1] >> 3)
	default:
		h.playersMask = playersMaskAll
	}
	if size > maxMvdSize {
		return mvdRecordHeader{}, mvdRecordInvalid
	}
	h.size = int(size) + h.payloadOffset
	if c.left < h.size {
		return h, mvdRecordIncomplete
	}
	return h, mvdRecordComplete
}

// consistantMVD reports the complete prefix and its original container time.
func consistantMVD(b []byte, elapsed bool) (available int, milliseconds int64) {
	cur := byteViewOf(b).cursor(0)
	for cur.left > 0 {
		header, state := inspectMVDRecord(&cur, elapsed)
		if state != mvdRecordComplete {
			break
		}
		available += header.size
		milliseconds += int64(header.playbackMS)
		cur.skip(header.size)
	}
	return
}

func (us *uStream) gameInProgress() bool {
	status := us.qp.serverInfo.Get("status")
	return !strings.EqualFold(status, "Standby")
}

func (us *uStream) expectedIngameDelay() float64 {
	if us.isDemo() {
		// We delay only live games to prevent ghosting and that is demo, so no delay.
		return 0
	}

	expectedDelay := us.qtv.qvs.Get("parse_delay").Float
	if us.options.ingameDelay.Present() {
		expectedDelay = us.options.ingameDelay.OrElse(0)
	}
	return bound(0, expectedDelay, 15)
}

func (us *uStream) guessPlaybackSpeed(view byteView) (demoSpeed float64, isLowLatency bool) {
	// Use 100% demo speed, because buffer adjustment works badly in demo case.
	// Auto adjustment works badly because we always have too much or too less data in buffer in demo(mvd file) case.
	if us.isDemo() {
		return 1, false
	}

	expectedDelay := us.expectedIngameDelay()
	if expectedDelay == 0 {
		return 1, true // Low latency.
	}

	if us.getState() != usActive {
		return 1, false // We are not ready, so use 100% demoSpeed.
	}

	isElapsedTime := (us.qp.extFlagsMVD1 & mvdPext1HiddenMessages) != 0
	ms := us.mvdTime.pending(view, isElapsedTime)

	if !us.gameInProgress() {
		expectedDelay = 0.5 // In prewar use short delay.
	}

	// Guess playback speed.
	expectedDelay = bound(0.5, expectedDelay, 20.0) // Bound it to reasonable values.
	currentDelay := 0.001 * float64(ms)

	demoSpeed = currentDelay / expectedDelay

	// Hysteresis, should prevent demoSpeed wobble.
	if demoSpeed >= 0.85 && demoSpeed <= 1.15 {
		demoSpeed = 1.0
	}

	return bound(0.1, demoSpeed, 3.0), false
}

func (us *uStream) parseMVD() (err error) {
	defer func() {
		if err != nil {
			us.mvdTime.reset()
		}
		err = multierror.Prefix(err, "uStream.parseMVD:")
	}()

	view := us.rb.Snapshot()
	defer view.release()
	cur := view.cursor(0)
	totalDiscard := 0
	var totalMS int64
	demoSpeed, isLowLatency := us.guessPlaybackSpeed(view)

	for isLowLatency || us.curTime >= us.parseTime {
		hidden := us.qp.extFlagsMVD1&mvdPext1HiddenMessages != 0
		header, state := inspectMVDRecord(&cur, us.isTCP() && hidden)
		if state == mvdRecordInvalid {
			return errors.New("corrupted MVD stream")
		}
		if state == mvdRecordIncomplete {
			if header.size > us.rb.Capacity() || (totalDiscard == 0 && view.size == us.rb.Capacity()) {
				return errors.New("MVD record exceeds input queue size")
			}
			if us.ioError && totalDiscard == 0 {
				return errors.New("IO error detected and input buffer is empty")
			}
			// Only rebuffer when this pass consumed no complete records.
			if totalDiscard == 0 {
				us.tryBuffering()
			}
			break
		}
		b := cur.read(header.size, &us.mvdScratch)
		payload := header.quakePayload(b, hidden)
		packetTime, err := us.parseMVDMessage(header, payload, demoSpeed)
		if err != nil {
			return err
		}
		totalDiscard += header.size
		totalMS += int64(header.playbackMS)
		b[0] = packetTime
		if err := us.sendMVDDataDown(b); err != nil {
			return err
		}
		cur.skip(header.size)
	}

	if err = us.rb.Discard(totalDiscard); err != nil {
		return err
	}
	us.mvdTime.consume(totalDiscard, totalMS, us.qp.extFlagsMVD1&mvdPext1HiddenMessages != 0)
	return nil
}

// If upstream does not have enough data for streaming then
// we set parseTime few seconds in to the future, we will not attempt to stream data during this time but will try to buffer data,
// hopefully we will get data and could continue streaming normally.
func (us *uStream) tryBuffering() {
	if us.isDemo() {
		return // In case of the local demo file do not try to buffer.
	}

	us.parseTime = us.curTime + 2000
}

// parseMVDMessage applies the framed payload and advances playback time on success.
func (us *uStream) parseMVDMessage(header mvdRecordHeader, payload []byte, demoSpeed float64) (packetTime byte, err error) {
	defer func() { err = multierror.Prefix(err, "uStream.parseMVDMessage:") }()

	packetTime = byte(float64(header.playbackMS) / demoSpeed)
	nextPacketTime := us.parseTime + uint64(packetTime)
	if payload != nil {
		if err := us.qp.readMessage(payload, header.kind, header.playersMask); err != nil {
			return 0, err
		}
	}
	us.parseTime = nextPacketTime
	return packetTime, nil
}

func (us *uStream) sendMVDDataDown(b []byte) (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.sendMVDDataDown:") }()

	us.mu.Lock()
	defer us.mu.Unlock()

	for _, ds := range us.linkedDs {
		if ds.isCanceled() {
			continue // Optimization, do not try write to the canceled stream.
		}

		if ds.getState() == dsNeedInitialData {
			if err := us.sendInitialMVDData(ds); err != nil {
				log.Trace().Err(multierror.Prefix(err, "uStream.sendMVDDataDown:")).
					Str("ctx", "uStream").Uint32("id", uint32(us.id)).Uint32("dsId", uint32(ds.id)).Msg("")
				ds.cancel() // Tell downstream to die.
				continue
			}
		}

		if ds.getState() == dsActive {
			if err := ds.send(b); err != nil {
				log.Trace().Err(multierror.Prefix(err, "uStream.sendMVDDataDown:")).
					Str("ctx", "uStream").Uint32("id", uint32(us.id)).Uint32("dsId", uint32(ds.id)).Msg("")
				ds.cancel() // Tell downstream to die.
				continue
			}
		}
	}

	return nil
}

func (us *uStream) sendInitialMVDData(ds *dStream) (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.sendInitialMVDData:") }()

	if us.getState() != usActive || ds.download.IsActive() ||
		(us.qp.usesRD() && (us.qp.rd == nil || !us.qp.rd.ready)) {
		return nil // Upstream not ready, do it later.
	}

	if (ds.qtvEzQuakeExt & qtvEzQuakeExtDownload) != 0 {
		return us.sendInitialMVDData_EzqExt(ds)
	} else {
		return us.sendInitialMVDData_1_0(ds)
	}
}

func (us *uStream) sendInitialMVDData_EzqExt(ds *dStream) (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.sendInitialMVDData_EzqExt:") }()

	if err := ds.sendInitialUserList(); err != nil {
		return err
	}
	if err := us.qp.sendServerData(ds); err != nil {
		return err
	}
	if err := ds.sendClientAliases(); err != nil {
		return err
	}

	return ds.setState(dsNeedSoundList)
}

func (us *uStream) sendInitialMVDData_1_0(ds *dStream) (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.sendInitialMVDData_1_0:") }()

	if err := us.qp.sendServerData(ds); err != nil {
		return err
	}
	if err := us.qp.sendList(ds, us.qp.soundList[:], svc_soundlist, svc_fte_soundlistshort); err != nil {
		return err
	}
	if err := us.qp.sendList(ds, us.qp.modelList[:], svc_modellist, svc_fte_modellistshort); err != nil {
		return err
	}
	if err := us.qp.sendPreSpawn(ds); err != nil {
		return err
	}
	if err := us.qp.sendPlayers(ds); err != nil {
		return err
	}
	if err := us.qp.sendPlayerStats(ds); err != nil {
		return err
	}
	if err := us.qp.sendEnts(ds); err != nil {
		return err
	}
	if err := us.qp.sendStuffTextf(ds, "skins\n"); err != nil {
		return err
	}
	// Inform downstream if game is paused.
	if us.qp.isPaused {
		if err := us.qp.sendPause(ds); err != nil {
			return err
		}
	}
	// Workaround for ezQuake, otherwise we have invalid coordinates and model index for players.
	if err := us.qp.sendPlaybackDelay(ds, 5); err != nil {
		return err
	}

	return ds.setState(dsActive)
}
