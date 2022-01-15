package qtv

import (
	"encoding/binary"
	"errors"
	"fmt"
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

// mvdhidden_paused_duration(msec) embedded in dem_multiple(0) packet
// used to tell us how much elapsed time has gone by when paused
func isPausedTimeMessage(b []byte) (isPausedTime bool, elapsedTime byte) {
	// expect buffer length 7 (more correct to not demand this but these packets are special case)
	// <int: length> == 3, <short: type> == mvdhidden_paused_duration, <byte: elapsed_time> (returned)
	if len(b) == 7 && binary.LittleEndian.Uint32(b[:4]) == 3 && binary.LittleEndian.Uint16(b[4:6]) == mvdhidden_paused_duration {
		return true, b[6]
	}
	return false, 0
}

// Return non zero if we have at least one message, milliseconds contains how long our buffer in milliseconds.
func consistantMVD(b []byte, isElapsedTime bool) (available int, milliseconds int) {
	for {
		done, discard, packetTime := consistantMVDMessage(b, isElapsedTime)
		if done {
			return available, milliseconds
		}
		available += discard
		milliseconds += int(packetTime)
		b = b[discard:]
	}
}

func consistantMVDMessage(b []byte, isElapsedTime bool) (done bool, discard int, packetTime byte) {
	if len(b) < 2 {
		return true, 0, 0
	}

	packetTime = b[0]
	messageType := mvdMsgType(b[1]) & mvdMsgMask

	if !isKnownMvdMsg(messageType) {
		return true, 0, 0 // Corrupted MVD stream.
	}

	hiddenMessage := false
	if messageType == mvdMsgMulti && isElapsedTime {
		if len(b) < 6 {
			return true, 0, 0
		}
		hiddenMessage = (binary.LittleEndian.Uint32(b[2:6]) == 0)
	}

	// If streaming from server, use the elapsed time when paused rather than gametime.
	if hiddenMessage {
		if len(b) < 13 {
			return true, 0, 0
		}

		if isPausedTime, elapsedTime := isPausedTimeMessage(b[6 : 6+7]); isPausedTime {
			// We're streaming, use elapsed time when paused
			packetTime = elapsedTime
		}
	}

	var (
		length    int
		lengthOfs int
	)

	switch messageType {
	case mvdMsgSet: // dem_set is something really dated, it used in old MVD demos, should not be in TCP stream nowdays (unless streaming old file).
		length = 10

		if len(b) < length {
			return true, 0, 0
		}

		// Not done yet, skip 'length' bytes at the caller.
		return false, length, packetTime

	case mvdMsgMulti:
		lengthOfs = 6
	default:
		lengthOfs = 2
	}

	if len(b) < lengthOfs+4 {
		// The size parameter doesn't fit.
		return true, 0, 0
	}

	length = int(binary.LittleEndian.Uint32(b[lengthOfs : lengthOfs+4]))

	if length > maxMvdSize {
		return true, 0, 0 // Corrupted MVD stream.
	}

	if len(b) < length+lengthOfs+4 {
		return true, 0, 0
	}

	// Make length to be the length of the entire packet.
	length = length + lengthOfs + 4

	// Not done yet, skip 'length' bytes at the caller.
	return false, length, packetTime
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

func (us *uStream) guessPlaybackSpeed(b []byte) (demoSpeed float64, isLowLatency bool) {
	// Use 100% demo speed, because buffer adjustment works badly in demo case.
	// Auto adjustment works badly because we always have too much or too less data in buffer in demo(mvd file) case.
	if us.isDemo() {
		return 1, false
	}

	// {
	// 	_, ms := consistantMVD(b, false)
	// 	log.Trace().Str("ctx", "uStream").Uint32("id", us.id).Msgf("sec: %.3f", float64(ms)/1000)
	// }

	expectedDelay := us.expectedIngameDelay()
	if expectedDelay == 0 {
		return 1, true // Low latency.
	}

	if us.getState() != usActive {
		return 1, false // We are not ready, so use 100% demoSpeed.
	}

	isElapsedTime := (us.qp.extFlagsMVD1 & mvdPext1HiddenMessages) != 0
	_, ms := consistantMVD(b, isElapsedTime)

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
	defer func() { err = multierror.Prefix(err, "uStream.parseMVD:") }()

	b := us.rb.BytesOneReader()
	totalDiscard := 0

	demoSpeed, isLowLatency := us.guessPlaybackSpeed(b)

	for isLowLatency || us.curTime >= us.parseTime {
		done, discard, packetTime, err := us.parseMVDMessage(b, demoSpeed, isLowLatency)
		if err != nil {
			return err
		}
		totalDiscard += discard

		if done {
			// If IO error detected and nothing to parse - then we have to reconnect.
			if us.ioError && totalDiscard == 0 {
				return errors.New("IO error detected and input buffer is empty")
			}
			break
		}
		// We're about to destroy this data, so it had better be forwarded by now!
		b[0] = packetTime // Adjust packet time according to our demospeed.
		if err := us.sendMVDDataDown(b[:discard]); err != nil {
			return err
		}
		b = b[discard:]
	}

	return us.rb.Discard(totalDiscard)
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

func (us *uStream) parseMVDMessage(b []byte, demoSpeed float64, isLowLatency bool) (done bool, discard int, packetTime byte, err error) {
	defer func() { err = multierror.Prefix(err, "uStream.parseMVDMessage:") }()

	if len(b) < 2 {
		us.tryBuffering()
		return true, 0, 0, nil
	}

	packetTime = b[0]
	messageType := mvdMsgType(b[1]) & mvdMsgMask

	if !isKnownMvdMsg(messageType) {
		return true, 0, 0, errors.New("corrupted MVD stream")
	}

	hiddenMessage := false
	if messageType == mvdMsgMulti && (us.qp.extFlagsMVD1&mvdPext1HiddenMessages) != 0 {
		if len(b) < 6 {
			us.tryBuffering()
			return true, 0, 0, nil
		}
		hiddenMessage = (binary.LittleEndian.Uint32(b[2:6]) == 0)
	}

	// If streaming from server, use the elapsed time when paused rather than gametime.
	if hiddenMessage && us.isTCP() {
		if len(b) < 13 {
			us.tryBuffering()
			return true, 0, 0, nil
		}

		if isPausedTime, elapsedTime := isPausedTimeMessage(b[6 : 6+7]); isPausedTime {
			// We're streaming, use elapsed time when paused
			packetTime = elapsedTime
		}
	}

	packetTime = byte(float64(packetTime) / demoSpeed)
	// We will set parseTime to nextPacketTime after fully packet parse. Do NOT set it right now!!!
	nextPacketTime := us.parseTime + uint64(packetTime)

	var (
		length    int
		lengthOfs int
	)

	switch messageType {
	case mvdMsgSet: // mvdMsgSet is something really dated, it used in old MVD demos, should not be in TCP stream nowdays (unless streaming old file).
		length = 10

		if len(b) < length {
			us.tryBuffering()
			return true, 0, 0, nil
		}

		// We parsed packet successfully, advance parseTime.
		us.parseTime = nextPacketTime
		// Not done yet, discard 'length' bytes at the caller.
		return false, length, packetTime, nil

	case mvdMsgMulti:
		lengthOfs = 6
	default:
		lengthOfs = 2
	}

	// The size parameter doesn't fit.
	if len(b) < lengthOfs+4 {
		us.tryBuffering()
		return true, 0, 0, nil
	}

	length = int(binary.LittleEndian.Uint32(b[lengthOfs : lengthOfs+4]))

	if length > maxMvdSize {
		return true, 0, 0, errors.New("corrupted MVD stream")
	}

	if len(b) < length+lengthOfs+4 {
		us.tryBuffering()
		return true, 0, 0, nil
	}

	// Read actual message.
	mb := b[lengthOfs+4:]

	switch messageType {
	case mvdMsgMulti:
		playersMask := binary.LittleEndian.Uint32(b[2 : 2+4])
		if playersMask != 0 || (us.qp.extFlagsMVD1&mvdPext1HiddenMessages) == 0 {
			if err := us.qp.readMessage(mb[:length], messageType, playersMask); err != nil {
				return true, 0, 0, err
			}
		}
	case mvdMsgSingle, mvdMsgStats:
		playernum := (b[1] >> 3) // These are directed to a single player.
		playersMask := uint32(1 << playernum)
		if err := us.qp.readMessage(mb[:length], messageType, playersMask); err != nil {
			return true, 0, 0, err
		}
	case mvdMsgRead, mvdMsgAll:
		if err := us.qp.readMessage(mb[:length], messageType, playersMaskAll); err != nil {
			return true, 0, 0, err
		}
	default:
		return true, 0, 0, fmt.Errorf("unexpected messageType %v", messageType)
	}

	// Make length to be the length of the entire packet.
	length = length + lengthOfs + 4

	// We parsed packet successfully, advance parseTime.
	us.parseTime = nextPacketTime
	// Not done yet, discard 'length' bytes at the caller.
	return false, length, packetTime, nil
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

	if us.getState() != usActive || ds.download.IsActive() {
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
	if err := us.qp.sendList(ds, us.qp.soundList[:], svc_soundlist); err != nil {
		return err
	}
	if err := us.qp.sendList(ds, us.qp.modelList[:], svc_modellist); err != nil {
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

	return ds.setState(dsActive)
}
