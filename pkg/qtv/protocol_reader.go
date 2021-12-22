package qtv

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/rs/zerolog/log"
)

//
// QW protocol reader (deserializer).
//

var (
	nullEntityState entityState
)

func (qp *qProtocol) readServerData() (err error) {
	defer func() { err = multierror.Prefix(err, "qProtocol.readServerData:") }()

	if err := qp.us.setState(usParsingConnection); err != nil {
		return err
	}

	for {
		protocol := qp.r.GetUint32()
		switch protocol {
		case protocolVersionFTE:
			qp.extFlagsFTE1 = qp.r.GetUint32()
			continue
		case protocolVersionFTE2:
			qp.extFlagsFTE2 = qp.r.GetUint32()
			continue
		case protocolVersionMVD1:
			qp.extFlagsMVD1 = qp.r.GetUint32()
			continue
		case protocolVersion:
			// Nothing.
		default:
			return fmt.Errorf("invalid QW protocol number %X", protocol)
		}
		break
	}

	qp.r.floatCoord = ((qp.extFlagsFTE1 & ftePextFloatCoords) != 0)
	qp.w.floatCoord = qp.r.floatCoord

	// server count, we ignore it and use local one since remote could be the same all the time (for example for demo file).
	qp.r.GetUint32()

	qp.gameDir = qp.r.GetString()

	qp.r.GetFloat32() // server time

	qp.mapName = qp.r.GetString()

	// Get the movevars.
	qp.moveVars.gravity = qp.r.GetFloat32()
	qp.moveVars.stopSpeed = qp.r.GetFloat32()
	qp.moveVars.maxSpeed = qp.r.GetFloat32()
	qp.moveVars.spectatorMaxSpeed = qp.r.GetFloat32()
	qp.moveVars.accelerate = qp.r.GetFloat32()
	qp.moveVars.airAccelerate = qp.r.GetFloat32()
	qp.moveVars.waterAccelerate = qp.r.GetFloat32()
	qp.moveVars.friction = qp.r.GetFloat32()
	qp.moveVars.waterFriction = qp.r.GetFloat32()
	qp.moveVars.entgrav = qp.r.GetFloat32()

	log.Trace().Str("ctx", "qProtocol").Str("event", "serverData").
		Uint32("extFTE1", qp.extFlagsFTE1).Uint32("extFTE2", qp.extFlagsFTE2).Uint32("extMVD1", qp.extFlagsMVD1).
		Str("map", qp.mapName).Uint32("svCount", qp.us.serverCount).Str("gameDir", qp.us.gameDir()).Msg("")

	// Tell downstreams with ezQuake extensions to reconnect.
	qp.us.forceReconnectLinkedDownstreams(false)

	return nil
}

func (qp *qProtocol) readCDTrack() error {
	qp.cdTrack = qp.r.GetByte()
	return nil
}

func (qp *qProtocol) readStuffText() (err error) {
	defer func() { err = multierror.Prefix(err, "qProtocol.readStuffText:") }()

	// This one is for reducing memory allocations.
	b := qp.r.GetTmpBytes()
	// If you does not use 's' outside of this function it will not escape to the heap allocation and will be allocated on stack. (GOOD)
	// strings.HasPrefix() is inlined therefore 's' also does not escape to the heap allocation.
	// WARNING: If you want to use 's' outside of this function please check how it is done for fullserverinfo in this function.
	s := string(b)

	switch {
	case s == "skins\n":
		if err := qp.us.setState(usActive); err != nil {
			return err
		}
	case strings.HasPrefix(s, "cmd "):
		// Commands the game server asked for are pointless.
	case s == "bf\n":
		// Ignore it.
	case strings.HasPrefix(s, "//at"):
		// Ignore it.
	case strings.HasPrefix(s, "//wps"):
		// Ignore it.
	case strings.HasPrefix(s, "//sn"):
		// Ignore it.
	case strings.HasPrefix(s, "//ktx"):
		// Ignore it.
	case strings.HasPrefix(s, "play "):
		// Ignore it.
	case strings.HasPrefix(s, "changing"):
		// Ignore it.
	case strings.HasPrefix(s, "reconnect"):
		// Ignore it.
	case strings.HasPrefix(s, "packet "):
		// Ignore it.
	case strings.HasPrefix(s, "fullserverinfo "):
		heapStr := string(b) // Heap allocated.
		return qp.readFullServerInfo(heapStr)
	case strings.HasPrefix(s, "//finalscores "):
		heapStr := string(b) // Heap allocated.
		return qp.readLastScores(heapStr)
	case strings.Contains(s, "screenshot"):
		// Ignore it.
	default:
		// Just for debug.
		log.Trace().Str("ctx", "qProtocol").Str("event", "svc_stufftext").Str("value", string(b)).Msg("unknown")
	}

	return nil
}

func (qp *qProtocol) readFullServerInfo(s string) error {
	s = strings.TrimPrefix(s, "fullserverinfo ")

	si := strings.TrimRight(s, "\n")
	si = unquote(si)
	qp.serverInfo.FromStr(si)

	fromProxy := (qp.serverInfo.Get("*qtv") != "")

	// Replace/add our version.
	qp.serverInfo.Setf("*qtv", "%g", qtvVersion)
	// Save remote host name.
	remoteHostName := qp.serverInfo.Get("hostname")
	qp.remoteHostName.Store(remoteHostName)

	//
	// Change the hostname (the qtv's hostname with the server's hostname in brackets).
	//
	localHostName := qp.us.qtv.hostName()
	// Check if we already have brackets.
	bracket := strings.IndexByte(remoteHostName, '(')
	// The from-proxy check is because it's fairly common to find a qw server with brackets after it's name.
	if fromProxy && bracket != -1 && strings.HasSuffix(remoteHostName, ")") {
		// Strip the parent proxy's hostname, and put our hostname first, leaving the original server's hostname within the brackets.
		remoteHostName = fmt.Sprintf("%s %s", localHostName, remoteHostName[bracket:])
	} else {
		tag := "live"
		if qp.us.isDemo() {
			tag = "recorded from"
		}
		remoteHostName = fmt.Sprintf("%s (%s: %s)", localHostName, tag, remoteHostName)
	}
	qp.serverInfo.Set("hostname", remoteHostName)

	return nil
}

func (qp *qProtocol) readLastScores(s string) error {
	s = strings.TrimPrefix(s, "//")

	us := qp.us

	tr, _ := tokenizeString(s)

	ls := lastScores{}
	arg := 1
	ls.matchDate = tr.Argv(arg)
	arg++
	ls.matchType = redText(tr.Argv(arg))
	arg++
	ls.mapName = tr.Argv(arg)
	arg++
	ls.e1 = tr.Argv(arg)
	arg++
	ls.s1 = tr.Argv(arg)
	arg++
	ls.e2 = tr.Argv(arg)
	arg++
	ls.s2 = tr.Argv(arg)

	// Something goes wrong.
	if ls.matchDate == "" {
		return nil
	}

	// Check do we have same lastcores duplicate, ignore it if we have.
	for i := 0; i < maxLastScores; i++ {
		tmpScore := &us.lastScores[i]
		if tmpScore.matchDate == "" {
			continue // Empty entry.
		}

		if *tmpScore == ls {
			// Found duplicate.
			return nil
		}
	}

	// Save new lastscores.
	idx := us.lastScoresIndex % maxLastScores
	us.lastScores[idx] = ls
	us.lastScoresIndex = (idx + 1) % maxLastScores

	return nil
}

func (qp *qProtocol) readSetInfo() error {
	pnum := qp.r.GetByte()
	if pnum >= maxClients {
		return fmt.Errorf("readSetInfo: invalid client index %v", pnum)
	}
	k := qp.r.GetString()
	v := qp.r.GetString()
	qp.players[pnum].userInfo.Set(k, v)
	return nil
}

func (qp *qProtocol) readServerInfo() error {
	k := qp.r.GetString()
	v := qp.r.GetString()

	if k == "status" && strings.EqualFold(v, "standby") {
		qp.matchStartTime = 0
		qp.matchStartLocalTime = 0
	}

	// Don't allow the hostname to change, but allow the server to change other serverinfos.
	if k != "hostname" {
		qp.serverInfo.Set(k, v)
	}

	return nil
}

func (qp *qProtocol) readPrint() error {
	qp.r.GetByte() // level
	qp.r.SkipString()
	return nil
}

func (qp *qProtocol) readCenterPrint() error {
	qp.r.SkipString()
	return nil
}

// For baselines/static entities.
func (qp *qProtocol) readEntityState(es *entityState) {
	es.modelIndex = qp.r.GetByte()
	es.frame = qp.r.GetByte()
	es.colorMap = qp.r.GetByte()
	es.skinNum = qp.r.GetByte()
	for i := 0; i < 3; i++ {
		es.origin[i] = qp.r.GetCoord()
		es.angles[i] = qp.r.GetAngle()
	}
}

func (qp *qProtocol) readBaseLine(delta bool) error {
	if delta {
		if (qp.extFlagsFTE1 & ftePextSpawnStatic2) == 0 {
			return errors.New("readBaseLine: (delta) ftePextSpawnStatic2 flag is not set")
		}
		entNum, flags := qp.readEntityNum()
		if entNum >= maxEntities {
			return errors.New("readBaseline: (delta) maxEntities")
		}
		qp.readEntityDelta(&nullEntityState, &qp.baseLine[entNum], flags, false)
	} else {
		entNum := qp.r.GetUint16()
		if entNum >= maxEntities {
			return errors.New("readBaseline: maxEntities")
		}
		qp.readEntityState(&qp.baseLine[entNum])
	}
	return nil
}

func (qp *qProtocol) readStaticSound() error {
	if qp.staticSoundCount >= maxStaticSounds {
		return errors.New("readStaticSound: maxStaticSounds")
	}

	ss := &qp.staticSound[qp.staticSoundCount]
	ss.origin[0] = qp.r.GetCoord()
	ss.origin[1] = qp.r.GetCoord()
	ss.origin[2] = qp.r.GetCoord()
	ss.soundIndex = qp.r.GetByte()
	ss.volume = qp.r.GetByte()
	ss.attenuation = qp.r.GetByte()
	qp.staticSoundCount++
	return nil
}

func (qp *qProtocol) readInterMission() error {
	qp.r.GetCoord()
	qp.r.GetCoord()
	qp.r.GetCoord()
	qp.r.GetAngle()
	qp.r.GetAngle()
	qp.r.GetAngle()
	return nil
}

func (qp *qProtocol) readSpawnStatic(delta bool) error {
	if delta && (qp.extFlagsFTE1&ftePextSpawnStatic2) == 0 {
		return fmt.Errorf("readSpawnStatic: (delta = %v) ftePextSpawnStatic2 flag is not set", delta)
	}
	if qp.spawnStaticCount >= maxStaticEntities {
		return fmt.Errorf("readSpawnStatic: (delta = %v) maxStaticEntities", delta)
	}

	if delta {
		_, flags := qp.readEntityNum()
		qp.readEntityDelta(&nullEntityState, &qp.spawnStatic[qp.spawnStaticCount], flags, false)
	} else {
		qp.readEntityState(&qp.spawnStatic[qp.spawnStaticCount])
	}
	qp.spawnStaticCount += 1
	return nil
}

func (qp *qProtocol) readPlayerInfo() error {
	pnum := qp.r.GetByte()
	if pnum >= maxClients {
		return fmt.Errorf("readPlayerInfo: invalid client index %v", pnum)
	}

	p := &qp.players[pnum]

	flags := qp.r.GetUint16()
	p.gibbed = ((flags & dfGib) != 0)
	p.dead = ((flags & dfDead) != 0)
	p.frame = qp.r.GetByte()

	for i := 0; i < 3; i++ {
		if (flags & (dfOrigin << i)) != 0 {
			p.origin[i] = qp.r.GetCoord()
		}
	}

	for i := 0; i < 3; i++ {
		if (flags & (dfAngles << i)) != 0 {
			p.angles[i] = qp.r.GetAngle16()
		}
	}

	if (flags & dfModel) != 0 {
		p.modelIndex = qp.r.GetByte()
	}
	if (flags & dfSkinNum) != 0 {
		p.skinNum = qp.r.GetByte()
	}
	if (flags & dfEffects) != 0 {
		p.effects = qp.r.GetByte()
	}
	if (flags & dfWeaponFrame) != 0 {
		p.weaponFrame = qp.r.GetByte()
	}

	return nil
}

func (qp *qProtocol) readEntityNum() (entNum uint, flags uint) {
	flags = uint(qp.r.GetUint16())

	if flags == 0 {
		return 0, 0
	}

	entNum = flags & 511
	flags &^= 511

	if (flags & uMoreBits) != 0 {
		flags |= uint(qp.r.GetByte())
	}

	return entNum, flags
}

func (qp *qProtocol) readEntityDelta(old *entityState, new *entityState, flags uint, forcereLink bool) {
	*new = *old

	if (flags & uModel) != 0 {
		new.modelIndex = qp.r.GetByte()
	}
	if (flags & uFrame) != 0 {
		new.frame = qp.r.GetByte()
	}
	if (flags & uColorMap) != 0 {
		new.colorMap = qp.r.GetByte()
	}
	if (flags & uSkin) != 0 {
		new.skinNum = qp.r.GetByte()
	}
	if (flags & uEffects) != 0 {
		new.effects = qp.r.GetByte()
	}

	if (flags & uOrigin1) != 0 {
		new.origin[0] = qp.r.GetCoord()
	}
	if (flags & uAngle1) != 0 {
		new.angles[0] = qp.r.GetAngle()
	}
	if (flags & uOrigin2) != 0 {
		new.origin[1] = qp.r.GetCoord()
	}
	if (flags & uAngle2) != 0 {
		new.angles[1] = qp.r.GetAngle()
	}
	if (flags & uOrigin3) != 0 {
		new.origin[2] = qp.r.GetCoord()
	}
	if (flags & uAngle3) != 0 {
		new.angles[2] = qp.r.GetAngle()
	}
}

func canExpandFrame(newMax int) bool {
	return newMax < maxDemoPacketEntities
}

func (qp *qProtocol) readPacketEntities(isDelta bool) error {
	if isDelta {
		qp.r.GetByte()
	}

	oldframe := &qp.frame[qp.incomingSequence&(maxEntityFrames-1)]
	oldcount := oldframe.numEnts
	oldindex := 0

	qp.incomingSequence = (qp.incomingSequence + 1) & (maxEntityFrames - 1)
	newframe := &qp.frame[qp.incomingSequence]
	newindex := 0

	for {
		newnum, flags := qp.readEntityNum()
		if newnum == 0 {
			// End of packet, any remaining old ents need to be copied to the new frame.
			for oldindex < oldcount {
				if !canExpandFrame(newindex) {
					break
				}
				newframe.ents[newindex] = oldframe.ents[oldindex]
				newframe.entNums[newindex] = oldframe.entNums[oldindex]
				newindex++
				oldindex++
			}
			break
		}

		var oldnum uint
		if oldindex >= oldcount {
			oldnum = 0xffff
		} else {
			oldnum = uint(oldframe.entNums[oldindex])
		}

		for newnum > oldnum {
			if !canExpandFrame(newindex) {
				break
			}
			newframe.ents[newindex] = oldframe.ents[oldindex]
			newframe.entNums[newindex] = oldframe.entNums[oldindex]
			newindex++
			oldindex++

			if oldindex >= oldcount {
				oldnum = 0xffff
			} else {
				oldnum = uint(oldframe.entNums[oldindex])
			}
		}

		if newnum < oldnum {
			// This ent wasn't in the last packet.
			if (flags & uRemove) != 0 {
				// Remove this ent... just don't copy it across.
				continue
			}
			if !canExpandFrame(newindex) {
				break
			}
			qp.readEntityDelta(&qp.baseLine[newnum], &newframe.ents[newindex], flags, true)
			newframe.entNums[newindex] = uint16(newnum)
			newindex++
		} else if newnum == oldnum {
			if (flags & uRemove) != 0 {
				// Remove this ent... just don't copy it across.
				oldindex++
				continue
			}

			if !canExpandFrame(newindex) {
				break
			}

			qp.readEntityDelta(&oldframe.ents[oldindex], &newframe.ents[newindex], flags, false)
			newframe.entNums[newindex] = uint16(newnum)
			newindex++
			oldindex++
		}
	}

	newframe.numEnts = newindex
	return nil
}

func (qp *qProtocol) readUpdatePing() error {
	pnum := qp.r.GetByte()
	if pnum >= maxClients {
		return fmt.Errorf("readUpdatePing: invalid client index %v", pnum)
	}
	ping := int(qp.r.GetInt16())
	qp.players[pnum].ping = ping
	return nil
}

func (qp *qProtocol) readUpdateFrags() error {
	pnum := qp.r.GetByte()
	if pnum >= maxClients {
		return fmt.Errorf("readUpdateFrags: invalid client index %v", pnum)
	}
	frags := int(qp.r.GetInt16())
	qp.players[pnum].frags = frags
	return nil
}

func (qp *qProtocol) readUpdateStat() error {
	statnum := qp.r.GetByte()
	value := uint32(qp.r.GetByte())

	if statnum >= maxStats {
		log.Debug().Str("ctx", "qProtocol").Str("event", "svc_updatestat").Msg("invalid stat")
		return nil
	}

	for pnum := 0; pnum < maxClients; pnum++ {
		if (qp.playersMask & (1 << pnum)) != 0 {
			qp.players[pnum].stats[statnum] = value
		}
	}
	return nil
}

func (qp *qProtocol) readUpdateStatLong() error {
	statnum := qp.r.GetByte()
	value := uint64(qp.r.GetUint32())

	if statnum >= maxStats {
		log.Debug().Str("ctx", "qProtocol").Str("event", "svc_updatestatlong").Msg("invalid stat")
		return nil
	}

	if statnum == statMatchStartTime && value != qp.matchStartTime {
		qp.matchStartTime = value
		qp.matchStartLocalTime = qp.us.curTime
	}

	for pnum := 0; pnum < maxClients; pnum++ {
		if (qp.playersMask & (1 << pnum)) != 0 {
			qp.players[pnum].stats[statnum] = uint32(value)
		}
	}
	return nil
}

func (qp *qProtocol) readUpdateUserinfo() error {
	pnum := qp.r.GetByte()
	if pnum >= maxClients {
		return fmt.Errorf("readUpdateUserinfo: invalid client index %v", pnum)
	}
	id := qp.r.GetUint32()
	s := qp.r.GetString()
	qp.players[pnum].userId = id
	qp.players[pnum].userInfo.FromStr(s)
	return nil
}

func (qp *qProtocol) readPacketLoss() error {
	pnum := qp.r.GetByte() % maxClients
	if pnum >= maxClients {
		return fmt.Errorf("readPacketLoss: invalid client index %v", pnum)
	}
	value := int(qp.r.GetByte())
	qp.players[pnum].packetLoss = value
	return nil
}

func (qp *qProtocol) readUpdateEnterTime() error {
	pnum := qp.r.GetByte() % maxClients
	if pnum >= maxClients {
		return fmt.Errorf("readUpdateEnterTime: invalid client index %v", pnum)
	}
	value := qp.r.GetFloat32()
	qp.players[pnum].enterTime = float64(value)
	return nil
}

func (qp *qProtocol) readSound() error {
	const (
		SND_VOLUME      = (1 << 15)
		SND_ATTENUATION = (1 << 14)
	)

	channel := qp.r.GetUint16()
	if (channel & SND_VOLUME) != 0 {
		qp.r.GetByte()
	}
	if (channel & SND_ATTENUATION) != 0 {
		qp.r.GetByte()
	}
	qp.r.GetByte() // sound_num
	for i := 0; i < 3; i++ {
		qp.r.GetCoord()
	}
	return nil
}

func (qp *qProtocol) readDamage() error {
	qp.r.GetByte()
	qp.r.GetByte()
	qp.r.GetCoord()
	qp.r.GetCoord()
	qp.r.GetCoord()
	return nil
}

const (
	teSpike          = 0
	teSuperSpike     = 1
	teGunShot        = 2
	teExplosion      = 3
	teTarExplosion   = 4
	teLightning1     = 5
	teLightning2     = 6
	teWizSpike       = 7
	teKnightSpike    = 8
	teLightning3     = 9
	teLavaSplash     = 10
	teTeleport       = 11
	teBlood          = 12
	teLightningBlood = 13
)

func (qp *qProtocol) readTempEntity() error {
	i := qp.r.GetByte()
	switch i {
	case teSpike:
		qp.r.GetCoord()
		qp.r.GetCoord()
		qp.r.GetCoord()
	case teSuperSpike:
		qp.r.GetCoord()
		qp.r.GetCoord()
		qp.r.GetCoord()
	case teGunShot:
		qp.r.GetByte()
		qp.r.GetCoord()
		qp.r.GetCoord()
		qp.r.GetCoord()
	case teExplosion:
		qp.r.GetCoord()
		qp.r.GetCoord()
		qp.r.GetCoord()
	case teTarExplosion:
		qp.r.GetCoord()
		qp.r.GetCoord()
		qp.r.GetCoord()
	case teLightning1, teLightning2, teLightning3:
		qp.r.GetUint16()
		qp.r.GetCoord()
		qp.r.GetCoord()
		qp.r.GetCoord()
		qp.r.GetCoord()
		qp.r.GetCoord()
		qp.r.GetCoord()
	case teWizSpike:
		qp.r.GetCoord()
		qp.r.GetCoord()
		qp.r.GetCoord()
	case teKnightSpike:
		qp.r.GetCoord()
		qp.r.GetCoord()
		qp.r.GetCoord()
	case teLavaSplash:
		qp.r.GetCoord()
		qp.r.GetCoord()
		qp.r.GetCoord()
	case teTeleport:
		qp.r.GetCoord()
		qp.r.GetCoord()
		qp.r.GetCoord()
	case teBlood:
		qp.r.GetByte()
		qp.r.GetCoord()
		qp.r.GetCoord()
		qp.r.GetCoord()
	case teLightningBlood:
		qp.r.GetCoord()
		qp.r.GetCoord()
		qp.r.GetCoord()
	default:
		return fmt.Errorf("readTempEntity: temp entity %v not recognized", i)
	}
	return nil
}

func (qp *qProtocol) readLightStyle() error {
	style := qp.r.GetByte()
	if style >= maxLightStyles {
		return fmt.Errorf("readLightStyle: invalid lightstyle index %v", style)
	}
	qp.lightStyle[style] = qp.r.GetString()
	return nil
}

func (qp *qProtocol) readNails(nails2 bool) error {
	count := qp.r.GetByte()
	for ; count > 0; count -= 1 {
		if nails2 {
			qp.r.GetByte()
		}
		for i := 0; i < 6; i++ {
			qp.r.GetByte()
		}
	}
	return nil
}

func (qp *qProtocol) readDownload() error {
	size := int(qp.r.GetInt16())
	qp.r.GetByte() // percent

	if size < 0 {
		return fmt.Errorf("readDownload: download failed")
	}

	for b := 0; b < size; b++ {
		qp.r.GetByte()
	}
	return nil
}

func (qp *qProtocol) readFTEVoiceChat() error {
	/* sender = */ qp.r.GetByte()
	/* gen = */ qp.r.GetByte()
	/* seq =*/ qp.r.GetByte()
	bytes := int(qp.r.GetInt16())

	if bytes < 0 {
		return fmt.Errorf("readFTEVoiceChat: can't parse")
	}

	for ; bytes > 0; bytes-- {
		qp.r.GetByte()
	}

	return nil
}

func (qp *qProtocol) readParticle() error {
	qp.r.GetCoord()
	qp.r.GetCoord()
	qp.r.GetCoord()
	qp.r.GetByte()
	qp.r.GetByte()
	qp.r.GetByte()
	qp.r.GetByte()
	qp.r.GetByte()
	return nil
}

const (
	endOfDemo = "EndOfDemo" // MVDSV append this at the end of demo/stream.
)

func (qp *qProtocol) readDisconnect() error {
	s := qp.r.GetString()
	if s != endOfDemo {
		log.Debug().Str("ctx", "qProtocol").Str("event", "svc_disconnect").Msgf("non standard disconnect message %q", s)
	}
	return nil
}

func (qp *qProtocol) readSetAngle() error {
	qp.r.GetByte()
	qp.r.GetAngle()
	qp.r.GetAngle()
	qp.r.GetAngle()
	return nil
}

func (qp *qProtocol) readSetPause() error {
	qp.isPaused = (qp.r.GetByte() != 0)
	return nil
}

func (qp *qProtocol) readFinale() error {
	qp.r.SkipString()
	return nil
}

func (qp *qProtocol) readMuzzleFlash() error {
	qp.r.GetUint16()
	return nil
}

func (qp *qProtocol) readChokeCount() error {
	qp.r.GetByte()
	return nil
}

func (qp *qProtocol) readEntGravity() error {
	qp.r.GetFloat32()
	return nil
}

func (qp *qProtocol) readMaxSpeed() error {
	qp.r.GetFloat32()
	return nil
}

func (qp *qProtocol) readList(list []string) (byte, error) {
	first := int(qp.r.GetByte()) + 1
	for ; first < len(list); first++ {
		list[first] = qp.r.GetString()
		if list[first] == "" {
			break
		}
	}

	if first >= len(list) {
		return 0, errors.New("readList: overflow")
	}

	return qp.r.GetByte(), nil
}

func (qp *qProtocol) readModelList() error {
	_, err := qp.readList(qp.modelList[:])
	if err != nil {
		return err
	}

	// HACK for empty map name (mapper mistake).
	qp.fixEmptyMapName()

	return nil
}

// If map doesn't have friendly descriptive name - use basic instead.
func (qp *qProtocol) fixEmptyMapName() {
	m := qp.modelList[1]
	if m != "" && qp.mapName == "" {
		slash := strings.IndexByte(m, '/')
		if slash != -1 {
			m = m[slash+1:]
		}
		qp.mapName = m
	}
}

func (qp *qProtocol) readSoundList() error {
	_, err := qp.readList(qp.soundList[:])
	return err
}

func (qp *qProtocol) readMessage(b []byte, messageType mvdMsgType, playersMask uint32) (err error) {
	defer func() { err = multierror.Prefix(err, "qp.readMessage:") }()
	defer qp.r.Reset(nil, false) // That prevents keeping invalid reference to 'b' after we return.

	qp.r.Reset(b, (qp.extFlagsFTE1&ftePextFloatCoords) != 0)
	qp.messageType = messageType
	qp.playersMask = playersMask

	for qp.r.rPos < len(b) && qp.r.error == false {
		qp.svc = qp.r.GetSVC()
		if err := qp.readMessageForSvc(); err != nil {
			return err
		}
	}

	if qp.r.error {
		return errors.New("parse error: short read")
	}

	return nil
}

func (qp *qProtocol) readMessageForSvc() error {
	switch qp.svc {
	case svc_bad:
		return errors.New("parse error: svc_bad")
	case svc_nop:
		return nil
	case svc_disconnect:
		return qp.readDisconnect()
	case svc_updatestat:
		return qp.readUpdateStat()
	case svc_sound:
		return qp.readSound()
	case svc_print:
		return qp.readPrint()
	case svc_stufftext:
		return qp.readStuffText()
	case svc_setangle:
		return qp.readSetAngle()
	case svc_serverdata:
		return qp.readServerData()
	case svc_lightstyle:
		return qp.readLightStyle()
	case svc_updatefrags:
		return qp.readUpdateFrags()
	case svc_particle:
		return qp.readParticle()
	case svc_damage:
		return qp.readDamage()
	case svc_spawnstatic:
		return qp.readSpawnStatic(false)
	case svc_fte_spawnstatic2:
		return qp.readSpawnStatic(true)
	case svc_spawnbaseline:
		return qp.readBaseLine(false)
	case svc_fte_spawnbaseline2:
		return qp.readBaseLine(true)
	case svc_temp_entity:
		return qp.readTempEntity()
	case svc_setpause:
		return qp.readSetPause()
	case svc_centerprint:
		return qp.readCenterPrint()
	case svc_killedmonster:
		return nil
	case svc_foundsecret:
		return nil
	case svc_spawnstaticsound:
		return qp.readStaticSound()
	case svc_intermission:
		return qp.readInterMission()
	case svc_finale:
		return qp.readFinale()
	case svc_cdtrack:
		return qp.readCDTrack()
	case svc_smallkick:
		return nil
	case svc_bigkick:
		return nil
	case svc_updateping:
		return qp.readUpdatePing()
	case svc_updateentertime:
		return qp.readUpdateEnterTime()
	case svc_updatestatlong:
		return qp.readUpdateStatLong()
	case svc_muzzleflash:
		return qp.readMuzzleFlash()
	case svc_updateuserinfo:
		return qp.readUpdateUserinfo()
	case svc_download:
		return qp.readDownload()
	case svc_playerinfo:
		return qp.readPlayerInfo()
	case svc_nails:
		return qp.readNails(false)
	case svc_chokecount:
		return qp.readChokeCount()
	case svc_modellist:
		return qp.readModelList()
	case svc_soundlist:
		return qp.readSoundList()
	case svc_packetentities:
		return qp.readPacketEntities(false)
	case svc_deltapacketentities:
		return qp.readPacketEntities(true)
	case svc_entgravity:
		return qp.readEntGravity()
	case svc_maxspeed:
		return qp.readMaxSpeed()
	case svc_setinfo:
		return qp.readSetInfo()
	case svc_serverinfo:
		return qp.readServerInfo()
	case svc_updatepl:
		return qp.readPacketLoss()
	case svc_nails2:
		return qp.readNails(true)
	case svc_fte_voicechat:
		return qp.readFTEVoiceChat()
	default:
		return fmt.Errorf("readMessageForSvc: unknown svc %v", qp.svc)
	}
}
