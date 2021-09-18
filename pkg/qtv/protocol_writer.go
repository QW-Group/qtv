package qtv

//
// QW protocol writer (serializer).
// Functions with 'put' prefix puts data inside msg.
// Functions with 'send' prefix sends data to dStream.
//

func (qp *qProtocol) sendServerData(ds *dStream) error {
	msg := qp.w.Clear()

	msg.PutSVC(svc_serverdata)
	if qp.extFlagsFTE1 != 0 {
		msg.PutUint32(protocolVersionFTE)
		msg.PutUint32(qp.extFlagsFTE1)
	}
	if qp.extFlagsFTE2 != 0 {
		msg.PutUint32(protocolVersionFTE2)
		msg.PutUint32(qp.extFlagsFTE2)
	}
	if qp.extFlagsMVD1 != 0 {
		msg.PutUint32(protocolVersionMVD1)
		msg.PutUint32(qp.extFlagsMVD1)
	}
	msg.PutUint32(protocolVersion)
	msg.PutUint32(qp.us.serverCount)

	msg.PutString(qp.us.gameDir())

	msg.PutFloat32(0)
	msg.PutString(qp.mapName)

	msg.PutFloat32(qp.moveVars.gravity)
	msg.PutFloat32(qp.moveVars.stopSpeed)
	msg.PutFloat32(qp.moveVars.maxSpeed)
	msg.PutFloat32(qp.moveVars.spectatorMaxSpeed)
	msg.PutFloat32(qp.moveVars.accelerate)
	msg.PutFloat32(qp.moveVars.airAccelerate)
	msg.PutFloat32(qp.moveVars.waterAccelerate)
	msg.PutFloat32(qp.moveVars.friction)
	msg.PutFloat32(qp.moveVars.waterFriction)
	msg.PutFloat32(qp.moveVars.entgrav)

	qp.putStuffTextf(msg, "fullserverinfo \"%s\"\n", qp.serverInfo.String())

	return ds.sendMVDMessage(msg, mvdMsgRead, playersMaskAll)
}

func (qp *qProtocol) putList(msg *netMsgW, first int, list []string, svc protocolSvc) (i int) {
	msg.PutSVC(svc)
	msg.PutByte(byte(first))

	for i = first + 1; i < len(list); i++ {
		msg.PutString(list[i])
		if list[i] == "" {
			msg.PutByte(0)
			return -1 // No more.
		}

		if msg.wPos > 768 {
			// Truncate.
			i--
			break
		}
	}

	msg.PutByte(0)
	msg.PutByte(byte(i))

	return i
}

func (qp *qProtocol) sendList(ds *dStream, list []string, svc protocolSvc) error {
	for prespawn := 0; prespawn >= 0; {
		msg := qp.w.Clear()
		prespawn = qp.putList(msg, prespawn, list, svc)
		if err := ds.sendMVDMessage(msg, mvdMsgRead, playersMaskAll); err != nil {
			return err
		}
	}
	return nil
}

func (qp *qProtocol) putUserInfos(msg *netMsgW, cursize int, maxbuffersize int, i int) int {
	if i < 0 {
		return i
	}
	if i >= maxClients {
		return i
	}

	for ; i < maxClients; i++ {
		ui := qp.players[i].userInfo.String()

		if msg.wPos+cursize+len(ui) > maxbuffersize {
			return i
		}

		msg.PutSVC(svc_updateuserinfo)
		msg.PutByte(byte(i))
		msg.PutUint32(qp.players[i].userId)
		msg.PutString(ui)

		msg.PutSVC(svc_updatefrags)
		msg.PutByte(byte(i))
		msg.PutUint16(uint16(qp.players[i].frags))

		msg.PutSVC(svc_updateping)
		msg.PutByte(byte(i))
		msg.PutUint16(uint16(qp.players[i].ping))

		msg.PutSVC(svc_updatepl)
		msg.PutByte(byte(i))
		msg.PutByte(byte(qp.players[i].packetLoss))
	}

	return i + 1
}

func (qp *qProtocol) putEntityState(msg *netMsgW, es *entityState) {
	msg.PutByte(es.modelIndex)
	msg.PutByte(es.frame)
	msg.PutByte(es.colorMap)
	msg.PutByte(es.skinNum)
	for i := 0; i < 3; i++ {
		msg.PutCoord(es.origin[i])
		msg.PutAngle(es.angles[i])
	}
}

func (qp *qProtocol) putBaseLines(msg *netMsgW, cursize int, maxbuffersize int, i int) int {
	if i < 0 || i >= maxEntities {
		return i
	}

	for ; i < maxEntities; i++ {
		if msg.wPos+cursize+16 > maxbuffersize {
			return i
		}

		if qp.baseLine[i].modelIndex != 0 {
			msg.PutSVC(svc_spawnbaseline)
			msg.PutUint16(uint16(i))
			qp.putEntityState(msg, &qp.baseLine[i])
		}
	}

	return i
}

func (qp *qProtocol) putLightMaps(msg *netMsgW, cursize int, maxbuffersize int, i int) int {
	if i < 0 || i >= maxLightStyles {
		return i
	}

	for ; i < maxLightStyles; i++ {
		if msg.wPos+cursize+len(qp.lightStyle[i]) > maxbuffersize {
			return i
		}
		msg.PutSVC(svc_lightstyle)
		msg.PutByte(byte(i))
		msg.PutString(qp.lightStyle[i])
	}
	return i
}

func (qp *qProtocol) putStaticSounds(msg *netMsgW, cursize int, maxbuffersize int, i int) int {
	if i < 0 || i >= maxStaticSounds {
		return i
	}

	for ; i < maxStaticSounds; i++ {
		if msg.wPos+cursize+16 > maxbuffersize {
			return i
		}
		if qp.staticSound[i].soundIndex == 0 {
			continue
		}

		msg.PutSVC(svc_spawnstaticsound)
		msg.PutCoord(qp.staticSound[i].origin[0])
		msg.PutCoord(qp.staticSound[i].origin[1])
		msg.PutCoord(qp.staticSound[i].origin[2])
		msg.PutByte(qp.staticSound[i].soundIndex)
		msg.PutByte(qp.staticSound[i].volume)
		msg.PutByte(qp.staticSound[i].attenuation)
	}

	return i
}

func (qp *qProtocol) putStaticEntities(msg *netMsgW, cursize int, maxbuffersize int, i int) int {
	if i < 0 || i >= maxStaticEntities {
		return i
	}

	for ; i < maxStaticEntities; i++ {
		if msg.wPos+cursize+16 > maxbuffersize {
			return i
		}
		if qp.spawnStatic[i].modelIndex == 0 {
			continue
		}

		msg.PutSVC(svc_spawnstatic)
		qp.putEntityState(msg, &qp.spawnStatic[i])
	}

	return i
}

// Returns the next putPreSpawn 'buffer' number to use, or -1 if no more.
func (qp *qProtocol) putPreSpawn(msg *netMsgW, curmsgsize int, bufnum int) int {
	r := bufnum

	ni := qp.putUserInfos(msg, curmsgsize, 768, bufnum)
	r += ni - bufnum
	bufnum = ni
	bufnum -= maxClients

	ni = qp.putBaseLines(msg, curmsgsize, 768, bufnum)
	r += ni - bufnum
	bufnum = ni
	bufnum -= maxEntities

	ni = qp.putLightMaps(msg, curmsgsize, 768, bufnum)
	r += ni - bufnum
	bufnum = ni
	bufnum -= maxLightStyles

	ni = qp.putStaticSounds(msg, curmsgsize, 768, bufnum)
	r += ni - bufnum
	bufnum = ni
	bufnum -= maxStaticSounds

	ni = qp.putStaticEntities(msg, curmsgsize, 768, bufnum)
	r += ni - bufnum
	bufnum = ni
	bufnum -= maxStaticEntities

	if bufnum == 0 {
		return -1
	}

	return r
}

func (qp *qProtocol) sendPreSpawn(ds *dStream) error {
	for prespawn := 0; prespawn >= 0; {
		msg := qp.w.Clear()
		prespawn = qp.putPreSpawn(msg, 0, prespawn)
		if err := ds.sendMVDMessage(msg, mvdMsgRead, playersMaskAll); err != nil {
			return err
		}
	}
	return nil
}

func (qp *qProtocol) putDelta(msg *netMsgW, entnum int, from *entityState, to *entityState, force bool) {
	bits := 0
	if from.angles[0] != to.angles[0] {
		bits |= uAngle1
	}
	if from.angles[1] != to.angles[1] {
		bits |= uAngle2
	}
	if from.angles[2] != to.angles[2] {
		bits |= uAngle3
	}

	if from.origin[0] != to.origin[0] {
		bits |= uOrigin1
	}
	if from.origin[1] != to.origin[1] {
		bits |= uOrigin2
	}
	if from.origin[2] != to.origin[2] {
		bits |= uOrigin3
	}

	if from.colorMap != to.colorMap {
		bits |= uColorMap
	}
	if from.skinNum != to.skinNum {
		bits |= uSkin
	}
	if from.modelIndex != to.modelIndex {
		bits |= uModel
	}
	if from.frame != to.frame {
		bits |= uFrame
	}
	if from.effects != to.effects {
		bits |= uEffects
	}

	if (bits & 255) != 0 {
		bits |= uMoreBits
	}

	if bits == 0 && !force {
		return
	}

	i := (entnum & 511) | (bits &^ 511)
	msg.PutUint16(uint16(i))

	if (bits & uMoreBits) != 0 {
		msg.PutByte(byte(bits & 255))
	}
	if (bits & uModel) != 0 {
		msg.PutByte(to.modelIndex)
	}
	if (bits & uFrame) != 0 {
		msg.PutByte(to.frame)
	}
	if (bits & uColorMap) != 0 {
		msg.PutByte(to.colorMap)
	}
	if (bits & uSkin) != 0 {
		msg.PutByte(to.skinNum)
	}
	if (bits & uEffects) != 0 {
		msg.PutByte(to.effects)
	}
	if (bits & uOrigin1) != 0 {
		msg.PutCoord(to.origin[0])
	}
	if (bits & uAngle1) != 0 {
		msg.PutAngle(to.angles[0])
	}
	if (bits & uOrigin2) != 0 {
		msg.PutCoord(to.origin[1])
	}
	if (bits & uAngle2) != 0 {
		msg.PutAngle(to.angles[1])
	}
	if (bits & uOrigin3) != 0 {
		msg.PutCoord(to.origin[2])
	}
	if (bits & uAngle3) != 0 {
		msg.PutAngle(to.angles[2])
	}
}

func (qp *qProtocol) sendEnts(ds *dStream) error {
	msg := qp.w.Clear()

	msg.PutSVC(svc_packetentities)
	frame := &qp.frame[qp.incomingSequence&(maxEntityFrames-1)]
	for i := 0; i < frame.numEnts; i++ {
		entnum := frame.entNums[i]
		qp.putDelta(msg, int(entnum), &qp.baseLine[entnum], &frame.ents[i], true)
	}
	msg.PutUint16(0)

	return ds.sendMVDMessage(msg, mvdMsgRead, playersMaskAll)
}

func (qp *qProtocol) sendPause(ds *dStream) error {
	// Do not send pause if it not set.
	if !qp.isPaused {
		return nil
	}

	msg := qp.w.Clear()

	msg.PutSVC(svc_setpause)
	msg.PutByte(bool2byte(qp.isPaused))

	return ds.sendMVDMessage(msg, mvdMsgRead, playersMaskAll)
}

func (qp *qProtocol) putStuffTextf(msg *netMsgW, format string, a ...interface{}) {
	if format == "" {
		return
	}
	msg.PutSVC(svc_stufftext)
	msg.PutStringf(format, a...)
}

func (qp *qProtocol) sendStuffTextf(ds *dStream, format string, a ...interface{}) error {
	msg := qp.w.Clear()

	qp.putStuffTextf(msg, format, a...)

	return ds.sendMVDMessage(msg, mvdMsgRead, playersMaskAll)
}

func (qp *qProtocol) sendPlayerStats(ds *dStream) error {
	msg := qp.w.Clear()

	sentTimes := false

	for i := 0; i < maxClients; i++ {
		client := &qp.players[i]
		if !client.isPlayer() {
			continue
		}

		msg.Clear()

		for statNum := 0; statNum < maxStats; statNum++ {
			if client.stats[statNum] != 0 {
				if client.stats[statNum] > 255 {
					msg.PutSVC(svc_updatestatlong)
					msg.PutByte(byte(statNum))
					msg.PutUint32(client.stats[statNum])
				} else {
					msg.PutSVC(svc_updatestat)
					msg.PutByte(byte(statNum))
					msg.PutByte(byte(client.stats[statNum]))
				}
			}
		}

		if msg.WPos() != 0 {
			if qp.matchStartTime != 0 && !sentTimes {
				qp.putMatchTimes(msg) // Hack for STAT_TIME.
				sentTimes = true
			}

			// Send stats for particular player.
			if err := ds.sendMVDMessage(msg, mvdMsgStats|mvdMsgType(i<<3), 0); err != nil {
				return err
			}
		}
	}

	return nil
}

// Hack for STAT_TIME.
func (qp *qProtocol) putMatchTimes(msg *netMsgW) {
	msg.PutSVC(svc_updatestatlong)
	msg.PutByte(statMatchStartTime)
	msg.PutUint32(uint32(qp.matchStartTime))

	msg.PutSVC(svc_updatestatlong)
	msg.PutByte(statTime)
	msg.PutUint32(uint32(qp.matchStartTime + (qp.us.curTime - qp.matchStartLocalTime)))
}

func (qp *qProtocol) sendPlayers(ds *dStream) error {
	msg := qp.w.Clear()

	for i := 0; i < maxClients; i++ {
		client := &qp.players[i]
		if !client.isPlayer() {
			continue
		}

		flags := (dfOrigin << 0) | (dfOrigin << 1) | (dfOrigin << 2) |
			(dfAngles << 0) | (dfAngles << 1) | (dfAngles << 2) |
			dfEffects | dfSkinNum | dfWeaponFrame |
			dfModel // That why we wrote this function.

		if client.dead {
			flags |= dfDead
		}
		if client.gibbed {
			flags |= dfGib
		}

		msg.PutSVC(svc_playerinfo)
		msg.PutByte(byte(i))
		msg.PutUint16(uint16(flags))

		msg.PutByte(client.frame)

		for j := 0; j < 3; j++ {
			if (flags & (dfOrigin << j)) != 0 {
				msg.PutCoord(client.origin[j])
			}
		}
		for j := 0; j < 3; j++ {
			if (flags & (dfAngles << j)) != 0 {
				msg.PutAngle16(client.angles[j])
			}
		}
		if (flags & dfModel) != 0 {
			msg.PutByte(client.modelIndex)
		}
		if (flags & dfSkinNum) != 0 {
			msg.PutByte(client.skinNum)
		}
		if (flags & dfEffects) != 0 {
			msg.PutByte(client.effects)
		}
		if (flags & dfWeaponFrame) != 0 {
			msg.PutByte(client.weaponFrame)
		}
	}

	return ds.sendMVDMessage(msg, mvdMsgRead, playersMaskAll)
}

func (qp *qProtocol) putSvcPrintf(msg *netMsgW, level printLevel, format string, a ...interface{}) {
	msg.PutSVC(svc_print)
	msg.PutByte(byte(level))
	msg.PutStringf(format, a...)
}

func (qp *qProtocol) putLastScores(msg *netMsgW) {
	var (
		cnt                int
		le1, le2, lastType string
	)

	us := qp.us

	for i, idx := 0, us.lastScoresIndex%maxLastScores; i < maxLastScores; i, idx = i+1, (idx+1)%maxLastScores {
		ls := &us.lastScores[idx]
		date := ls.matchDate
		curType := ls.matchType
		mapName := ls.mapName
		e1 := ls.e1
		s1 := ls.s1
		e2 := ls.e2
		s2 := ls.s2

		if date == "" || curType == "" || e1 == "" || e2 == "" {
			continue // Empty entry.
		}

		// If match mode and team roster does not changed - then just print scores and map,
		// if something changed then additionally print header with info.
		if curType != lastType || le1 != e1 || le2 != e2 {
			// [dag vs qqshka] duel
			us.qp.putSvcPrintf(msg, printHigh, "\220%s \366\363 %s\221 %s\n", e1, e2, curType)
		}
		// -5:100 > dm6
		us.qp.putSvcPrintf(msg, printHigh, "  %3s:%-3s \215 %-8.8s %s\n", s1, s2, mapName, date)

		lastType = curType
		le1 = e1
		le2 = e2
		cnt++
	}

	if cnt != 0 {
		entry := "entries"
		if cnt == 1 {
			entry = "entry"
		}
		us.qp.putSvcPrintf(msg, printHigh, "\nLastscores: %d %s found\n", cnt, entry)
	} else {
		us.qp.putSvcPrintf(msg, printHigh, "Lastscores: empty\n")
	}
}
