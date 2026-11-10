package qtv

import (
	"errors"
	"math"
)

// PutRDUpdateIndex writes one complete 14-bit RD index, including its high byte.
func (msg *netMsgW) PutRDUpdateIndex(entity uint32, remove bool) {
	size := 2
	if entity > rdUpdateIndexMask {
		size++
	}
	if msg.error || entity >= maxRDEntities || len(msg.b)-msg.wPos < size {
		msg.error = true
		return
	}
	word := uint16(entity & rdUpdateIndexMask)
	if remove {
		word |= rdUpdateRemove
	}
	if size == 3 {
		word |= rdUpdateExtend
	}
	msg.PutUint16(word)
	if size == 3 {
		msg.PutByte(byte(entity >> 14))
	}
}

// rdStateBits compares persistent fields; prediction also carries transient values.
func rdStateBits(s, base rdEntityState) uint32 {
	b := rdReset
	if s.origin[0] != base.origin[0] || s.origin[1] != base.origin[1] {
		b |= rdOriginXY
	}
	if s.origin[2] != base.origin[2] {
		b |= rdOriginZ
	}
	if s.angles[0] != base.angles[0] || s.angles[2] != base.angles[2] || s.viewAngles[0] != base.viewAngles[0] || s.viewAngles[2] != base.viewAngles[2] {
		b |= rdAnglesXZ
	}
	if s.angles[1] != base.angles[1] || s.viewAngles[1] != base.viewAngles[1] {
		b |= rdAnglesY
	}
	if s.frame != base.frame {
		b |= rdFrameBit
	}
	if s.effects != base.effects {
		b |= rdEffects
	}
	if s.modelIndex != base.modelIndex {
		b |= rdModel
	}
	if s.modelIndex2 != base.modelIndex2 {
		b |= rdModel2
	}
	if s.skinNum != base.skinNum {
		b |= rdSkin
	}
	if s.colorMap != base.colorMap {
		b |= rdColorMap
	}
	if s.solidSize != base.solidSize {
		b |= rdSolid
	}
	if s.dpFlags != base.dpFlags {
		b |= rdFlags
	}
	if s.trans != base.trans {
		b |= rdAlpha
	}
	if s.scale != base.scale {
		b |= rdScale
	}
	if s.drawFlags != base.drawFlags || s.absLight != base.absLight {
		b |= rdDrawFlags
	}
	if s.tagEntity != base.tagEntity || s.tagIndex != base.tagIndex {
		b |= rdTagInfo
	}
	if s.light != base.light || s.lightStyle != base.lightStyle || s.lightFlags != base.lightFlags {
		b |= rdLight
	}
	if s.trailEffect != base.trailEffect || s.emitEffect != base.emitEffect {
		b |= rdTrailEffect
	}
	if s.colourMod != base.colourMod {
		b |= rdColorMod
	}
	if s.glowSize != base.glowSize || s.glowColour != base.glowColour || s.glowMod != base.glowMod {
		b |= rdGlow
	}
	if s.fatness != base.fatness {
		b |= rdFatness
	}
	if s.gravityDir != base.gravityDir {
		b |= rdGravityDir
	}
	if s.pmoveType != base.pmoveType || s.weaponFrame != base.weaponFrame || s.movement != [3]int16{} || s.velocity != [3]int16{} || s.msec != 0 {
		b |= rdPredInfo
	}
	// A prediction-encoded angle may matter even when all prediction values are zero.
	if b&rdAnglesXZ != 0 && s.viewAngles[0] != rdPitch(s.angles[0], false) {
		b |= rdPredInfo
	}
	return b
}

// rdPitch reproduces the view-pitch basis associated with base UF_PREDINFO.
func rdPitch(angle float32, pred bool) float32 {
	angle = -angle
	if pred {
		angle *= 3
	}
	return float32(int64(angle*65536/360)&65535) * (360.0 / 65536)
}

// rdFineAngle reports values that cannot survive the ordinary byte-angle form.
func rdFineAngle(v float32) bool {
	return v != float32(int64(math.Round(float64(v)*256/360)))*(360.0/256)
}

// rdPayloadBits also selects 16-bit angles when the ordinary primitives are narrow.
func (qp *qProtocol) rdPayloadBits(s, base rdEntityState) uint32 {
	b := rdStateBits(s, base)
	if !qp.w.floatCoord && (b&rdAnglesY != 0 && rdFineAngle(s.angles[1]) || b&rdAnglesXZ != 0 && (rdFineAngle(s.angles[0]) || rdFineAngle(s.angles[2]))) {
		b |= rdPredInfo
	}
	return b
}

// rdPredictionTail preserves a plain pitch basis while restoring prediction and
// fine yaw in a later message that does not modify pitch/roll.
func (qp *qProtocol) rdPredictionTail(s, base rdEntityState) bool {
	return qp.rdPayloadBits(s, base)&rdPredInfo != 0 && s.viewAngles[0] != rdPitch(s.angles[0], true)
}

// putRDPayload emits a complete reset payload relative to a retained baseline.
func (qp *qProtocol) putRDPayload(msg *netMsgW, s, base rdEntityState) error {
	bits := qp.rdPayloadBits(s, base)
	if qp.rdPredictionTail(s, base) {
		bits &^= rdPredInfo
		if !qp.w.floatCoord && rdFineAngle(s.angles[1]) {
			bits &^= rdAnglesY
		}
	}
	return qp.putRDFields(msg, s, bits)
}

// putRDFields serializes the selected base-profile values in FTE wire order.
func (qp *qProtocol) putRDFields(msg *netMsgW, s rdEntityState, b uint32) error {
	if s.number >= maxRDEntities || s.tagEntity >= maxRDEntities || s.weaponFrame > 32767 || s.trailEffect > rdEffectIndexMask || s.emitEffect > rdEffectIndexMask {
		return errors.New("invalid retained RD state")
	}
	if s.frame > 255 || s.modelIndex > 255 || s.modelIndex2 > 255 || s.skinNum < 0 || s.skinNum > 255 {
		b |= rdWide
	}
	if b&rdEffects != 0 {
		if s.effects > 65535 {
			b |= rdEffects2
		} else if s.effects > 255 {
			b = (b &^ rdEffects) | rdEffects2
		}
	}
	if b&0xff000000 != 0 {
		b |= rdExtend3
	}
	if b&0x00ff0000 != 0 {
		b |= rdExtend2
	}
	if b&0x0000ff00 != 0 {
		b |= rdExtend1
	}
	msg.PutByte(byte(b))
	if b&rdExtend1 != 0 {
		msg.PutByte(byte(b >> 8))
	}
	if b&rdExtend2 != 0 {
		msg.PutByte(byte(b >> 16))
	}
	if b&rdExtend3 != 0 {
		msg.PutByte(byte(b >> 24))
	}
	wide := func(v uint16) {
		if b&rdWide != 0 {
			msg.PutUint16(v)
		} else {
			msg.PutByte(byte(v))
		}
	}
	coord := func(v float32) {
		if qp.rdFloatCoords() {
			msg.PutFloat32(v)
		} else {
			msg.PutCoord(v)
		}
	}
	angle := func(v float32) {
		if b&rdPredInfo != 0 || msg.floatCoord {
			msg.PutUint16(uint16(int64(math.Round(float64(v) * 65536 / 360))))
		} else {
			msg.PutByte(byte(int64(math.Round(float64(v) * 256 / 360))))
		}
	}
	if b&rdFrameBit != 0 {
		wide(s.frame)
	}
	if b&rdOriginXY != 0 {
		coord(s.origin[0])
		coord(s.origin[1])
	}
	if b&rdOriginZ != 0 {
		coord(s.origin[2])
	}
	if b&rdAnglesXZ != 0 {
		angle(s.angles[0])
		angle(s.angles[2])
	}
	if b&rdAnglesY != 0 {
		angle(s.angles[1])
	}
	switch b & (rdEffects | rdEffects2) {
	case rdEffects | rdEffects2:
		msg.PutUint32(s.effects)
	case rdEffects2:
		msg.PutUint16(uint16(s.effects))
	case rdEffects:
		msg.PutByte(byte(s.effects))
	}
	if b&rdPredInfo != 0 {
		pred := rdPredMoveType | rdPredWeaponFrameOld
		for i := range 3 {
			if s.movement[i] != 0 {
				pred |= rdPredForward << i
			}
		}
		if s.velocity[0] != 0 || s.velocity[1] != 0 {
			pred |= rdPredVelocityXY
		}
		if s.velocity[2] != 0 {
			pred |= rdPredVelocityZ
		}
		if s.msec != 0 {
			pred |= rdPredMsec
		}
		msg.PutByte(pred)
		for i := range 3 {
			if pred&(rdPredForward<<i) != 0 {
				msg.PutUint16(uint16(s.movement[i]))
			}
		}
		msg.PutByte(s.pmoveType)
		if pred&rdPredVelocityXY != 0 {
			msg.PutUint16(uint16(s.velocity[0]))
			msg.PutUint16(uint16(s.velocity[1]))
		}
		if pred&rdPredVelocityZ != 0 {
			msg.PutUint16(uint16(s.velocity[2]))
		}
		if pred&rdPredMsec != 0 {
			msg.PutByte(s.msec)
		}
		v := byte(s.weaponFrame & 127)
		if s.weaponFrame > 127 {
			v |= 128
		}
		msg.PutByte(v)
		if s.weaponFrame > 127 {
			msg.PutByte(byte(s.weaponFrame >> 7))
		}
	}
	if b&rdModel != 0 {
		wide(s.modelIndex)
	}
	if b&rdSkin != 0 {
		wide(uint16(s.skinNum))
	}
	if b&rdColorMap != 0 {
		msg.PutByte(s.colorMap)
	}
	if b&rdSolid != 0 {
		msg.PutUint16(s.solidSize)
	}
	if b&rdFlags != 0 {
		msg.PutByte(s.dpFlags)
	}
	if b&rdAlpha != 0 {
		msg.PutByte(s.trans)
	}
	if b&rdScale != 0 {
		msg.PutByte(s.scale)
	}
	if b&rdDrawFlags != 0 {
		msg.PutByte(s.drawFlags)
		if s.drawFlags&rdMLSMask >= rdMLSAddLight {
			msg.PutByte(s.absLight)
		}
	}
	if b&rdTagInfo != 0 {
		msg.PutUint16(uint16(s.tagEntity))
		msg.PutByte(s.tagIndex)
	}
	if b&rdLight != 0 {
		for _, v := range s.light {
			msg.PutUint16(v)
		}
		msg.PutByte(s.lightStyle)
		msg.PutByte(s.lightFlags)
	}
	if b&rdTrailEffect != 0 {
		v := s.trailEffect
		if s.emitEffect != 0 {
			v |= rdEmitEffectPresent
		}
		msg.PutUint16(v)
		if s.emitEffect != 0 {
			msg.PutUint16(s.emitEffect)
		}
	}
	if b&rdColorMod != 0 {
		msg.PutData(s.colourMod[:])
	}
	if b&rdGlow != 0 {
		msg.PutByte(s.glowSize)
		msg.PutByte(s.glowColour)
		msg.PutData(s.glowMod[:])
	}
	if b&rdFatness != 0 {
		msg.PutByte(byte(s.fatness))
	}
	if b&rdModel2 != 0 {
		wide(s.modelIndex2)
	}
	if b&rdGravityDir != 0 {
		msg.PutData(s.gravityDir[:])
	}
	if msg.error {
		return errors.New("RD record exceeds message buffer")
	}
	return nil
}

// sendRDPreSpawn reuses bounded metadata writers and supplies raw RD signon state.
func (qp *qProtocol) sendRDPreSpawn(ds *dStream) error {
	if qp.rd == nil || !qp.rd.ready {
		return errors.New("RD startup requires an accepted world reset")
	}
	groups := []struct {
		count    int
		overhead int // Record bytes not included in the reused metadata helper's budget.
		put      func(*netMsgW, int, int, int) int
	}{{maxClients, 18, qp.putUserInfos}, {maxLightStyles, 3, qp.putLightMaps}, {maxStaticSounds, 0, qp.putStaticSounds}}
	for _, group := range groups {
		for cursor := 0; cursor < group.count; {
			msg := qp.w.Clear()
			next := group.put(msg, group.overhead, maxMsgSize, cursor)
			if next <= cursor {
				return errors.New("RD metadata writer made no progress")
			}
			cursor = next
			if err := ds.sendMVDMessage(msg, mvdMsgRead, playersMaskAll); err != nil {
				return err
			}
		}
	}
	msg := qp.w.Clear()
	appendRecord := func(record *netMsgW) error {
		if record.error || record.wPos > maxMsgSize {
			return errors.New("RD signon record exceeds message limit")
		}
		if msg.wPos+record.wPos > maxMsgSize {
			if err := ds.sendMVDMessage(msg, mvdMsgRead, playersMaskAll); err != nil {
				return err
			}
			msg.Clear()
		}
		msg.PutData(record.Bytes())
		return nil
	}
	var data [256]byte
	record := newNetMsgW(data[:], qp.w.floatCoord)
	for id, present := range qp.rd.baselinePresent {
		if !present {
			continue
		}
		record.Clear()
		if qp.rd.baselineClassic[id] {
			qp.putRDClassicSignon(record, qp.rd.baselines[id], true)
		} else {
			record.PutSVC(svc_fte_spawnbaseline2)
			record.PutUint16(uint16(id))
			if err := qp.putRDPayload(record, qp.rd.baselines[id], defaultRDState()); err != nil {
				return err
			}
		}
		if err := appendRecord(record); err != nil {
			return err
		}
	}
	for i, s := range qp.rd.statics {
		record.Clear()
		if i < len(qp.rd.staticClassic) && qp.rd.staticClassic[i] {
			qp.putRDClassicSignon(record, s, false)
		} else {
			record.PutSVC(svc_fte_spawnstatic2)
			if err := qp.putRDPayload(record, s, defaultRDState()); err != nil {
				return err
			}
		}
		if err := appendRecord(record); err != nil {
			return err
		}
	}
	if msg.wPos > 0 {
		return ds.sendMVDMessage(msg, mvdMsgRead, playersMaskAll)
	}
	return nil
}

// putRDClassicSignon preserves classic baseline inheritance and ordinary primitives.
func (qp *qProtocol) putRDClassicSignon(msg *netMsgW, s rdEntityState, numbered bool) {
	if numbered {
		msg.PutSVC(svc_spawnbaseline)
		msg.PutUint16(uint16(s.number))
	} else {
		msg.PutSVC(svc_spawnstatic)
	}
	msg.PutByte(byte(s.modelIndex))
	msg.PutByte(byte(s.frame))
	msg.PutByte(s.colorMap)
	msg.PutByte(byte(s.skinNum))
	for i := range 3 {
		msg.PutCoord(s.origin[i])
		if msg.floatCoord {
			msg.PutUint16(uint16(int64(math.Round(float64(s.angles[i]) * 65536 / 360))))
		} else {
			msg.PutByte(byte(int64(math.Round(float64(s.angles[i]) * 256 / 360))))
		}
	}
}

// sendRDEnts queues a self-contained retained world in bounded complete chunks.
func (qp *qProtocol) sendRDEnts(ds *dStream) error {
	w := qp.rd
	if w == nil || !w.ready {
		return errors.New("RD startup requires an accepted world reset")
	}
	msg := qp.w.Clear()
	msg.PutSVC(svc_fte_updateentities)
	msg.PutFloat32(w.time)
	msg.PutUint16(rdUpdateRemove)
	flush := func() error {
		msg.PutUint16(0)
		if err := ds.sendMVDMessage(msg, mvdMsgRead, playersMaskAll); err != nil {
			return err
		}
		msg.Clear()
		msg.PutSVC(svc_fte_updateentities)
		msg.PutFloat32(w.time)
		return nil
	}
	var data [256]byte
	record := newNetMsgW(data[:], qp.w.floatCoord)
	appendEntity := func(id int, s, base rdEntityState, predOnly bool) error {
		record.Clear()
		record.PutRDUpdateIndex(uint32(id), false)
		var err error
		if predOnly {
			bits := rdPredInfo
			if !qp.w.floatCoord && qp.rdPayloadBits(s, base)&rdAnglesY != 0 && rdFineAngle(s.angles[1]) {
				bits |= rdAnglesY
			}
			err = qp.putRDFields(record, s, bits)
		} else {
			err = qp.putRDPayload(record, s, base)
		}
		if err != nil {
			return err
		}
		if record.wPos+7 > maxMsgSize {
			return errors.New("RD entity exceeds message limit")
		}
		if msg.wPos+record.wPos+2 > maxMsgSize {
			if err = flush(); err != nil {
				return err
			}
		}
		msg.PutData(record.Bytes())
		return nil
	}
	hasTail := false
	for id, present := range w.present {
		if !present {
			continue
		}
		base := defaultRDState()
		if w.baselinePresent[id] {
			base = w.baselines[id]
		}
		s := w.current[id]
		if err := appendEntity(id, s, base, false); err != nil {
			return err
		}
		hasTail = hasTail || qp.rdPredictionTail(s, base)
	}
	if err := flush(); err != nil {
		return err
	}
	if hasTail {
		for id, present := range w.present {
			if !present {
				continue
			}
			base := defaultRDState()
			if w.baselinePresent[id] {
				base = w.baselines[id]
			}
			s := w.current[id]
			if qp.rdPredictionTail(s, base) {
				if err := appendEntity(id, s, base, true); err != nil {
					return err
				}
			}
		}
		return flush()
	}
	return nil
}
