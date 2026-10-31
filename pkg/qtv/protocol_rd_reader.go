package qtv

import "errors"

// readRDEntityRef reads ordinary FTE references, not RD's 14-bit update header.
func readRDEntityRef(msg *netMsgR) (uint32, error) {
	n := uint32(msg.GetUint16())
	if n&fteEntityRefExtend != 0 {
		n = (n&fteEntityRefShortMask)<<8 | uint32(msg.GetByte())
	}
	if msg.error || n >= maxRDEntities {
		return 0, errors.New("invalid or truncated RD entity reference")
	}
	return n, nil
}

// readRDDelta decodes one base-profile payload without mutating retained state.
func readRDDelta(msg *netMsgR, floatCoords bool) (rdDelta, error) {
	d := rdDelta{bits: uint32(msg.GetByte())}
	if d.bits&rdExtend1 != 0 {
		d.bits |= uint32(msg.GetByte()) << 8
	}
	if d.bits&rdExtend2 != 0 {
		d.bits |= uint32(msg.GetByte()) << 16
	}
	if d.bits&rdExtend3 != 0 {
		d.bits |= uint32(msg.GetByte()) << 24
	}
	if msg.error || d.bits&(rdBoneData|rdUnused|rdExtend4) != 0 {
		return d, errors.New("unsupported or truncated RD flags")
	}
	b := d.bits
	s := &d.state
	wide := func() uint16 {
		if b&rdWide != 0 {
			return msg.GetUint16()
		}
		return uint16(msg.GetByte())
	}
	coord := func() float32 {
		if floatCoords {
			return msg.GetFloat32()
		}
		return float32(msg.GetInt16()) / 8
	}
	angle := func() float32 {
		if b&rdPredInfo != 0 || msg.floatCoord {
			return msg.GetAngle16()
		}
		return float32(msg.GetByte()) * 360 / 256
	}
	if b&rdFrameBit != 0 {
		s.frame = wide()
	}
	if b&rdOriginXY != 0 {
		s.origin[0] = coord()
		s.origin[1] = coord()
	}
	if b&rdOriginZ != 0 {
		s.origin[2] = coord()
	}
	if b&rdAnglesXZ != 0 {
		s.angles[0] = angle()
		s.angles[2] = angle()
	}
	if b&rdAnglesY != 0 {
		s.angles[1] = angle()
	}
	switch b & (rdEffects | rdEffects2) {
	case rdEffects | rdEffects2:
		s.effects = msg.GetUint32()
	case rdEffects2:
		s.effects = uint32(msg.GetUint16())
	case rdEffects:
		s.effects = uint32(msg.GetByte())
	}
	if b&rdPredInfo != 0 {
		d.pred = msg.GetByte()
		for i := range 3 {
			if d.pred&(rdPredForward<<i) != 0 {
				s.movement[i] = msg.GetInt16()
			}
		}
		if d.pred&rdPredMoveType != 0 {
			s.pmoveType = msg.GetByte()
		}
		if d.pred&rdPredVelocityXY != 0 {
			s.velocity[0] = msg.GetInt16()
			s.velocity[1] = msg.GetInt16()
		}
		if d.pred&rdPredVelocityZ != 0 {
			s.velocity[2] = msg.GetInt16()
		}
		if d.pred&rdPredMsec != 0 {
			s.msec = msg.GetByte()
		}
		if d.pred&rdPredWeaponFrameOld != 0 {
			s.weaponFrame = uint16(msg.GetByte())
			if s.weaponFrame&128 != 0 {
				s.weaponFrame = (s.weaponFrame & 127) | (uint16(msg.GetByte()) << 7)
			}
		}
	}
	if b&rdModel != 0 {
		s.modelIndex = wide()
	}
	if b&rdSkin != 0 {
		if b&rdWide != 0 {
			s.skinNum = msg.GetInt16()
		} else {
			s.skinNum = int16(msg.GetByte())
		}
	}
	if b&rdColorMap != 0 {
		s.colorMap = msg.GetByte()
	}
	if b&rdSolid != 0 {
		s.solidSize = msg.GetUint16()
	}
	if b&rdFlags != 0 {
		s.dpFlags = msg.GetByte()
	}
	if b&rdAlpha != 0 {
		s.trans = msg.GetByte()
	}
	if b&rdScale != 0 {
		s.scale = msg.GetByte()
	}
	if b&rdDrawFlags != 0 {
		s.drawFlags = msg.GetByte()
		if s.drawFlags&rdMLSMask >= rdMLSAddLight {
			s.absLight = msg.GetByte()
		}
	}
	if b&rdTagInfo != 0 {
		var err error
		s.tagEntity, err = readRDEntityRef(msg)
		if err != nil {
			return d, err
		}
		s.tagIndex = msg.GetByte()
	}
	if b&rdLight != 0 {
		for i := range 4 {
			s.light[i] = msg.GetUint16()
		}
		s.lightStyle = msg.GetByte()
		s.lightFlags = msg.GetByte()
	}
	if b&rdTrailEffect != 0 {
		v := msg.GetUint16()
		s.trailEffect = v & rdEffectIndexMask
		if v&rdEmitEffectPresent != 0 {
			s.emitEffect = msg.GetUint16() & rdEffectIndexMask
		}
	}
	if b&rdColorMod != 0 {
		for i := range 3 {
			s.colourMod[i] = msg.GetByte()
		}
	}
	if b&rdGlow != 0 {
		s.glowSize = msg.GetByte()
		s.glowColour = msg.GetByte()
		for i := range 3 {
			s.glowMod[i] = msg.GetByte()
		}
	}
	if b&rdFatness != 0 {
		s.fatness = int8(msg.GetByte())
	}
	if b&rdModel2 != 0 {
		s.modelIndex2 = wide()
	}
	if b&rdGravityDir != 0 {
		s.gravityDir = [2]byte{msg.GetByte(), msg.GetByte()}
	}
	if msg.error {
		return d, errors.New("truncated RD payload")
	}
	for _, v := range s.origin {
		if !finiteRD(v) {
			return d, errors.New("nonfinite RD coordinate")
		}
	}
	return d, nil
}

// readRDFrame consumes one complete updateentities stream, including its terminator.
func readRDFrame(msg *netMsgR, floatCoords bool) (rdFrame, error) {
	f := rdFrame{time: msg.GetFloat32()}
	if !finiteRD(f.time) {
		return f, errors.New("nonfinite RD time")
	}
	last := uint32(0)
	first := true
	for {
		word := uint32(msg.GetUint16())
		remove := word&rdUpdateRemove != 0
		n := word & rdUpdateIndexMask
		if word&rdUpdateExtend != 0 {
			n |= uint32(msg.GetByte()) << 14
		}
		if msg.error {
			return f, errors.New("truncated RD update stream")
		}
		if n == 0 && !remove {
			return f, nil
		}
		if n == 0 && remove {
			if !first {
				return f, errors.New("RD reset must be first")
			}
			f.reset = true
			first = false
			continue
		}
		first = false
		if n >= maxRDEntities || n <= last {
			return f, errors.New("invalid RD update index or order")
		}
		last = n
		u := rdUpdate{number: n, remove: remove}
		if !remove {
			var err error
			u.delta, err = readRDDelta(msg, floatCoords)
			if err != nil {
				return f, err
			}
		}
		f.updates = append(f.updates, u)
	}
}

// readRDBaseline decodes a numbered baseline or unnumbered static from defaults.
func readRDBaseline(msg *netMsgR, numbered, floatCoords bool) (rdEntityState, error) {
	var n uint32
	var err error
	if numbered {
		n, err = readRDEntityRef(msg)
		if err != nil {
			return rdEntityState{}, err
		}
	}
	d, err := readRDDelta(msg, floatCoords)
	if err != nil {
		return rdEntityState{}, err
	}
	s := d.apply(nil, nil)
	s.number = n
	return s, nil
}

// usesRD follows the format of this map, never a viewer's download capability.
func (qp *qProtocol) usesRD() bool { return qp.extFlagsFTE2&ftePext2ReplacementDeltas != 0 }

// rdFloatCoords includes MVD's entity-only coordinate override.
func (qp *qProtocol) rdFloatCoords() bool {
	return qp.extFlagsFTE1&ftePextFloatCoords != 0 || qp.extFlagsMVD1&mvdPext1FloatCoords != 0
}

// ensureRD creates the sole source-owned state, allocated only for an RD map.
func (qp *qProtocol) ensureRD() *rdWorld {
	if qp.rd == nil {
		qp.rd = &rdWorld{}
	}
	qp.rd.ensureStorage()
	return qp.rd
}

// readRDUpdate applies one complete validated message without a legacy projection.
func (qp *qProtocol) readRDUpdate() error {
	f, err := readRDFrame(qp.r, qp.rdFloatCoords())
	if err != nil {
		return err
	}
	for _, u := range f.updates {
		if int(u.delta.state.modelIndex) >= len(qp.modelList) || int(u.delta.state.modelIndex2) >= len(qp.modelList) {
			return errors.New("RD model index exceeds resource capacity")
		}
	}
	return qp.ensureRD().apply(f)
}

// readRDSignon accepts RD payloads and the legacy baseline/static form allowed by FTE.
func (qp *qProtocol) readRDSignon(delta, numbered bool) error {
	var s rdEntityState
	var err error
	if delta {
		s, err = readRDBaseline(qp.r, numbered, qp.rdFloatCoords())
	} else {
		s = defaultRDState()
		if numbered {
			s.number = uint32(qp.r.GetUint16())
		}
		s.modelIndex = uint16(qp.r.GetByte())
		s.frame = uint16(qp.r.GetByte())
		s.colorMap = qp.r.GetByte()
		s.skinNum = int16(qp.r.GetByte())
		for i := range 3 {
			// Classic signon uses ordinary primitives, not MVD's RD-only override.
			s.origin[i] = qp.r.GetCoord()
			s.angles[i] = qp.r.GetAngle()
		}
		if qp.r.error || s.number >= maxRDEntities {
			err = errors.New("invalid legacy RD signon record")
		}
	}
	if err != nil {
		return err
	}
	for _, v := range s.origin {
		if !finiteRD(v) {
			return errors.New("nonfinite RD signon coordinate")
		}
	}
	if int(s.modelIndex) >= len(qp.modelList) || int(s.modelIndex2) >= len(qp.modelList) {
		return errors.New("RD model index exceeds resource capacity")
	}
	w := qp.ensureRD()
	if numbered {
		w.baselines[s.number] = s
		w.baselinePresent[s.number] = true
	} else {
		if len(w.statics) >= maxStaticEntities {
			return errors.New("too many RD statics")
		}
		w.statics = append(w.statics, s)
	}
	return nil
}

// readEventEntity consumes an ordinary entity reference with the current wire profile.
func (qp *qProtocol) readEventEntity() (uint32, error) {
	if qp.usesRD() {
		return readRDEntityRef(qp.r)
	}
	return uint32(qp.r.GetUint16()), nil
}
