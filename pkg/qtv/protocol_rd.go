package qtv

import (
	"errors"
	"math"
)

// Base RD uses FTE's original payload layout, without newer PEXT2 profiles.
const (
	rdFrameBit uint32 = 1 << iota
	rdOriginXY
	rdOriginZ
	rdAnglesXZ
	rdAnglesY
	rdEffects
	rdPredInfo
	rdExtend1
	rdReset
	rdWide
	rdModel
	rdSkin
	rdColorMap
	rdSolid
	rdFlags
	rdExtend2
	rdAlpha
	rdScale
	rdBoneData
	rdDrawFlags
	rdTagInfo
	rdLight
	rdTrailEffect
	rdExtend3
	rdColorMod
	rdGlow
	rdFatness
	rdModel2
	rdGravityDir
	rdEffects2
	rdUnused
	rdExtend4
	maxRDEntities = 32768
)

// Prediction presence bits use the original UFP layout, including old weapon frames.
const (
	rdPredForward byte = 1 << iota
	rdPredSide
	rdPredUp
	rdPredMoveType
	rdPredVelocityXY
	rdPredVelocityZ
	rdPredMsec
	rdPredWeaponFrameOld
)

// RD update indices have distinct flags from ordinary FTE entity references.
const (
	rdUpdateRemove    = 0x8000
	rdUpdateExtend    = 0x4000
	rdUpdateIndexMask = 0x3fff
)

// Hexen2 draw flags select lighting and the model's scaling origin.
const (
	rdMLSMask           = 7
	rdMLSAddLight       = 6
	rdScaleOriginBottom = 32
	rdScaleOriginTop    = 64
	rdScaleOriginOrigin = rdScaleOriginTop | rdScaleOriginBottom
)

// Trail effects pack an optional emit-effect presence bit beside the index.
const (
	rdEffectIndexMask   = 0x3fff
	rdEmitEffectPresent = 0x8000
)

// rdEntityState retains the complete supported wire state, not a legacy projection.
type rdEntityState struct {
	number                         uint32     // Wire slot; zero is used only for unnumbered statics.
	origin, angles, viewAngles     [3]float32 // Position, model angles and derived player view angles.
	frame, modelIndex, modelIndex2 uint16     // Full-width animation and resource indices.
	skinNum                        int16      // Wide skins are signed in the base profile.
	colorMap                       byte       // Player color/skin mapping.
	effects                        uint32     // Complete effect mask.
	solidSize                      uint16     // Base size16 collision representation.
	movement, velocity             [3]int16   // Prediction values in their wire units.
	pmoveType, msec                byte       // Persistent movement flags and transient update age.
	weaponFrame                    uint16     // Base prediction animation, up to 32767.
	dpFlags, trans, scale          byte       // Render flags, opacity and scale.
	drawFlags, absLight            byte       // Optional Hexen2 appearance values.
	tagEntity                      uint32     // Ordinary entity reference for attachments.
	tagIndex                       byte       // Attachment tag, including the special value 255.
	light                          [4]uint16  // Optional dynamic light values.
	lightStyle, lightFlags         byte       // Dynamic light style and flags.
	trailEffect, emitEffect        uint16     // Particle effect indices, limited to 14 bits.
	colourMod                      [3]byte    // RGB multiplier, identity 32.
	glowSize, glowColour           byte       // Glow size and palette index.
	glowMod                        [3]byte    // Glow RGB multiplier.
	fatness                        int8       // Signed model expansion.
	gravityDir                     [2]byte    // Encoded gravity direction.
}

// defaultRDState supplies FTE's nonzero appearance defaults.
func defaultRDState() rdEntityState {
	return rdEntityState{trans: 255, scale: 16, drawFlags: rdScaleOriginOrigin, colourMod: [3]byte{32, 32, 32}, glowMod: [3]byte{32, 32, 32}}
}

// rdDelta holds only fields selected by bits and its prediction submask.
type rdDelta struct {
	bits  uint32        // Base UF field mask, including reset and angle precision.
	pred  byte          // UFP presence bits for persistent prediction fields.
	state rdEntityState // Decoded values; absent fields do not overwrite retained values.
}

// apply resolves inheritance, resetting transient prediction fields on each update.
func (d *rdDelta) apply(old, baseline *rdEntityState) rdEntityState {
	s := defaultRDState()
	if d.bits&rdReset != 0 {
		if baseline != nil {
			s = *baseline
		}
	} else if old != nil {
		s = *old
	}
	v := &d.state
	b := d.bits
	if b&rdFrameBit != 0 {
		s.frame = v.frame
	}
	if b&rdOriginXY != 0 {
		s.origin[0] = v.origin[0]
		s.origin[1] = v.origin[1]
	}
	if b&rdOriginZ != 0 {
		s.origin[2] = v.origin[2]
	}
	if b&rdAnglesXZ != 0 {
		s.angles[0] = v.angles[0]
		s.angles[2] = v.angles[2]
	}
	if b&rdAnglesY != 0 {
		s.angles[1] = v.angles[1]
	}
	for i := range 3 {
		mask := rdAnglesXZ
		if i == 1 {
			mask = rdAnglesY
		}
		if b&mask != 0 {
			a := s.angles[i]
			if i == 0 {
				if b&rdPredInfo != 0 {
					a *= -3
				} else {
					a = -a
				}
			}
			s.viewAngles[i] = float32(int64(a*65536/360)&65535) * (360.0 / 65536)
		}
	}
	if b&(rdEffects|rdEffects2) != 0 {
		s.effects = v.effects
	}
	s.movement = [3]int16{}
	s.velocity = [3]int16{}
	s.msec = 0
	if b&rdPredInfo != 0 {
		s.movement = v.movement
		s.velocity = v.velocity
		s.msec = v.msec
		if d.pred&rdPredMoveType != 0 {
			s.pmoveType = v.pmoveType
		}
		if d.pred&rdPredWeaponFrameOld != 0 {
			s.weaponFrame = v.weaponFrame
		}
	}
	if b&rdModel != 0 {
		s.modelIndex = v.modelIndex
	}
	if b&rdSkin != 0 {
		s.skinNum = v.skinNum
	}
	if b&rdColorMap != 0 {
		s.colorMap = v.colorMap
	}
	if b&rdSolid != 0 {
		s.solidSize = v.solidSize
	}
	if b&rdFlags != 0 {
		s.dpFlags = v.dpFlags
	}
	if b&rdAlpha != 0 {
		s.trans = v.trans
	}
	if b&rdScale != 0 {
		s.scale = v.scale
	}
	if b&rdDrawFlags != 0 {
		s.drawFlags = v.drawFlags
		s.absLight = v.absLight
	}
	if b&rdTagInfo != 0 {
		s.tagEntity = v.tagEntity
		s.tagIndex = v.tagIndex
	}
	if b&rdLight != 0 {
		s.light = v.light
		s.lightStyle = v.lightStyle
		s.lightFlags = v.lightFlags
	}
	if b&rdTrailEffect != 0 {
		s.trailEffect = v.trailEffect
		s.emitEffect = v.emitEffect
	}
	if b&rdColorMod != 0 {
		s.colourMod = v.colourMod
	}
	if b&rdGlow != 0 {
		s.glowSize = v.glowSize
		s.glowColour = v.glowColour
		s.glowMod = v.glowMod
	}
	if b&rdFatness != 0 {
		s.fatness = v.fatness
	}
	if b&rdModel2 != 0 {
		s.modelIndex2 = v.modelIndex2
	}
	if b&rdGravityDir != 0 {
		s.gravityDir = v.gravityDir
	}
	return s
}

// rdUpdate is one ordered entity change or removal; world reset belongs to rdFrame.
type rdUpdate struct {
	number uint32  // Nonzero network slot, in strictly ascending message order.
	remove bool    // Removal carries no payload and clears occupancy only.
	delta  rdDelta // Changed values for an ordinary update.
}

// rdFrame is a complete validated update message, independent of retained storage.
type rdFrame struct {
	time    float32    // Source server time shared by this message's changes.
	reset   bool       // Initial remove-world marker, preceding all entity records.
	updates []rdUpdate // Complete decoded records; never published incrementally.
}

// rdWorld belongs to one source/map. No world copies are allocated per viewer/frame.
type rdWorld struct {
	current, baselines       []rdEntityState // Slot-indexed current values and signon baselines.
	present, baselinePresent []bool          // Occupancy is independent of model index.
	baselineClassic          []bool          // Classic signon must keep its inherited angle semantics.
	statics                  []rdEntityState // Accepted unnumbered signon records in source order.
	staticClassic            []bool          // Encoding form of each retained static record.
	ready                    bool            // A source world reset has been accepted for this map.
	time                     float32         // Time of the last accepted RD message.
}

// ensureStorage grows all indexed source slices together after wire validation.
func (w *rdWorld) ensureStorage(required int) error {
	if required < 0 || required > maxRDEntities {
		return errors.New("RD storage exceeds entity capacity")
	}
	if required <= len(w.current) && w.current != nil {
		return nil
	}
	capacity := max(2048, len(w.current))
	for capacity < required {
		capacity *= 2
	}
	current := make([]rdEntityState, capacity)
	baselines := make([]rdEntityState, capacity)
	present := make([]bool, capacity)
	baselinePresent := make([]bool, capacity)
	baselineClassic := make([]bool, capacity)
	copy(current, w.current)
	copy(baselines, w.baselines)
	copy(present, w.present)
	copy(baselinePresent, w.baselinePresent)
	copy(baselineClassic, w.baselineClassic)
	w.current, w.baselines = current, baselines
	w.present, w.baselinePresent = present, baselinePresent
	w.baselineClassic = baselineClassic
	return nil
}

// finiteRD rejects nonfinite protocol values before they enter retained state.
func finiteRD(v float32) bool { return !math.IsNaN(float64(v)) && !math.IsInf(float64(v), 0) }

// apply preflights the complete message before publishing any changes.
func (w *rdWorld) apply(f rdFrame) error {
	if !finiteRD(f.time) || (!w.ready && !f.reset) {
		return errors.New("RD update requires a finite time and an initial reset")
	}
	last := uint32(0)
	for i := range f.updates {
		u := &f.updates[i]
		if u.number == 0 || u.number >= maxRDEntities || u.number <= last {
			return errors.New("invalid RD entity order or index")
		}
		last = u.number
		if u.delta.bits&(rdBoneData|rdUnused|rdExtend4) != 0 || u.delta.state.tagEntity >= maxRDEntities {
			return errors.New("unsupported RD payload")
		}
		for _, v := range u.delta.state.origin {
			if !finiteRD(v) {
				return errors.New("nonfinite RD position")
			}
		}
		for _, v := range u.delta.state.angles {
			if !finiteRD(v) {
				return errors.New("nonfinite RD angle")
			}
		}
	}
	if err := w.ensureStorage(int(last) + 1); err != nil {
		return err
	}
	if f.reset {
		clear(w.present)
	}
	for i := range f.updates {
		u := &f.updates[i]
		id := u.number
		if u.remove {
			w.present[id] = false
			continue
		}
		var old, baseline *rdEntityState
		if w.present[id] {
			old = &w.current[id]
		}
		if w.baselinePresent[id] {
			baseline = &w.baselines[id]
		}
		w.current[id] = u.delta.apply(old, baseline)
		w.current[id].number = id
		w.present[id] = true
	}
	w.ready = true
	w.time = f.time
	return nil
}
