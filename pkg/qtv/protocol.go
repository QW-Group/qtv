package qtv

import (
	"github.com/qqshka/qtv-go/pkg/info"
	"go.uber.org/atomic"
)

//
// QW protocol constants and types.
//

// QW protocol reader (deserializer) and writer (serializer).
type qProtocol struct {
	us                 *uStream    // Link to upstream which contains this struct.
	w                  *netMsgW    // Message for writing/generating data.
	r                  *netMsgR    // Message for reading/parsing data.
	svc                protocolSvc // QW protocol SVC for currently parsed message.
	messageType        mvdMsgType  // MVD message type for currently parsed message.
	playersMask        uint32      // MVD players mask for currently parsed message.
	protocolReaderData             // Cumulative data we parsed from message(s). Resets on each map load. HUGE!
}

func newQProtocol(us *uStream) *qProtocol {
	qp := &qProtocol{
		us: us,
		r:  newNetMsgR(nil, false),
		w:  newNetMsgW(make([]byte, maxMvdSize), false),
	}

	qp.reset()

	return qp
}

// Reset quake protocol, should be called each time we connect to upstream.
func (qp *qProtocol) reset() {
	// Reset message for generate, but reuse buffer.
	qp.w.Reset(qp.w.b, false)
	// Reset message for parse.
	qp.r.Reset(nil, false)
	// Reset MVD message related fields.
	qp.svc = 0
	qp.messageType = 0
	qp.playersMask = 0
	// Reset reader storage.
	qp.protocolReaderData.reset()
}

// Quake protocol reader storage.
type protocolReaderData struct {
	incomingSequence uint
	isPaused         bool
	extFlagsFTE1     uint32
	extFlagsFTE2     uint32
	extFlagsMVD1     uint32
	gameDir          string
	mapName          string
	moveVars         moveVars
	spawnStatic      [maxStaticEntities]entityState
	spawnStaticCount int
	players          [maxClients]playerInfo
	baseLine         [maxEntities]entityState
	frame            [maxEntityFrames]qFrame
	modelList        [maxModels]string
	soundList        [maxSounds]string
	lightStyle       [maxLightStyles]string
	staticSound      [maxStaticSounds]staticSound
	staticSoundCount int
	cdTrack          byte
	serverInfo       info.InfoTs
	remoteHostName   atomic.String // Remote serverinfo hostname. (atomic since it could be used concurrently by upstream and downstream)
	// meag:
	// MVDSV sends STAT_TIME directly to player, so it isn't in mvd/qtv stream,
	// but STAT_MATCHSTARTTIME is sent (by KTX) every time the player respawns, so can pick this value up and use a local offset to guess.
	// qqshka: above comment valid only if upstream connected to server before match start.
	matchStartTime      uint64
	matchStartLocalTime uint64
}

func (pd *protocolReaderData) reset() {
	*pd = protocolReaderData{}
}

type playerInfo struct {
	stats      [maxStats]uint32
	userInfo   info.InfoTs
	userId     uint32
	ping       int
	packetLoss int
	frags      int
	enterTime  float64 // Despite we get it over network as 32 bit still better keep time as 64 bit.
	gibbed     bool
	dead       bool

	playerState
}

// Server send svc_updateuserinfo with empty userinfo if client disconnected.
// So we detect if user active just by checking is userinfo empty or not,
// that similar to how QW client checking if user is active.
func (pi *playerInfo) isActive() bool {
	// Seems like meag broke something and userInfo may contain *auth key despite player is not really connected,
	// as a workaround checking for "name" instead of IsEmpty.
	//	return pi.userInfo.IsEmpty() == false
	return pi.userInfo.Get("name") != ""
}

// Return true if pi is spectator.
func (pi *playerInfo) isSpectator() bool {
	return pi.isActive() && pi.userInfo.Get("*spectator") != ""
}

// Return true if pi is player.
func (pi *playerInfo) isPlayer() bool {
	return pi.isActive() && pi.userInfo.Get("*spectator") == ""
}

type playerState struct {
	origin      [3]float32
	angles      [3]float32
	velocity    [3]int16
	frame       byte
	modelIndex  byte
	skinNum     byte
	effects     byte
	weaponFrame byte
}

type moveVars struct {
	gravity           float32
	maxSpeed          float32
	spectatorMaxSpeed float32
	accelerate        float32
	airAccelerate     float32
	waterFriction     float32
	entgrav           float32
	stopSpeed         float32
	waterAccelerate   float32
	friction          float32
}

type qFrame struct {
	ents    [maxDemoPacketEntities]entityState
	entNums [maxDemoPacketEntities]uint16
	numEnts int
}

type entityState struct {
	origin     [3]float32
	angles     [3]float32
	frame      byte
	modelIndex byte
	colorMap   byte
	skinNum    byte
	effects    byte
}

type staticSound struct {
	origin      [3]float32
	soundIndex  byte
	volume      byte
	attenuation byte
}

const (
	msgBufSize       = 8192
	maxMsgSize       = 1450
	fileTransferSize = (maxMsgSize - 100)
)

const (
	maxClients            = 32
	maxList               = 256
	maxModels             = maxList
	maxSounds             = maxList
	maxEntityFrames       = 64
	maxDemoPacketEntities = 300
	maxEntities           = 512
	maxStaticEntities     = 512
	maxLightStyles        = 64
	maxStaticSounds       = 512
	maxStats              = 32
)

const (
	statTime           = 17
	statMatchStartTime = 18 // Server should send this as msec (int).
)

const (
	protocolVersion     uint32 = 28
	protocolVersionFTE  uint32 = (('F' << 0) + ('T' << 8) + ('E' << 16) + ('X' << 24)) //fte extensions.
	protocolVersionFTE2 uint32 = (('F' << 0) + ('T' << 8) + ('E' << 16) + ('2' << 24)) //fte extensions.
	protocolVersionMVD1 uint32 = (('M' << 0) + ('V' << 8) + ('D' << 16) + ('1' << 24)) //mvdsv extensions.
)

const (
	ftePextFloatCoords  uint32 = 0x00008000
	ftePextSpawnStatic2 uint32 = 0x00400000 // Sends an entity delta instead of a baseline (or spawnstatic). Obsoleted by PEXT2_REPLACEMENTDELTAS.
)

const (
	qtvEzQuakeExt = "QTV_EZQUAKE_EXT"

	qtvEzQuakeExtDownload    = (1 << 0) // QTV support download and also different connection process.
	qtvEzQuakeExtSetInfo     = (1 << 1) // QTV support setinfo.
	qtvEzQuakeExtQtvUserList = (1 << 2) // QTV support qtvuserlist command.

	qtvEzQuakeExtMask = (qtvEzQuakeExtDownload | qtvEzQuakeExtSetInfo | qtvEzQuakeExtQtvUserList)
)

// Flags on entities.
const (
	uOrigin1  = (1 << 9)
	uOrigin2  = (1 << 10)
	uOrigin3  = (1 << 11)
	uAngle2   = (1 << 12)
	uFrame    = (1 << 13)
	uRemove   = (1 << 14)
	uMoreBits = (1 << 15)
)

// If uMoreBits is set, these additional flags are read in next.
const (
	uAngle1   = (1 << 0)
	uAngle3   = (1 << 1)
	uModel    = (1 << 2)
	uColorMap = (1 << 3)
	uSkin     = (1 << 4)
	uEffects  = (1 << 5)
	uSolid    = (1 << 6)
)

// svc_print messages have an id, so messages can be filtered.
type printLevel byte

const (
	printLow    printLevel = 0
	printMedium printLevel = 1
	printHigh   printLevel = 2
	printChat   printLevel = 3
)

type protocolSvc byte

const (
	svc_bad protocolSvc = iota
	svc_nop
	svc_disconnect
	svc_updatestat
	svc_version_UNUSED
	svc_nqsetview
	svc_sound
	svc_nqtime
	svc_print
	svc_stufftext
	svc_setangle
	svc_serverdata
	svc_lightstyle
	svc_nqupdatename
	svc_updatefrags
	svc_nqclientdata
	svc_stopsound_UNUSED
	svc_nqupdatecolors
	svc_particle
	svc_damage
	svc_spawnstatic
	svc_fte_spawnstatic2
	svc_spawnbaseline
	svc_temp_entity
	svc_setpause
	svc_nqsignonnum
	svc_centerprint
	svc_killedmonster
	svc_foundsecret
	svc_spawnstaticsound
	svc_intermission
	svc_finale
	svc_cdtrack
	svc_sellscreen_UNUSED
	svc_smallkick
	svc_bigkick
	svc_updateping
	svc_updateentertime
	svc_updatestatlong
	svc_muzzleflash
	svc_updateuserinfo
	svc_download
	svc_playerinfo
	svc_nails
	svc_chokecount
	svc_modellist
	svc_soundlist
	svc_packetentities
	svc_deltapacketentities
	svc_maxspeed
	svc_entgravity
	svc_setinfo
	svc_serverinfo
	svc_updatepl
	svc_nails2
	svc_unused_55
	svc_unused_56
	svc_unused_57
	svc_unused_58
	svc_unused_59
	svc_unused_60
	svc_unused_61
	svc_unused_62
	svc_unused_63
	svc_unused_64
	svc_unused_65
	svc_fte_spawnbaseline2
	svc_unused_67
	svc_unused_68
	svc_unused_69
	svc_unused_70
	svc_unused_71
	svc_unused_72
	svc_unused_73
	svc_unused_74
	svc_unused_75
	svc_unused_76
	svc_unused_77
	svc_unused_78
	svc_unused_79
	svc_unused_80
	svc_unused_81
	svc_unused_82
	svc_unused_83
	svc_fte_voicechat
)
