package qtv

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/adam-lavrik/go-imath/i64"
	"github.com/adam-lavrik/go-imath/ix"
	"github.com/hashicorp/go-multierror"
	"github.com/markphelps/optional"
	"github.com/qqshka/qtv-go/pkg/qfs"
	"github.com/rs/zerolog/log"
)

//
// Read downstream client input, parse, tokenize, execute.
//

var (
	// Mapping between downstream client command name and handler function.
	dStreamCommands map[string]*dStreamCmd
)

func init() {
	dStreamCommands = map[string]*dStreamCmd{
		"qtvspawn":     {f: (*dStream).spawnClientCmd},
		"qtvsoundlist": {f: (*dStream).soundListClientCmd},
		"qtvmodellist": {f: (*dStream).modelListClientCmd},
		"download":     {f: (*dStream).downloadClientCmd},
		"stopdl":       {f: (*dStream).stopDownloadClientCmd},
		"stopdownload": {f: (*dStream).stopDownloadClientCmd},
		"say":          {f: (*dStream).sayClientCmd},
		"say_team":     {f: (*dStream).sayClientCmd},
		"say_game":     {f: (*dStream).sayClientCmd},
		"users":        {f: (*dStream).usersClientCmd},
		"setinfo":      {f: (*dStream).setInfoClientCmd},
		"ptrack":       {f: (*dStream).ptrackClientCmd},
		"lastscores":   {f: (*dStream).lastScoresClientCmd, sendAlias: true},
		"follow":       {f: (*dStream).followClientCmd, sendAlias: true},
		"qul":          {f: nil}, // We does not support user list update from the downstream - ignore it.
	}
}

type dStreamCmd struct {
	f         dStreamCmdFunc // If 'f' is nil then it silently ignored, useful for known command which should be ignored.
	sendAlias bool           // If true then alias is sended to client, useful if command should be accessible directly without /cmd prefix.
}

type dStreamCmdFunc func(ds *dStream, tr *tokenizerResult) error

// Tokenize and execute command from downstream client.
func (ds *dStream) executeClientCommand(cmd string) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.executeClientCommand:") }()

	// Tokenize command.
	tr, _ := tokenizeString(cmd)
	if tr.Argc() == 0 {
		return nil // Empty command.
	}
	// Lowercase command name.
	name := strings.ToLower(tr.Argv(0))
	tr.SetArgv(0, name)
	// Get command handler function.
	c, found := dStreamCommands[name]
	if found == false {
		log.Debug().Str("ctx", "dStream").Uint32("id", uint32(ds.id)).Str("event", "unknownCommand").Str("command", name).Str("args", tr.Args()).Msg("")
		return nil
	}
	log.Trace().Str("ctx", "dStream").Uint32("id", uint32(ds.id)).Str("event", "command").Str("command", name).Str("args", tr.Args()).Msg("")
	if c.f == nil {
		return nil // Command known, but ignored.
	}
	// Execute.
	return c.f(ds, &tr)
}

// Specifies input type from downstream.
// 'clc' stands for client command.
type clcType byte

const (
	clcInvalid   clcType = 0
	clcStringCmd clcType = 1 // Commands like: say/download/setinfo ...
)

// Check downstrteam incoming buffer for possible client input.
func (ds *dStream) processClientInput() (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.processClientInput:") }()

	// Process all messages inside client incoming buffer.
	for {
		if done, err := ds.processClientInputMsg(); err != nil {
			return err
		} else if done {
			return nil
		}
	}
}

// Check if incoming buffer contains at least one message, read it and process.
// Also discard processed data, so if we have one more command it would be executed when this functions called again.
func (ds *dStream) processClientInputMsg() (done bool, err error) {
	defer func() { err = multierror.Prefix(err, "dStream.processClientInputMsg:") }()

	state := ds.getState()
	if state < dsNeedInitialData {
		return true, fmt.Errorf("invalid state: %v", state)
	}

	msg := newNetMsgR(ds.rb.BytesOneReader(), false)
	if msg.BLen() < 2 {
		return true, nil // Not enough data, wait for it.
	}
	len := int(msg.GetUint16())
	if msg.error {
		return true, fmt.Errorf("invalid input: read error")
	}
	// If len is greater than capacity of our buffer - that is a no-go.
	if len > ds.rb.Capacity() || len < 3 { // 3 = two bytes for len and one byte for clc.
		return true, fmt.Errorf("invalid input: len = %v", len)
	}
	if len > msg.BLen() {
		return true, nil // Not enough data, wait for it.
	}

	clc := clcType(msg.GetByte())
	switch clc {
	case clcStringCmd:
		cmd := msg.GetString()
		if msg.error {
			return true, fmt.Errorf("invalid input: read error")
		}
		if err := ds.executeClientCommand(cmd); err != nil {
			return true, err
		}
	default:
		// Ignore clc but log it.
		log.Trace().Str("ctx", "dStream").Uint32("id", uint32(ds.id)).Str("event", "unknownClc").Uint8("clc", uint8(clc)).Msg("")
	}

	return false, ds.rb.Discard(int(len))
}

// Send text as "svc_print" to particular dStream.
func (ds *dStream) svcPrintf(level printLevel, format string, a ...interface{}) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.svcPrintf:") }()

	us := ds.linkedUs

	msg := us.qp.w.Clear()
	us.qp.putSvcPrintf(msg, level, format, a...)

	return ds.sendMVDMessage(msg, mvdMsgAll, playersMaskAll)
}

var (
	errCheckAvail = errors.New("overflow")
)

const (
	defaultCheckAvail = 1024 * 16
)

// Wrap data inside MVD message and write it to dStream.
func (ds *dStream) sendMVDMessageEx(msg *netMsgW, msgType mvdMsgType, playersMask uint32, checkAvail int, msgTime byte) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.sendMVDMessageEx:") }()

	us := ds.linkedUs

	mvdHdr := us.mvdHdr.Clear()

	mvdHdr.PutByte(msgTime)
	mvdHdr.PutByte(byte(msgType))
	mvdHdr.PutUint32(uint32(msg.WPos()))

	if msgType == mvdMsgMulti {
		mvdHdr.PutUint32(playersMask)
	}

	// With checkAvail we trying not to flood our output buffer with chat or user lists updates (even if we loose such info),
	// especially useful during connection when output buffer under huge pressure.
	if checkAvail > 0 {
		totalSize := mvdHdr.WPos() + msg.WPos() + checkAvail
		if totalSize > ds.wb.Available() {
			return errCheckAvail
		}
	}

	if err := ds.sendNetMsg(mvdHdr); err != nil {
		return err
	}
	return ds.sendNetMsg(msg)
}

// Wrap data inside MVD message and write it to dStream.
func (ds *dStream) sendMVDMessage(msg *netMsgW, msgType mvdMsgType, playersMask uint32) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.sendMVDMessage:") }()
	return ds.sendMVDMessageEx(msg, msgType, playersMask, 0, 0)
}

// Some client commands should be accessible without /cmd prefix at client console, f.e. /follow.
// In order to do so we send aliases to the client.
func (ds *dStream) sendClientAliases() (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.sendClientAliases:") }()

	if ds.connectedAtLeastOnce {
		return nil
	}

	if (ds.qtvEzQuakeExt & qtvEzQuakeExtDownload) == 0 {
		return nil
	}

	us := ds.linkedUs

	for k, v := range dStreamCommands {
		if v.sendAlias == false {
			continue
		}
		if err := us.qp.sendStuffTextf(ds, "alias %s cmd %s %%0\n", k, k); err != nil {
			return err
		}
	}
	return nil
}

func (ds *dStream) soundListClientCmd(tr *tokenizerResult) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.soundListClientCmd:") }()

	us := ds.linkedUs

	if tr.ArgvAtoi(1) != int(us.serverCount) {
		return nil // Ignore command from previous map instance.
	}
	if ds.getState() != dsNeedSoundList {
		log.Trace().Str("ctx", "dStream").Str("name", "soundListClientCmd").Uint32("id", uint32(ds.id)).Str("event", "needReconnect").Msg("invalid ds state")
		// Tell downstream to reconnect.
		return ds.setState(dsNeedInitialData)
	}
	if us.getState() != usActive {
		log.Trace().Str("ctx", "dStream").Str("name", "soundListClientCmd").Uint32("id", uint32(ds.id)).Str("event", "needReconnect").Msg("invalid us state")
		// Tell downstream to reconnect.
		return ds.setState(dsNeedInitialData)
	}

	if err := us.qp.sendList(ds, us.qp.soundList[:], svc_soundlist, svc_fte_soundlistshort_UNUSED); err != nil {
		return err
	}

	return ds.setState(dsNeedModelList)
}

func (ds *dStream) modelListClientCmd(tr *tokenizerResult) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.modelListClientCmd:") }()

	us := ds.linkedUs

	if tr.ArgvAtoi(1) != int(us.serverCount) {
		return nil // Ignore command from previous map instance.
	}
	if ds.getState() != dsNeedModelList {
		log.Trace().Str("ctx", "dStream").Str("name", "modelListClientCmd").Uint32("id", uint32(ds.id)).Str("event", "needReconnect").Msg("invalid ds state")
		// Tell downstream to reconnect.
		return ds.setState(dsNeedInitialData)
	}
	if us.getState() != usActive {
		log.Trace().Str("ctx", "dStream").Str("name", "modelListClientCmd").Uint32("id", uint32(ds.id)).Str("event", "needReconnect").Msg("invalid us state")
		// Tell downstream to reconnect.
		return ds.setState(dsNeedInitialData)
	}

	if err := us.qp.sendList(ds, us.qp.modelList[:], svc_modellist, svc_fte_modellistshort); err != nil {
		return err
	}

	return ds.setState(dsNeedSpawn)
}

func (ds *dStream) spawnClientCmd(tr *tokenizerResult) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.spawnClientCmd:") }()

	us := ds.linkedUs

	if tr.ArgvAtoi(1) != int(us.serverCount) {
		return nil // Ignore command from previous map instance.
	}
	if ds.getState() != dsNeedSpawn {
		log.Trace().Str("ctx", "dStream").Str("name", "spawnClientCmd").Uint32("id", uint32(ds.id)).Str("event", "needReconnect").Msg("invalid ds state")
		// Tell downstream to reconnect.
		return ds.setState(dsNeedInitialData)
	}
	if us.getState() != usActive {
		log.Trace().Str("ctx", "dStream").Str("name", "spawnClientCmd").Uint32("id", uint32(ds.id)).Str("event", "needReconnect").Msg("invalid us state")
		// Tell downstream to reconnect.
		return ds.setState(dsNeedInitialData)
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

// Executed periodically when download is active.
func (ds *dStream) nextDownloadDo() (done bool, err error) {
	defer func() {
		err = multierror.Prefix(err, "dStream.nextDownloadDo:")
		if err != nil {
			ds.download.Reset()
		}
	}()

	if !ds.download.IsActive() {
		return true, nil
	}

	sendBytes := i64.Min(fileTransferSize, ds.download.size-ds.download.current)

	if int64(ds.wb.Length())+sendBytes > int64(ds.wb.Capacity()*2/3) {
		return true, nil // Done, no free space right now.
	}

	percent := 0
	ds.download.current += sendBytes
	if ds.download.current >= ds.download.size {
		percent = 100 // We are done.
	} else {
		percent = int(float64(100*ds.download.current) / float64(i64.Max(1, ds.download.size)))
		percent = ix.Min(99, percent) // Percent should not be more than 99 if we are not done yet.
	}

	us := ds.linkedUs

	msg := us.qp.w.Clear()

	msg.PutSVC(svc_download)
	msg.PutUint16(uint16(sendBytes))
	msg.PutByte(byte(percent))
	msg.PutDataFromReader(ds.download.file, int(sendBytes))
	if err := ds.sendMVDMessage(msg, mvdMsgAll, playersMaskAll); err != nil {
		return true, err
	}

	if ds.download.current >= ds.download.size {
		ds.download.Reset()
		return true, nil // Done, no errors.
	}

	// Signaling to the caller what it should call us again since we still have space in the output buffer.
	return false, nil
}

// Executed periodically when download is active.
func (ds *dStream) nextDownload() (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.nextDownload:") }()

	for i := 0; i < 100; i++ {
		if done, err := ds.nextDownloadDo(); err != nil {
			return err
		} else if done {
			return nil
		}
	}

	return nil
}

func (ds *dStream) downloadClientCmd(tr *tokenizerResult) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.downloadClientCmd:") }()

	us := ds.linkedUs

	if tr.Argc() != 2 {
		return ds.svcPrintf(printHigh, "download [filename]\n")
	}

	name := tr.Argv(1)
	gameDir := us.gameDir()

	allow := false
	isDemo := false
	if strings.HasPrefix(name, "demos/") {
		isDemo = true
		gameDir = "" // We attempt to open demos without gameDir.
		name = us.qtv.demoDir() + "/" + strings.TrimPrefix(name, "demos/")
	}

	// Guess name without gameDir.
	if name, err = qfs.BasePath("", name); err != nil {
		err = nil
		goto denyDownload
	}
	name = strings.ReplaceAll(name, "\\", "/")
	name = strings.ToLower(name)

	if !us.qtv.qvs.Get("allow_download").Bool {
		// Download is not allowed at all.
	} else if fileNameHasSensitiveExtension(name) {
		// Do not allow download sensitive data.
	} else if name[0] == '.' {
		// Do not allow names with leading dot.
	} else if name[0] == '/' {
		// Do not allow absolute names.
	} else if !strings.Contains(name, "/") {
		// Requested file should be in subdirectory.
	} else if strings.HasPrefix(name, "skins/") {
		allow = us.qtv.qvs.Get("allow_download_skins").Bool
	} else if strings.HasPrefix(name, "progs/") {
		allow = us.qtv.qvs.Get("allow_download_models").Bool
	} else if strings.HasPrefix(name, "sound/") {
		allow = us.qtv.qvs.Get("allow_download_sounds").Bool
	} else if strings.HasPrefix(name, "maps/") {
		allow = us.qtv.qvs.Get("allow_download_maps").Bool
	} else if isDemo {
		allow = us.qtv.qvs.Get("allow_download_demos").Bool && demoNameHasValidExtension(name)
	} else {
		allow = us.qtv.qvs.Get("allow_download_other").Bool
	}

	if !allow {
		goto denyDownload
	}

	// Close previous download if any.
	ds.download.Reset()

	// Open new download.
	if f, s, err := qfs.Open(gameDir, name); err == nil {
		ds.download.file = f
		ds.download.size = s
	}

	if !ds.download.IsActive() {
		goto denyDownload
	}

	// All checks passed, start downloading.
	if err := ds.nextDownload(); err != nil {
		return err
	}

	// Download info.
	return ds.svcPrintf(printHigh, "File %s is %vKB (%.2fMB)\n", name, ds.download.size/1024, float64(ds.download.size)/1024/1024)

denyDownload:

	msg := us.qp.w.Clear()

	msg.PutSVC(svc_download)
	msg.PutUint16(math.MaxUint16)
	msg.PutByte(0)

	return ds.sendMVDMessage(msg, mvdMsgAll, playersMaskAll)
}

func (ds *dStream) stopDownloadClientCmd(tr *tokenizerResult) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.stopDownloadClientCmd:") }()

	us := ds.linkedUs

	if !ds.download.IsActive() {
		return nil
	}

	ds.download.Reset()

	msg := us.qp.w.Clear()

	msg.PutSVC(svc_download)
	msg.PutUint16(0)
	msg.PutByte(100)

	if err := ds.sendMVDMessage(msg, mvdMsgAll, playersMaskAll); err != nil {
		return err
	}

	return ds.svcPrintf(printHigh, "Download stopped\n")
}

func (ds *dStream) putPovToMsg(msg *netMsgW) {
	if !ds.pov.Present() {
		return
	}

	us := ds.linkedUs

	slot := iBound(0, ds.pov.OrElse(0), maxClients-1)
	player := &us.qp.players[slot]

	us.qp.putStuffTextf(msg, "track %v\n", player.userId)
}

func (ds *dStream) sendPovChangeToFollowers(singleCast *dStream) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.sendPovChangeToFollowers:") }()

	us := ds.linkedUs

	msg := us.qp.w.Clear()
	ds.putPovToMsg(msg)
	if msg.WPos() == 0 {
		return nil
	}

	us.assertMutexIsLocked()
	for _, d := range us.linkedDs {
		if singleCast != nil && singleCast != d {
			continue
		}
		if d.followId != ds.id {
			continue // downstream does not follow us.
		}
		if err := d.sendMVDMessage(msg, mvdMsgAll, playersMaskAll); err != nil {
			d.cancel() // Tell downstream to die.
			continue
		}
	}

	return nil
}

func (ds *dStream) ptrackClientCmd(tr *tokenizerResult) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.ptrackClientCmd:") }()

	if tr.Argc() != 2 {
		ds.pov = optional.Int{}
		return ds.sendPovChangeToFollowers(nil)
	}

	slot := tr.ArgvAtoi(1)
	if slot < 0 || slot >= maxClients {
		return ds.svcPrintf(printHigh, "Invalid client to track: %v\n", slot)
	}
	ds.pov.Set(slot)
	return ds.sendPovChangeToFollowers(nil)
}

func (ds *dStream) followClientCmd(tr *tokenizerResult) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.followClientCmd:") }()

	if tr.Argc() != 2 {
		ds.followId = 0
		return ds.svcPrintf(printHigh, "follow: turned off\n")
	}

	us := ds.linkedUs

	leaderName := tr.Argv(1)
	leader := us.getDownstreamByNameOrId(leaderName)
	if leader == nil {
		return ds.svcPrintf(printHigh, "follow: user '%s' with such name or id not found\n", leaderName)
	}
	if leader == ds {
		return ds.svcPrintf(printHigh, "follow: you can't follow yourself\n")
	}

	ds.followId = leader.id

	if err = ds.svcPrintf(printHigh, "follow: tracking '%s'\n", leaderName); err != nil {
		return err
	}

	// Send leader's pov to 'ds' right now.
	if err := leader.sendPovChangeToFollowers(ds); err != nil {
		// That was rather 'leader' error so kill him instead of 'ds'.
		leader.cancel() // Tell downsream to die.
		return nil
	}
	return nil
}

func (ds *dStream) lastScoresClientCmd(tr *tokenizerResult) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.lastScoresClientCmd:") }()

	us := ds.linkedUs

	msg := us.qp.w.Clear()
	us.qp.putLastScores(msg)
	if msg.error {
		return nil // Silently ignore it.
	}
	return ds.sendMVDMessage(msg, mvdMsgAll, playersMaskAll)
}

const (
	maxFpCommands = 10
)

type floodProtect struct {
	locked   uint64
	cmdTime  [maxFpCommands]uint64
	lastCmd  int
	warnings int
}

// Returns true if 'ds' is flooding chat with say command.
func (ds *dStream) isSayFlood() (bool, error) {
	fp := &ds.fp
	idx := iBound(0, fp.lastCmd, maxFpCommands-1)
	sayTime := fp.cmdTime[idx]
	curTime := curTime()

	if fp.locked > curTime {
		seconds := (fp.locked - curTime) / 1000
		if err := ds.svcPrintf(printChat, "You can't talk for %v more seconds\n", seconds); err != nil {
			return true, err
		}
		return true, nil
	}

	fp_persecond := uint64(iBound(1, ds.qtv.qvs.Get("fp_persecond").Int, 999999))
	if sayTime != 0 && curTime-sayTime < fp_persecond*1000 {
		fp_secondsdead := uint64(iBound(1, ds.qtv.qvs.Get("fp_secondsdead").Int, 999999))
		fp.locked = curTime + 1000*fp_secondsdead
		fp.warnings += 1 // collected but unused stat
		if err := ds.svcPrintf(printChat, "FloodProt: You can't talk for %v more seconds\n", fp_secondsdead); err != nil {
			return true, err
		}
		return true, nil
	}

	fp.cmdTime[idx] = curTime
	fp_messages := iBound(1, ds.qtv.qvs.Get("fp_messages").Int, maxFpCommands)
	fp.lastCmd = (idx + 1) % fp_messages
	return false, nil
}

func (ds *dStream) sayClientCmd(tr *tokenizerResult) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.sayClientCmd:") }()

	if flooder, err := ds.isSayFlood(); err != nil {
		return err
	} else if flooder {
		return nil
	}

	us := ds.linkedUs
	args := unquote(tr.Args())
	sayGame := false
	prefix := ""

	switch {
	case tr.Argv(0) == "say_game":
		sayGame = true
	case strings.HasPrefix(args, "say_game "):
		sayGame = true
		prefix = "say_game "
		args = strings.TrimPrefix(args, "say_game ")
	}

	// Convert say_game to say if upstream is demo file.
	if us.isDemo() {
		sayGame = false
	}

	if sayGame {
		// Do not let flood upstream buffer with chat,
		// let some space for essential things like user list notifications and such.
		if us.wb.Available() < 1024*4 {
			return ds.svcPrintf(printHigh, "say_game failed\n")
		}
		if err := us.sendClcStringCmdf("%s \"%s#%d:%s: %s\"", tr.Argv(0), prefix, ds.id, ds.name(), args); err != nil {
			if errDs := ds.svcPrintf(printHigh, "say_game failed\n"); errDs != nil {
				ds.cancel() // Tell downstream to die.
			}
			if getUStreamSendNetMsgError(err) == nil {
				err = nil // Ignore minor errors in attempt to keep downstream alive.
			}
			return err
		}
		return nil
	}

	s := fmt.Sprintf("#0:qtv_%s:#%d:%s: %s", tr.Argv(0), ds.id, ds.name(), args)
	if len(s) >= 1024 {
		return ds.svcPrintf(printHigh, "say failed\n")
	}

	msg := us.qp.w.Clear()
	us.qp.putSvcPrintf(msg, printChat, s)
	if msg.error {
		return ds.svcPrintf(printHigh, "say failed\n")
	}

	us.assertMutexIsLocked()
	for _, d := range us.linkedDs {
		if err := d.sendMVDMessageEx(msg, mvdMsgAll, playersMaskAll, defaultCheckAvail, 0); err != nil {
			if errors.Is(err, errCheckAvail) {
				log.Trace().Err(multierror.Prefix(err, "dStream.sayClientCmd:")).Msg("")
				continue // Ignore error, keep downstream alive.
			}
			d.cancel() // Tell downstream to die.
			continue
		}
	}
	return nil
}

// Performs various checks about client's name like invalid characters or reserved words
// or client with such name already exists.
func (ds *dStream) sanitizeClientName() (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.sanitizeClientName:") }()

	cur := ds.userInfo.Get("name")
	new := replaceAll(cur, ' '|1<<7, ' ') // Replace quake "high" spaces with normal.
	new = strings.TrimSpace(new)

	if cur != new {
		ds.userInfo.Set("name", new)
		new = ds.userInfo.Get("name")
	}

	if new == "" || new == "console" ||
		strings.ContainsAny(new, "#:") ||
		strings.Contains(new, "&c") || strings.Contains(new, "&r") {
		ds.userInfo.Set("name", "unnamed")
		new = ds.userInfo.Get("name")
	}

	us := ds.linkedUs

	dup := false
	us.assertMutexIsLocked()
	for _, d := range us.linkedDs {
		if d == ds {
			continue // Ignore self.
		}

		if dName := d.userInfo.Get("name"); dName == new {
			dup = true
			break
		}
	}

	if !dup {
		return nil // There is no other user with the same name.
	}

	// Skip prefix from the name, if any.
	if len(new) >= 3 && new[0] == '(' {
		if idx := strings.IndexByte(new, ')'); idx != -1 {
			new = new[idx+1:]
		}
	}

outer:
	for {
		prefixed := fmt.Sprintf("(%v)%-.25s", randomString(8), new)

		us.assertMutexIsLocked()
		for _, d := range us.linkedDs {
			if d == ds {
				continue // Ignore self.
			}

			if dName := d.userInfo.Get("name"); dName == prefixed {
				continue outer // Found duplicate, lets try new prefixed name.
			}
		}
		// Duplicate name not found, lets use this new name.
		if err := ds.userInfo.Set("name", prefixed); err != nil {
			return err
		}
		break
	}
	return nil
}

func (ds *dStream) setInfoClientCmd(tr *tokenizerResult) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.setInfoClientCmd:") }()

	if tr.Argc() == 1 {
		s := ds.userInfo.PrintList()
		return ds.svcPrintf(printHigh, "User info settings:\n%s", s)
	}

	if tr.Argc() != 3 {
		return ds.svcPrintf(printHigh, "Usage: setinfo [ <key> <value> ]\n")
	}

	cur := ds.userInfo.Get(tr.Argv(1))
	ds.userInfo.Set(tr.Argv(1), tr.Argv(2))
	new := ds.userInfo.Get(tr.Argv(1))

	if cur == new {
		return nil
	}

	if tr.Argv(1) == "name" {
		if err := ds.sanitizeClientName(); err != nil {
			return err
		}
		if err := ds.userListUpdateBroadCast(qulChange); err != nil {
			return err
		}
	}

	return nil
}

func (ds *dStream) usersClientCmd(tr *tokenizerResult) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.usersClientCmd:") }()

	us := ds.linkedUs

	if err := ds.svcPrintf(printHigh, "userid name\n------ ----\n"); err != nil {
		return err
	}
	us.assertMutexIsLocked()
	for _, d := range us.linkedDs {
		if err := ds.svcPrintf(printHigh, "%6d %s\n", d.id, d.name()); err != nil {
			return err
		}
	}
	return ds.svcPrintf(printHigh, "%v total users\n", len(us.linkedDs))
}

type qtvUserListAction int

const (
	qulInvalid qtvUserListAction = 0 // Unused.
	qulAdd     qtvUserListAction = 1 // User joined.
	qulChange  qtvUserListAction = 2 // User changed name.
	qulDel     qtvUserListAction = 3 // User dropped.
)

func (ds *dStream) userListActionToString(action qtvUserListAction, isPrefix bool) string {
	var prefix string
	if isPrefix {
		prefix = "//"
	}

	switch action {
	case qulAdd, qulChange:
		return fmt.Sprintf("%squl %d %d \"%s\"\n", prefix, action, ds.id, ds.name())
	case qulDel:
		return fmt.Sprintf("%squl %d %d\n", prefix, action, ds.id)
	}
	return ""
}

func (ds *dStream) userListActionToMsg(msg *netMsgW, action qtvUserListAction) {
	us := ds.linkedUs
	s := ds.userListActionToString(action, true)
	us.qp.putStuffTextf(msg, s)
}

// Notify about user list change all downstreams and upstream.
func (ds *dStream) userListUpdateBroadCast(action qtvUserListAction) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.userListUpdateBroadCast:") }()

	us := ds.linkedUs

	if err := us.sendUserListUpdateToUpstream(ds, action); err != nil {
		return err
	}

	msg := us.qp.w.Clear()

	ds.userListActionToMsg(msg, action)
	if msg.WPos() == 0 {
		return nil // Nothing to send.
	}

	us.assertMutexIsLocked()
	for _, d := range us.linkedDs {
		if action == qulDel && d == ds {
			continue // We are trying to notify what 'ds' is closing, do not try to notify 'ds' itself.
		}

		if (d.qtvEzQuakeExt & qtvEzQuakeExtQtvUserList) == 0 {
			continue
		}
		if err := d.sendMVDMessageEx(msg, mvdMsgRead, playersMaskAll, defaultCheckAvail, 0); err != nil {
			if errors.Is(err, errCheckAvail) {
				log.Trace().Err(multierror.Prefix(err, "dStream.userListUpdateBroadCast:")).Msg("")
				continue // Ignore error, keep downstream alive.
			}
			d.cancel() // Tell downstream to die.
			continue
		}
	}

	return nil
}

// Send initial user list to downstream.
func (ds *dStream) sendInitialUserList() (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.sendInitialUserList:") }()

	if ds.connectedAtLeastOnce {
		return nil
	}

	if (ds.qtvEzQuakeExt & qtvEzQuakeExtQtvUserList) == 0 {
		return nil
	}

	us := ds.linkedUs

	us.assertMutexIsLocked()
	for _, d := range us.linkedDs {
		msg := us.qp.w.Clear()
		d.userListActionToMsg(msg, qulAdd)
		if msg.WPos() == 0 {
			continue
		}
		if err := ds.sendMVDMessageEx(msg, mvdMsgRead, playersMaskAll, defaultCheckAvail, 0); err != nil {
			if errors.Is(err, errCheckAvail) {
				log.Trace().Err(multierror.Prefix(err, "dStream.sendInitialUserList:")).Msg("")
				return nil // Ignore error, keep downstream alive.
			}
			ds.cancel() // Tell downstream to die.
			return err
		}
	}
	return nil
}
