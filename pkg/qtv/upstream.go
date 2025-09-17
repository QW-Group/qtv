package qtv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/markphelps/optional"
	"github.com/qqshka/ringbuffer"
	"github.com/qw-group/qtv-go/pkg/info"
	"github.com/qw-group/qtv-go/pkg/qfs"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/sha3"
)

//
// Upstream allocation, cancelation, state manipulation, main loop, basic IO handling.
// Initial headers parsing.
//

var (
	errInvalidProtocol  = errors.New("invalid protocol")
	errNoSuchId         = errors.New("no such Id")
	errStreamIsInactive = errors.New("stream is inactive")
)

// Upstream representation object.
type uStream struct {
	// {
	// mu protects fields under curly braces.
	// Purpose of this mutex is to help synchronize dStream and uStream.
	// Particularly when dStream trying to "link" (inserted inside linkedDs map) with uStream it
	// should be done only when uStream state is higher than closing_uStreamState, so state change require locking as well.
	// When we accessing linkedDs we also should lock mutex since both dStream and uStream sharing it.
	mu       ringbuffer.Mutex
	state    uStreamState           // Stream state.
	linkedDs map[dStreamId]*dStream // Linked dStreams we know about.
	// }
	id                  uStreamId                 // Stream id.
	qtv                 *QTV                      // Parent object.
	dsCloseNotifyCh     chan dStreamId            // Linked dStreams notify this uStream about closing.
	dsInputNotifyCh     chan dStreamId            // Linked dStreams notify this uStream about input avalaibality.
	server              string                    // Server string address.
	ussName             string                    // Key for this uStream inside uStreamStorage map, so we could lookup it fast.
	options             uStreamOptions            // Upstream options.
	ctx                 context.Context           // Context for mainLoop().
	cancelFunc          context.CancelFunc        // Cancel func for ctx above.
	io                  uStreamIO                 // Wrapper around IO so we can use TCP or file in some abstract way.
	ioError             bool                      // True if IO error detected.
	ioECh               chan error                // This channel used for receiving IO errors from ioReader/ioWriter goroutines.
	ussNotifyCh         chan<- interface{}        // Send id there when we about to quit so uStreamStorage will do clean up.
	reconnectDelay      time.Duration             // Delay between connections.
	parseTime           uint64                    // Time in milliseconds since we started parsing MVD stream, increased each time we successfully parsed MVD message.
	curTime             uint64                    // Current QTV time in milliseconds.
	rb                  *ringbuffer.RingBuffer    // Input buffer from upstream.
	wb                  *ringbuffer.RingBuffer    // Output buffer to upstream.
	hp                  uHeaderParse              // Initial headers handshake.
	qp                  *qProtocol                // Quake protocol, we stored parsed data from upstream here, also used for generating quake protocol messages.
	mvdHdr              *netMsgW                  // MVD header we use frequently for writing data to downstream(s).
	lastScores          [maxLastScores]lastScores // Lastscores data parsed from MVD stream.
	lastScoresIndex     uint                      // Next Lastscored index.
	updateUStreamInfoAt time.Time                 // Time when we willing to update upstream info next time.
	serverCount         uint32                    // Server count, served locally, server count from upstream ignored since it could be same number (for example for demo file).
}

// Allocates new upstream.
func newUStream(qtv *QTV, ussNotifyCh chan<- interface{}, server string, ussName string, usId uStreamId, options uStreamOptions) (us *uStream, err error) {
	defer func() { err = multierror.Prefix(err, "newUStream:") }()

	if server == "" {
		return nil, errors.New("server not specified")
	}

	usIO, err := newUStreamIO(qtv, server)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(qtv.ctx)

	us = &uStream{
		qtv:             qtv,
		id:              usId,
		server:          server,
		ussName:         ussName,
		io:              usIO,
		options:         options,
		ctx:             ctx,
		cancelFunc:      cancel,
		ussNotifyCh:     ussNotifyCh,
		dsCloseNotifyCh: make(chan dStreamId), // Unbuffered, that important. This way dStream change state to closing only when uStream unlink it (if it was linked).
		dsInputNotifyCh: make(chan dStreamId, qtvMaxClients),
		linkedDs:        map[dStreamId]*dStream{},
		reconnectDelay:  initialReconnectDelay,
		rb:              ringbuffer.NewExtended(qtv.qvs.Get("ustream_read_buf_size").Int, false, true),   // Underlying buffer is two times more.
		wb:              ringbuffer.NewExtended(qtv.qvs.Get("ustream_write_buf_size").Int, false, false), // Underlying buffer is two times more.
		mvdHdr:          newNetMsgW(make([]byte, 16), false),
	}
	us.qp = newQProtocol(us)

	if err := us.setState(usInitial); err != nil {
		us.cancel()
		return nil, err
	}

	return us, nil
}

// Upstream id.
type uStreamId uint32

// Check if 'str' is id and cast it to uStreamId.
func isUsId(str string) (uStreamId, error) {
	id, err := isId(str)
	return uStreamId(id), err
}

// cancel cancels stream's context, this function could be safely executed from any goroutine.
func (us *uStream) cancel() error {
	if us.cancelFunc != nil {
		us.cancelFunc()
	}

	return nil
}

// Start upstream main loop inside new goroutine.
func (us *uStream) run() {
	go func() {
		defer us.cancel()
		defer us.io.Close()
		us.mainLoop(us.ctx)
	}()
}

// Custom per-stream options
type uStreamOptions struct {
	isUser      bool             // User created UpStream, implies it should be closed when no viewers and also we should reuse UpStream if other user trying to create it.
	usPassword  optional.String  // UpStream password, this upstream use that password when connecting to remote server.
	dsPassword  optional.String  // DownStream password, clients should provide it in order to connect to this upstream.
	ingameDelay optional.Float64 // Stream may overwrite 'parse_delay' setting.
	address     optional.String  // Upstream (mvdsv) will use this setting to broadcast it to server-browsers.
}

const (
	maxLastScores = 32
)

type lastScores struct {
	matchDate string // Date of the match.
	matchType string // duel, team, etc.
	mapName   string // Map name: dm6, dm3.
	e1        string // First team/person.
	s1        string // Scores for first team/person.
	e2        string // Second team/person.
	s2        string // Scores for second.
}

type uStreamIO interface {
	Open(ctx context.Context) error
	Close() error
	Read(b []byte) (n int, err error)
	Write(p []byte) (n int, err error)
}

func newUStreamIO(qtv *QTV, server string) (_ uStreamIO, err error) {
	defer func() { err = multierror.Prefix(err, "newUStreamIO:") }()
	if isTcpProtocolOrRemoteFile(server) {
		return newUStreamTCP(qtv, server)
	} else if strings.HasPrefix(server, "file:") {
		return newUStreamFile(qtv, server)
	}
	return nil, errors.New("unknown or unspecified protocol")
}

type uStreamState int

const (
	usInvalid           uStreamState = iota
	usClosing                        // Stream is closing or closed.
	usInitial                        // We about to start connection to upstream or waiting for reconnect.
	usParsingHeader                  // In process of parsing a QTV answer.
	usParsingConnection              // We need to receive some data before will be able stream.
	usActive                         // We are ready to forward/stream data.
)

var (
	uStreamStateToName = map[uStreamState]string{
		usInvalid:           "invalid",
		usClosing:           "closing",
		usInitial:           "initial",
		usParsingHeader:     "parsingHeader",
		usParsingConnection: "parsingConnection",
		usActive:            "active",
	}
)

func (us *uStream) getState() uStreamState {
	return us.state
}

func (us *uStream) setState(state uStreamState) (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.setState:") }()

	us.mu.Lock()
	defer us.mu.Unlock()

	oldState := us.getState()

	// If we active then do not try to set active state second time.
	if oldState == state && state == usActive {
		return nil
	}

	log.Trace().Str("ctx", "uStream").Str("event", "setState").Uint32("id", uint32(us.id)).
		Str("state", uStreamStateToName[state]).Str("oldState", uStreamStateToName[oldState]).Msg("")

	if oldState == usClosing {
		return errors.New("attempting to set state on closing stream")
	}

	us.state = state

	// Ensure proper time.
	us.curTime = curTime()

	switch state {
	case usClosing:
		us.mu.Unlock()
		us.unlinkAllDownstreams()
		us.mu.Lock()
	case usInitial:
		us.parseTime = 0

		us.hp.setState(invalid_uHeaderParseState)
		us.rb.Reset()
		us.wb.Reset()
		us.ioError = false

		us.qp.reset()

		us.mu.Unlock()
		us.forceReconnectLinkedDownstreams(true) // Set safe state for all downstreams.
		us.mu.Lock()

	case usParsingHeader:
		us.hp.setState(version_uHeaderParseState)
	case usParsingConnection:
		us.parseTime = us.curTime + (uint64)(us.expectedIngameDelay()*1000)
		// Reset reader storage. Should be done on each map load.
		us.qp.protocolReaderData.reset()
		us.serverCount += 1 // We use local server count.
		us.mu.Unlock()
		us.sendInitialUserListToUpstream()
		us.mu.Lock()
	case usActive:
		// If we succeed with connection - then we should reset reconnect delay to initial state.
		us.updateReconnectDelay(true)
	default:
		return errors.New("invalid state")
	}
	return nil
}

type uHeaderParse struct {
	state   uHeaderParseState
	headers map[string]string
}

type uHeaderParseState int

// Substates of parsingHeader_uStreamState.
const (
	invalid_uHeaderParseState uHeaderParseState = iota
	version_uHeaderParseState                   // Parsing QTV version line.
	header_uHeaderParseState                    // Parsing QTV headers.
)

func (hp *uHeaderParse) setState(state uHeaderParseState) {
	hp.state = state

	switch hp.state {
	case invalid_uHeaderParseState:
		hp.headers = map[string]string{}
	case version_uHeaderParseState:
		// If upstream password protected then we send connection request two times,
		// so this line will reset headers on first and second send.
		hp.headers = map[string]string{}
	case header_uHeaderParseState:
	default:
	}
}

type uStreamInfo struct {
	Id             uStreamId
	Server         string
	Address        string
	SvInfoHostName string
	MapName        string
	MapNameLong    string
	MatchStatus    string
	UpstreamStatus string
	Protected      bool
	WithPlayers    bool
	Teams          []*uStreamTeamInfo
	Ds             []*dStreamInfo
}

type uStreamTeamInfo struct {
	Name    string
	Score   int
	Players []*uStreamPlayerInfo
}

type uStreamPlayerInfo struct {
	Name  string
	Score int
}

type dStreamInfo struct {
	Name         string
	Id           dStreamId
	ConnectStart uint64
}

// Periodically collect/preprocess upstream info (including linked downstreams) and send it to upstream storage,
// this info used for UDP and HTTP, so intensive locking and processing is not required (good) but data is delayed (bad).
func (us *uStream) updateUStreamInfo() {
	if time.Now().Before(us.updateUStreamInfoAt) {
		return
	}

	us.mu.Lock()

	// Update info periodically at random intervals so we get lesser load on CPU.
	us.updateUStreamInfoAt = time.Now().Add(time.Millisecond * time.Duration(5000+rand.Intn(5000)))

	info := &uStreamInfo{
		Id:             us.id,
		Server:         strings.TrimPrefix(strings.TrimPrefix(us.server, "tcp:"), "file:"),
		Address:        us.address(),
		SvInfoHostName: normalizeText(us.qp.serverInfo.Get("hostname")),
		MapName:        us.qp.serverInfo.Get("map"),
		MapNameLong:    normalizeText(us.qp.mapName),
		MatchStatus:    us.qp.serverInfo.Get("status"),
		Teams:          us.updateUStreamTeamInfo(),
	}

	info.WithPlayers = (len(info.Teams) != 0)

	if matchTag := us.qp.serverInfo.Get("matchtag"); matchTag != "" {
		info.MatchStatus = matchTag + ": " + info.MatchStatus
	}
	info.MatchStatus = normalizeText(info.MatchStatus)

	if us.password() != "" {
		info.Protected = true
	}

	if us.getState() != usActive {
		info.UpstreamStatus = "NOT CONNECTED"
	}

	for _, ds := range us.linkedDs {
		info.Ds = append(info.Ds, &dStreamInfo{
			Name:         ds.name(),
			Id:           ds.id,
			ConnectStart: ds.connectStart,
		})
	}

	us.mu.Unlock()

	us.ussNotifyCh <- info
}

func (us *uStream) updateUStreamTeamInfo() []*uStreamTeamInfo {
	teamPlay := (us.qp.serverInfo.Get("teamplay") != "")
	teams := map[string]*uStreamTeamInfo{}
	for i := 0; i < maxClients; i++ {
		client := &us.qp.players[i]
		if !client.isPlayer() {
			continue
		}
		pl := uStreamPlayerInfo{
			Name:  normalizeText(client.userInfo.Get("name")),
			Score: client.frags,
		}
		teamName := ""
		if teamPlay {
			teamName = normalizeText(client.userInfo.Get("team"))
		}
		t := teams[teamName]
		if t == nil {
			// New team.
			t = &uStreamTeamInfo{Name: teamName}
			teams[teamName] = t
		}
		t.Score += pl.Score
		t.Players = append(t.Players, &pl)
	}
	// Sort team/players so HTTP interface does not waste time on each request for sorting same data.
	sortedTeams := make([]*uStreamTeamInfo, 0, len(teams))
	for _, t := range teams {
		// Sort players per team.
		sort.Slice(t.Players, func(i, j int) bool { return t.Players[i].Score > t.Players[j].Score })
		sortedTeams = append(sortedTeams, t)
	}
	// Sort teams.
	sort.Slice(sortedTeams, func(i, j int) bool { return sortedTeams[i].Score > sortedTeams[j].Score })
	return sortedTeams
}

// Send data to upstream.
func (us *uStream) send(bs ...[]byte) (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.send:") }()

	if us.getState() <= usInitial {
		return errors.New("invalid state")
	}

	for _, b := range bs {
		if _, err := us.wb.WriteFull(b); err != nil {
			return err
		}
	}
	return nil
}

// Send data to upstream.
func (us *uStream) sendPrint(a ...interface{}) (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.sendPrint:") }()

	s := fmt.Sprint(a...)
	return us.send([]byte(s))
}

// Send data to upstream.
func (us *uStream) sendPrintf(format string, a ...interface{}) (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.sendPrintf:") }()

	s := fmt.Sprintf(format, a...)
	return us.send([]byte(s))
}

// Error wrapper, helps to determine source of the error.
type uStreamSendNetMsgError struct {
	error
}

func (me *uStreamSendNetMsgError) Unwrap() error {
	return me.error
}

// Helper for detection if error chain contains uStreamSendNetMsgError error.
func getUStreamSendNetMsgError(err error) (msgErr *uStreamSendNetMsgError) {
	if errors.As(err, &msgErr) {
		return msgErr
	}
	return nil
}

// Send data to upstream.
func (us *uStream) sendNetMsg(msg *netMsgW) (err error) {
	defer func() {
		if err != nil {
			// Wrap error inside uStreamSendNetMsgError, check for it at caller.
			err = &uStreamSendNetMsgError{error: err}
		}
		err = multierror.Prefix(err, "uStream.sendNetMsg:")
	}()

	if msg.error {
		return errors.New("netMsg with error")
	}

	// We does not send binary data to upstream until we complete initial headers handshake.
	if us.getState() < usParsingConnection {
		return errors.New("invalid state")
	}

	if _, err := us.wb.WriteFull(msg.Bytes()); err != nil {
		return err
	}

	return nil
}

// Send clcStringCmd data to upstream.
func (us *uStream) sendClcStringCmdf(format string, a ...interface{}) (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.sendClcStringCmdf:") }()

	if us.getState() < usParsingConnection {
		return errors.New("invalid state")
	}

	cmd := fmt.Sprintf(format, a...)
	if cmd == "" {
		return nil // Nothing to send.
	}

	if len(cmd) >= 1024 {
		return errors.New("too long command")
	}

	msg := us.qp.w.Clear()
	msg.PutUint16(0) // Put fake len value, we will update it few lines below.
	msg.PutByte(byte(clcStringCmd))
	msg.PutString(cmd)
	msg.UpdateUint16At(uint16(msg.WPos()), 0) // Update len with actual value.

	if msg.error {
		return errors.New("netMsg with error")
	}

	if us.wb.Length()+msg.WPos() > us.wb.Capacity() {
		return errors.New("command skipped since it could overflow upstream buffer")
	}

	return us.sendNetMsg(msg)
}

// Check if server address has tcp protocol or file protocol but this file should be accessed with tcp.
func isTcpProtocolOrRemoteFile(server string) bool {
	protocol := protocolFromServerStr(server)
	if protocol == "tcp:" {
		return true
	} else if protocol == "file:" && strings.Contains(server, "@") {
		return true
	}
	return false
}

// If server string contains only numbers then consider this as stream id.
func isId(server string) (uint32, error) {
	if id, err := strconv.ParseUint(server, 10, 32); err != nil {
		return 0, err
	} else {
		return uint32(id), nil
	}
}

// Get protocol from server address.
func protocolFromServerStr(server string) string {
	if strings.HasPrefix(server, "tcp:") {
		return "tcp:"
	} else if strings.HasPrefix(server, "file:") {
		return "file:"
	}
	return ""
}

// Get remote address from server address string.
// tcp:host:port -> host:port
// tcp:streamId@host:port -> host:port
// file:filename.mvd@host:port -> host:port
func addressFromServerStr(protocol string, server string) (string, error) {
	// Validate protocol and skip it.
	if !strings.HasPrefix(server, protocol) {
		return "", multierror.Prefix(errInvalidProtocol, "addressFromServerStr:")
	}
	server = server[len(protocol):]
	// If server string contains '@' then use characters after '@' as server.
	at := strings.LastIndexByte(server, '@')
	if at != -1 {
		server = server[at+1:]
	}
	return server, nil
}

// Get "source" component from the server address string.
// Examples of how server address string should be mapped to source:
// "tcp:hostname:port" -> ""
// "tcp:hostname:port@hostname:port" -> "tcp:hostname:port"
// Note that in the next case we also skip "tcp:" prefix
// "tcp:streamId@hostname:port" -> "streamId"
// "tcp:streamId@hostname2:port2@hostname1:port1" -> "tcp:streamId@hostname2:port2"
// "file:filename.mvd@hostname:port" -> "file:filename.mvd"
func (us *uStream) sourceFromServerAddress() string {
	src := us.server
	at := strings.LastIndexByte(src, '@')
	// If source is not specified then return empty value.
	if at == -1 {
		return "" // Empty source.
	}
	// Cut last "host:port" value, there could be more in case of chaining.
	src = src[0:at]
	// Check if we have more "host:port", if we do then return as is.
	if strings.LastIndexByte(src, '@') != -1 {
		return src
	}
	// Check if we does NOT have "tcp:" prefix then return as is.
	if !strings.HasPrefix(src, "tcp:") {
		return src
	}
	// Cut "tcp:" prefix.
	src = src[4:]
	return src
}

// Get password for this upstream. Upstream could have either own password or global qtv_password.
func (us *uStream) password() string {
	if pass, err := us.options.dsPassword.Get(); err == nil {
		return pass
	}
	return us.qtv.qvs.Get("qtv_password").Str
}

// Get address for this upstream. Upstream could have either own address or global qtv address.
func (us *uStream) address() string {
	if addr, err := us.options.address.Get(); err == nil {
		return addr
	}
	address := us.qtv.qvs.Get("address")
	return address.Str
}

// Get upstream user info for QTV headers handshake.
func (us *uStream) userInfo() string {
	ui := info.NewInfo()

	address := us.address()
	hostname := us.qtv.hostName()
	name := fmt.Sprintf("%v (%v)", hostname, us.id)

	ui.Set("name", name)
	if address != "" {
		ui.Set("address", address)
		ui.Set("streamid", us.id)
	}

	return ui.String()
}

// Send QTV versions during initial headers handshake.
func (us *uStream) sendVersion() (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.sendVersion:") }()

	return us.sendPrint("QTV\n", "VERSION: 1\n")
}

// Send desired QTV source during initial headers handshake.
func (us *uStream) sendSource() (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.sendSource:") }()

	source := us.sourceFromServerAddress()
	if source == "" {
		return us.sendPrint("RECEIVE\n")
	} else {
		return us.sendPrintf("SOURCE: %v\n", source)
	}
}

// Send QTV userinfo during initial headers handshake.
func (us *uStream) sendUserInfo() (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.sendUserInfo:") }()

	if ui := us.userInfo(); ui != "" {
		return us.sendPrintf("USERINFO: \"%v\"\n", ui)
	}
	return nil
}

// Send QTV auth headers during initial headers handshake.
// If remote server password protected we send AUTH two times,
// First time we send AUTH with all supported auth methods to the server,
// server choose AUTH method it prefers and send us reply with chosen AUTH and random CHALLENGE,
// we process challenge and send reply with that AUTH and PASSWORD (hash or the challenge + password).
func (us *uStream) sendAuth(authMethod string, challenge string) (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.sendAuth:") }()

	pass := us.options.usPassword.OrElse("")

	switch authMethod {
	case "": // Auth is empty, send all supported AUTH methods to remote server.
		return us.sendPrint("AUTH: SHA3_512\n", "AUTH: PLAIN\n", "AUTH: NONE\n")
	case "PLAIN":
		return us.sendPrintf("AUTH: %v\nPASSWORD: \"%v\"\n", authMethod, pass)
	case "SHA3_512":
		if len(challenge) < maxAuthChallengeSize {
			return fmt.Errorf("invalid challenge for auth method %v", authMethod)
		}
		hash := sha3.Sum512([]byte(challenge + pass))
		return us.sendPrintf("AUTH: %v\nPASSWORD: \"%x\"\n", authMethod, hash)
	case "NONE":
		return us.sendPrintf("AUTH: %v\nPASSWORD: \n", authMethod)
	default:
		return fmt.Errorf("auth method %v unknown", authMethod)
	}
}

// Send QTV initial headers handshake.
func (us *uStream) sendQTVConnectionRequest(authMethod string, challenge string) (err error) {
	defer func() { err = multierror.Prefix(err, "sendQTVConnectionRequest:") }()

	if err := us.setState(usParsingHeader); err != nil {
		return err
	}
	if err := us.sendVersion(); err != nil {
		return err
	}
	if err := us.sendSource(); err != nil {
		return err
	}
	if err := us.sendUserInfo(); err != nil {
		return err
	}
	if err := us.sendAuth(authMethod, challenge); err != nil {
		return err
	}
	return us.sendPrint("\n")
}

// Get downstream linked with this upstream either by id or name.
func (us *uStream) getDownstreamByNameOrId(s string) *dStream {
	us.assertMutexIsLocked()

	if dsId, err := isDsId(s); err == nil {
		if ds := us.linkedDs[dsId]; ds != nil {
			return ds
		}
		// Name may look like id (string of diggits only), so continue below.
	}

	for _, ds := range us.linkedDs {
		if ds.name() == s {
			return ds
		}
	}
	return nil
}

// Returns true if user created stream and no viewers.
func (us *uStream) isInactive() bool {
	us.mu.Lock()
	defer us.mu.Unlock()

	if us.options.isUser {
		return len(us.linkedDs) == 0
	}
	return false
}

// Unlink linked downstream.
func (us *uStream) unlinkDownstream(dsId dStreamId) {
	us.mu.Lock()
	defer us.mu.Unlock()

	ds := us.linkedDs[dsId]
	if ds != nil {
		ds.userListUpdateBroadCast(qulDel)
	}

	delete(us.linkedDs, dsId)
}

// Link upstream with downstream, downstream also linked with upstream (so they both know each other).
func (us *uStream) linkDownstream(ds *dStream) (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.linkDownstream:") }()

	us.mu.Lock()
	defer us.mu.Unlock()

	if us.getState() < usInitial {
		return errors.New("could not link downstream, we about to quit")
	}

	// Link upstream with downstrteam.
	us.linkedDs[ds.id] = ds

	// Link downstream with upstream.
	if err := ds.linkUpstream(us); err != nil {
		// Unlink if something went wrong.
		delete(us.linkedDs, ds.id)
		return err
	}

	return nil
}

// Returns count of linked downstreams.
func (us *uStream) linkedDownstreamCount() int {
	us.mu.Lock()
	defer us.mu.Unlock()

	return len(us.linkedDs)
}

// Downstream notify upstream about closing with this method.
// This method should block until upstream get that notify, this allows upstream change downstream's state without complicated locking.
func (us *uStream) downstreamCloseNotify(dsId dStreamId) {
	if us == nil {
		return
	}
	// Do not using mutex since uStream should be alive while at least one linked dStream alive.
	us.dsCloseNotifyCh <- dsId
}

// Downstream notify upstream about new input with this method.
func (us *uStream) downstreamInputNotify(dsId dStreamId) {
	if us == nil {
		return
	}
	// Do not using mutex since uStream should be alive while at least one linked dStream alive.
	us.dsInputNotifyCh <- dsId
}

// Tell downstreams to reconnect by reseting state to needInitialData_dStreamState.
// That happens on map change so clients could download required data.
// forceAll is used when we want to set state for ALL downstreams, that usefull when upstream disconnected.
func (us *uStream) forceReconnectLinkedDownstreams(forceAll bool) {
	us.mu.Lock()
	defer us.mu.Unlock()

	for _, ds := range us.linkedDs {
		// If downstream support required extension - tell to reconnect.
		if (ds.qtvEzQuakeExt&qtvEzQuakeExtDownload) != 0 || forceAll {
			if err := ds.setState(dsNeedInitialData); err != nil {
				log.Trace().Err(multierror.Prefix(err, "uStream.forceReconnectLinkedDownstreams:")).
					Str("ctx", "uStream").Uint32("id", uint32(us.id)).Uint32("dsId", uint32(ds.id)).Msg("")
				ds.cancel() // Tell downstream to die.
			}
		}
	}
}

// Check if we have input from downstreams and process it.
func (us *uStream) processDownstreamsInput(ctx context.Context) (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.processDownstreamsInput:") }()

	us.mu.Lock()
	defer us.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return nil
		case dsId := <-us.dsInputNotifyCh:
			if us.getState() != usActive {
				continue // Skip input processing for that dStream, upstream is not ready yet but try to process more notifications.
			}
			ds := us.linkedDs[dsId]
			if ds == nil {
				continue // Probably unlinked.
			}
			if err := ds.processClientInput(); err != nil {
				log.Trace().Err(multierror.Prefix(err, "uStream.processDownstreamsInput:")).Uint32("id", uint32(us.id)).Uint32("dsId", uint32(ds.id)).Msg("")
				ds.cancel() // Tell downstream to die.
				// Consider such situtation as upstream error, caller probably should initiate reconnect.
				if getUStreamSendNetMsgError(err) != nil {
					return err
				}
				continue
			}
		default:
			return nil
		}
	}
}

// Perform download for downstreams if required.
func (us *uStream) processDownstreamsDownload() (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.processDownstreamsDownload:") }()

	us.mu.Lock()
	defer us.mu.Unlock()

	for _, ds := range us.linkedDs {
		if ds.download.IsActive() && ds.getState() >= dsNeedInitialData {
			if err := ds.nextDownload(); err != nil {
				log.Trace().Err(multierror.Prefix(err, "uStream.processDownstreamsDownload:")).Uint32("id", uint32(us.id)).Uint32("dsId", uint32(ds.id)).Msg("")
				ds.cancel() // Tell downstream to die.
				continue
			}
		}
	}

	return nil
}

// Cancel all linked downstreams.
func (us *uStream) cancelLinkedDownstreams() {
	us.mu.Lock()
	defer us.mu.Unlock()

	for _, ds := range us.linkedDs {
		ds.cancel() // Tell downstream to die.
	}
}

// Unlink all downstreams from upstream.
func (us *uStream) unlinkAllDownstreams() (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.unlinkAllDownstreams:") }()

	if us.linkedDownstreamCount() == 0 {
		return nil // All unlinked.
	}

	// Tell downstreams to stop.
	us.cancelLinkedDownstreams()

	doneTimeout := time.NewTimer(3 * time.Second)
	defer doneTimeout.Stop()

	for {
		if us.linkedDownstreamCount() == 0 {
			return nil // All unlinked.
		}

		select {
		case dsId := <-us.dsCloseNotifyCh:
			us.unlinkDownstream(dsId)
		case <-doneTimeout.C:
			return errors.New("could not gracefully do uStream.unlinkAllDownstreams")
		}
	}
}

// Check for close notifies from downstreams and unlink them.
func (us *uStream) unlinkClosedDownstreams() {
	for {
		select {
		case dsId := <-us.dsCloseNotifyCh:
			us.unlinkDownstream(dsId)
		default:
			return
		}
	}
}

// Upstream's main loop.
func (us *uStream) mainLoop(ctx context.Context) (err error) {
	defer func() {
		err = multierror.Prefix(err, "uStream.mainLoop:")
		log.Trace().Str("ctx", "uStream").Str("name", "mainLoop").Uint32("id", uint32(us.id)).Str("event", "out").Err(err).Msg("")
	}()

	defer func() {
		us.ussNotifyCh <- us.id // Signaling uStreamStorage to remove us.
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	reconnectTimer := expiredTimer()
	defer reconnectTimer.Stop()

	defer us.setState(usClosing)

outer:
	for {
		err := us.mainLoopIteration(ctx)
		// Do not log error if we going to exit function anyway.
		if err != nil && ctx.Err() == nil {
			log.Err(multierror.Prefix(err, "uStream.mainLoop:")).Str("ctx", "uStream").Str("event", "disconnect").Uint32("id", uint32(us.id)).Msg("")
		}
		us.updateReconnectDelay(false)
		reconnectTimer.Reset(us.reconnectDelay)
		for {
			us.curTime = curTime()

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-reconnectTimer.C:
				continue outer
			default:
			}

			if us.isInactive() {
				return errStreamIsInactive
			}

			us.unlinkClosedDownstreams()

			if err := us.processDownstreamsInput(ctx); err != nil {
				return err
			}
			if err := us.processDownstreamsDownload(); err != nil {
				return err
			}
			us.updateUStreamInfo()

			us.qtv.tick.L.Lock()
			us.qtv.tick.Wait()
			us.qtv.tick.L.Unlock()
		}
	}
}

func (us *uStream) mainLoopIteration(ctx context.Context) (err error) {
	defer func() {
		err = multierror.Prefix(err, "uStream.mainLoopIteration:")
		log.Trace().Str("ctx", "uStream").Str("name", "mainLoopIteration").Uint32("id", uint32(us.id)).Str("event", "out").Msg("")
	}()

	defer us.setState(usInitial)

	log.Info().Str("ctx", "uStream").Str("event", "connect").Uint32("id", uint32(us.id)).Str("server", us.server).Msg("")

	if err := us.setState(usInitial); err != nil {
		return err
	}

	us.ioECh = make(chan error, 2)

	g := multierror.Group{}
	defer g.Wait()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := us.io.Open(ctx); err != nil {
		return err
	}
	defer us.io.Close()

	g.Go(func() error { return us.ioReader(ctx) })
	g.Go(func() error { return us.ioWriter(ctx) })

	if us.isDemo() {
		// In the case of the demo we start parsing connection right away.
		if err := us.setState(usParsingConnection); err != nil {
			return err
		}
	} else if err := us.sendQTVConnectionRequest("", ""); err != nil {
		return err
	}

	for {
		us.curTime = curTime()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-us.ioECh:
			us.ioError = true
			// If we successfully completed headers parsing (connected to remote) then we better
			// process incoming buffer and only then report error, that mostly useful for demos.
			// If we did not complete connection then report error right now.
			if us.getState() < usParsingConnection {
				return err
			}
		default:
		}
		// Checking IO notify separately so it does not affect error detection.
		select {
		case <-us.rb.WC:
			//Empty.
		default:
		}

		if us.isInactive() {
			return errStreamIsInactive
		}

		us.unlinkClosedDownstreams()

		if err := us.processDownstreamsInput(ctx); err != nil {
			return err
		}
		if err := us.processDownstreamsDownload(); err != nil {
			return err
		}

		if err := us.processRead(); err != nil {
			return err
		}

		us.updateUStreamInfo()

		// Sleep preventing reader to wake up us too frequently, that reduces CPU usage a lot.
		// We use centralized tick so all goroutines wake up more or less at the same time,
		// do required work and go to sleep again. If goroutines wake up at different times
		// that consumes a lot more CPU.
		us.qtv.tick.L.Lock()
		us.qtv.tick.Wait()
		us.qtv.tick.L.Unlock()
	}
}

// Process upstream read.
// Initially we parse headers (handshake), later we parse binary data (MVD sstream).
func (us *uStream) processRead() (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.processRead:") }()

	switch us.getState() {
	case usParsingHeader:
		return us.parseHeader()
	case usParsingConnection, usActive:
		return us.parseMVD()
	default:
		return errors.New("invalid state")
	}
}

// Perform initial headers handshake between this QTV and remote server (most of the time MVDSV).
func (us *uStream) parseHeader() (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.parseHeader:") }()

	// We does not know how much bytes to read until new line so we get whole buffer.
	bb := us.rb.BytesOneReader()
	discard := 0

	for us.getState() == usParsingHeader {
		nl := bytes.IndexByte(bb, '\n')
		if nl < 0 {
			break
		}
		// Accumulate how much bytes to skip from ringbuffer.
		discard += nl + 1
		// Data until new line.
		b := bb[:nl]
		// Data after new line.
		bb = bb[nl+1:]

		switch us.hp.state {
		case version_uHeaderParseState:
			if err := us.parseVersion(string(b)); err != nil {
				return err
			}
		case header_uHeaderParseState:
			if err := us.parseOneHeader(string(b)); err != nil {
				return err
			}
		default:
			return errors.New("invalid state")
		}
	}

	if err := us.rb.Discard(discard); err != nil {
		return err
	}
	return nil
}

var qtvGreeting = "QTVSV "

// Parse QTV version line.
func (us *uStream) parseVersion(s string) (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.parseVersion:") }()

	if !strings.HasPrefix(s, qtvGreeting) {
		return errors.New("not a QTV server")
	}

	// Skip greeting prefix and get version value.
	s = s[len(qtvGreeting):]
	s = strings.TrimSpace(s)

	// Version is a float value, but we compare only major version number here.
	if version, err := strconv.ParseFloat(s, 64); err != nil {
		return err
	} else if int(version) != int(qtvVersion) {
		return fmt.Errorf("QTV upstream doesn't support a compatible protocol version, expected %.2f, got %.2f", qtvVersion, version)
	}

	// Now we are ready to parse real headers.
	us.hp.setState(header_uHeaderParseState)
	// VERSION not really a header here (while it should), but it did not really used, so should not hurt.
	us.hp.headers["VERSION"] = s

	log.Trace().Str("ctx", "uStream").Str("event", "header").Uint32("id", uint32(us.id)).Str("header", "VERSION").Str("value", s).Msg("")

	return nil
}

// Parse one header line.
func (us *uStream) parseOneHeader(s string) (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.parseVersion:") }()

	// Empty 's' means no more headers = end of request, unless we need to send challenge reply.
	if s == "" {
		// Reply to the challenge if any.
		if auth, ok := us.hp.headers["AUTH"]; ok {
			challenge := us.hp.headers["CHALLENGE"]
			return us.sendQTVConnectionRequest(auth, challenge)
		}
		if _, ok := us.hp.headers["BEGIN"]; !ok {
			return errors.New("QTV upstream sent no begin command")
		}
		return us.setState(usParsingConnection)
	}

	// 's' is in form of "HEADERNAME: VALUE".
	var n, v string
	nv := strings.SplitN(s, ":", 2)
	switch len(nv) {
	case 2:
		n, v = nv[0], nv[1] // Name and value.
	case 1:
		n = nv[0] // Name only, value is empty.
	default:
		return errors.New("unexpected len")
	}
	// Header name.
	n = strings.TrimSpace(n)
	// Value, could be quoted or not quoted, usefull for CHALLENGE since it could contain quotes as value.
	qv := strings.TrimSpace(v)
	// Unquoted value.
	v = unquote(qv)

	log.Trace().Str("ctx", "uStream").Str("event", "header").Uint32("id", uint32(us.id)).Str("header", n).Str("value", qv).Msg("")

	switch n {
	case "AUTH":
		us.hp.headers[n] = v
	case "CHALLENGE":
		us.hp.headers[n] = qv
	case "BEGIN":
		us.hp.headers[n] = v
	case "COMPRESSION":
		return errors.New("QTV upstream wrongly used compression")
	case "PERROR", "TERROR", "ERROR":
		// We do not consider PERROR as permanent, just disconnect and try connect again since that error could be fixed by upstream.
		return fmt.Errorf("QTV upstream %v: %v", n, v)
	case "ASOURCE", "ADEMO", "PRINT":
		// Ignore it.
	default:
		log.Debug().Str("ctx", "uStream").Str("event", "headerUnrecognized").Uint32("id", uint32(us.id)).Str("header", n).Str("value", qv).Msg("")
	}
	return nil
}

const (
	initialReconnectDelay = 2 * time.Second
)

func (us *uStream) updateReconnectDelay(success bool) {
	if success {
		us.reconnectDelay = initialReconnectDelay
	} else {
		us.reconnectDelay *= 2
	}

	if us.reconnectDelay.Hours() > 1 {
		us.reconnectDelay = time.Hour
	}
}

// Returns gameDir which is not empty (defaults to qw), also do not allow tricks with absolute/relative paths.
func (us *uStream) gameDir() string {
	gameDir := us.qp.gameDir
	if !qfs.IsSimplePath(gameDir) {
		return "qw"
	}
	return gameDir
}

// Send user list update to upstream.
func (us *uStream) sendUserListUpdateToUpstream(ds *dStream, action qtvUserListAction) (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.sendUserListUpdateToUpstream:") }()

	s := ds.userListActionToString(action, false)
	if err := us.sendClcStringCmdf(s); err != nil {
		if getUStreamSendNetMsgError(err) == nil {
			err = nil // Ignore minor errors.
		}
		return err
	}
	return nil
}

// Send initial user list to upstream.
func (us *uStream) sendInitialUserListToUpstream() (err error) {
	defer func() { err = multierror.Prefix(err, "uStream.sendInitialUserListToUpstream:") }()

	us.mu.Lock()
	defer us.mu.Unlock()

	for _, ds := range us.linkedDs {
		if err := us.sendUserListUpdateToUpstream(ds, qulAdd); err != nil {
			return err
		}
	}

	return nil
}

// Check if mutex is at proper state, this is not 100% accurate but eventually it should trigger an error if we have programmatic error.
// Technically this is not required.
func (us *uStream) assertMutexIsLocked() {
	// 'us.mu' should be locked by caller(s) here, we should not be able to lock mutex ever.
	if us.mu.TryLock() {
		us.mu.Unlock()
		log.Panic().Str("ctx", "uStream").Uint32("id", uint32(us.id)).Msg("mutex should be locked")
	}
}

// Returns true it upstream underlying IO source is local file.
func (us *uStream) isDemo() bool {
	_, ok := us.io.(*uStreamFile)
	return ok
}

// Returns true if upstream underlying IO source is TCP (even if at remote server this is file).
func (us *uStream) isTCP() bool {
	_, ok := us.io.(*uStreamTCP)
	return ok
}

// Reader goroutine. Perform reading from abstract IO object to our read buffer.
func (us *uStream) ioReader(ctx context.Context) (err error) {
	defer func() {
		log.Trace().Str("ctx", "uStream").Str("name", "ioReader").Uint32("id", uint32(us.id)).Str("event", "out").Msg("")
	}()
	defer func() { err = multierror.Prefix(err, "uStream.ioReader:") }()

	_, err = us.rb.ReadFromWithContext(ctx, us.io)
	if err == nil {
		err = io.EOF
	}
	us.ioECh <- multierror.Prefix(err, "uStream.ioReader:")
	return err
}

// Writer goroutine. Perform writing to abstract IO object from our write buffer.
func (us *uStream) ioWriter(ctx context.Context) (err error) {
	defer func() {
		log.Trace().Str("ctx", "uStream").Str("name", "ioWriter").Uint32("id", uint32(us.id)).Str("event", "out").Msg("")
	}()
	defer func() { err = multierror.Prefix(err, "uStream.ioWriter:") }()

	_, err = us.wb.WriteToWithContext(ctx, us.io)
	if err == nil {
		err = io.EOF
	}
	us.ioECh <- multierror.Prefix(err, "uStream.ioWriter:")
	return err
}
