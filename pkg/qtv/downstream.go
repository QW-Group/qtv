package qtv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/markphelps/optional"
	"github.com/qqshka/ringbuffer"
	"github.com/qw-group/qtv-go/pkg/info"
	"github.com/rs/zerolog/log"
	"go.uber.org/atomic"
)

//
// Downstream allocation, cancelation, state manipulation, main loop, basic IO handling.
// Initial headers parsing.
//

// Downstream representation object.
type dStream struct {
	id                   dStreamId              // Stream id.
	qtv                  *QTV                   // Parent object.
	state                dStreamState           // Stream state.
	cancelFunc           context.CancelFunc     // Context cancel func.
	canceled             atomic.Bool            // True if context was canceled with cancel func.
	conn                 net.Conn               // Network connection with downstream.
	ioECh                chan error             // This channel used for receiving IO errors from ioReader/ioWriter goroutines.
	dssNotifyCh          chan<- dStreamId       // Send id there when we about to quit so dStreamStorage will do clean up.
	rb                   *ringbuffer.RingBuffer // Input buffer from downsstream.
	wb                   *ringbuffer.RingBuffer // Output buffer to downstream.
	headers              map[string]string      // Initial handshake headers.
	qtvEzQuakeExt        uint32                 // ezQuake extensions mask this downstream support.
	authChallenge        []byte                 // Auth challenge used for this downstream (if any).
	flushAndExit         bool                   // True when we have to flush output buffer before closing connection.
	flushData            *strings.Reader        // Extra data we have to flush besides output buffer.
	linkedUs             *uStream               // Non nil if we successfully linked (registered) with upstream.
	download             dStreamDownload        // Download related data.
	connectedAtLeastOnce bool                   // True if connection sequence was completed at least once.
	userInfo             info.InfoTs            // Client userinfo (used for name and similar things).
	fp                   floodProtect           // Say flood protection data.
	pov                  optional.Int           // Player slot this dStream tracks, optional.
	followId             dStreamId              // id of followed dStream, zero if not following.
	connectStart         uint64                 // Time when donstream connected.
}

// Allocates new downsteam.
func newDStream(qtv *QTV, dssNotifyCh chan<- dStreamId, conn net.Conn, dsId dStreamId) (ds *dStream, err error) {
	defer func() { err = multierror.Prefix(err, "newDStream:") }()

	ctx, cancel := context.WithCancel(qtv.ctx)

	ds = &dStream{
		qtv:          qtv,
		id:           dsId,
		cancelFunc:   cancel,
		conn:         conn,
		dssNotifyCh:  dssNotifyCh,
		ioECh:        make(chan error, 2),
		rb:           ringbuffer.NewExtended(qtv.qvs.Get("dstream_read_buf_size").Int, false, true),  // Underlying buffer is two times more.
		wb:           ringbuffer.NewExtended(qtv.qvs.Get("dstream_write_buf_size").Int, true, false), // Underlying buffer is two times more.
		connectStart: curTime(),
	}

	go func() {
		defer ds.cancel()
		ds.mainLoop(ctx)
	}()

	return ds, nil
}

// Downsteam id.
type dStreamId uint32

// Check if 'str' is id and cast it to dStreamId.
func isDsId(str string) (dStreamId, error) {
	id, err := isId(str)
	return dStreamId(id), err
}

// Returns true if stream was canceled.
func (ds *dStream) isCanceled() bool {
	return ds.canceled.Load()
}

// cancel cancels stream's context, this function could be safely executed from any goroutine.
func (ds *dStream) cancel() error {
	if !ds.canceled.CAS(false, true) {
		return nil
	}

	if ds.cancelFunc != nil {
		ds.cancelFunc()
	}

	if ds.conn != nil {
		ds.conn.Close()
	}

	return nil
}

// Downstream download related info.
type dStreamDownload struct {
	file    *os.File // File to download by client.
	size    int64    // How much bytes bytes to download.
	current int64    // How much bytes currently downloaded by client.
}

// Returns true if download in process for downstream.
func (dl *dStreamDownload) IsActive() bool {
	return dl.file != nil
}

// Close download(if any) and resets state to default.
func (dl *dStreamDownload) Reset() {
	if dl.file != nil {
		dl.file.Close()
	}
	*dl = dStreamDownload{}
}

type dStreamState int

const (
	dsInvalid         dStreamState = iota
	dsClosing                      // Stream is closing or closed.
	dsParsingHeader                // Initial headers handshake.
	dsNeedInitialData              // Handshake complete, need initial MVD data. (next state for non ezQuake clients is active_dStreamState)
	dsNeedSoundList                // Waiting for client requesting sound list. (optional, used by ezQuake clients)
	dsNeedModelList                // Waiting for client requesting model list. (optional, used by ezQuake clients)
	dsNeedSpawn                    // Waiting for client requesting spawn. (optional, used by ezQuake clients)
	dsActive                       // Client is active, ready to forward data to the client.
)

var (
	dStreamStateToName = map[dStreamState]string{
		dsInvalid:         "invalid",
		dsClosing:         "closing",
		dsParsingHeader:   "parsingHeader",
		dsNeedInitialData: "needInitialData",
		dsNeedSoundList:   "needSoundList",
		dsNeedModelList:   "needModelList",
		dsNeedSpawn:       "needSpawn",
		dsActive:          "active",
	}
)

func (ds *dStream) getState() dStreamState {
	return ds.state
}

func (ds *dStream) setState(state dStreamState) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.setState:") }()

	if state == dsClosing {
		// Let linked uStream know we about to quit.
		// This will block, we do it on purpose to reduce synchronization between dStream and uStream.
		// By doing so after dStream linked to uStream we can use dStream fields/methods inside uStream mainLoop without locking.
		ds.linkedUs.downstreamCloseNotify(ds.id)
	}

	oldState := ds.getState()

	log.Trace().Str("ctx", "dStream").Str("event", "setState").Uint32("id", uint32(ds.id)).
		Str("state", dStreamStateToName[state]).Str("oldState", dStreamStateToName[oldState]).Msg("")

	if oldState == dsClosing {
		return errors.New("attempting to set state on closing stream")
	}

	ds.state = state

	switch state {
	case dsClosing:
		ds.download.Reset()
	case dsParsingHeader:
		ds.conn.SetDeadline(time.Now().Add(5 * time.Second))
		ds.rb.Reset()
		ds.wb.Reset()
		ds.headers = map[string]string{}
	case dsNeedInitialData:
	case dsNeedSoundList:
	case dsNeedModelList:
	case dsNeedSpawn:
	case dsActive:
		ds.connectedAtLeastOnce = true
	default:
		return errors.New("invalid state")
	}
	return nil
}

// Downstream name.
func (ds *dStream) name() string {
	return ds.userInfo.Get("name")
}

// Generate header for reply to QTV client.
func (ds *dStream) qtvHeader() string {
	h := fmt.Sprintf("QTVSV %g\n", qtvVersion)
	if ds.qtvEzQuakeExt != 0 {
		h += fmt.Sprintf("%s: %v\n", qtvEzQuakeExt, int32(ds.qtvEzQuakeExt))
	}
	return h
}

// Send error reply to downstream (flush output buffer and close connection).
func (ds *dStream) sendError(format string, a ...interface{}) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.sendError:") }()
	return ds.sendReplyEx(true, time.Now().Add(100*time.Millisecond), "ERROR: "+format, a...)
}

// Send error reply to downstream (flush output buffer and close connection).
func (ds *dStream) sendPermanentError(format string, a ...interface{}) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.sendPermanentError:") }()
	return ds.sendReplyEx(true, time.Now().Add(100*time.Millisecond), "PERROR: "+format, a...)
}

// Send reply to downstream (flush output buffer and close connection).
func (ds *dStream) sendLastReply(format string, a ...interface{}) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.sendLastReply:") }()
	return ds.sendReplyEx(true, time.Now().Add(5*time.Second), format, a...)
}

// Send reply to downstream (to NOT close connection).
func (ds *dStream) sendReply(format string, a ...interface{}) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.sendReply:") }()

	return ds.sendReplyEx(false, time.Now().Add(5*time.Second), format, a...)
}

// General reply helper, sets flush flag and deadline, validate state.
func (ds *dStream) sendReplyCommon(flushAndExit bool, deadLine time.Time) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.sendReplyCommon:") }()

	state := ds.getState()
	if state <= dsClosing {
		return errors.New("could not send reply, stream is about to quit")
	}
	if state != dsParsingHeader {
		return errors.New("could not send reply, invalid state")
	}

	if ds.flushAndExit {
		return errors.New("could not send reply if already flushing")
	}

	ds.flushAndExit = flushAndExit
	ds.conn.SetDeadline(deadLine)

	return nil
}

// Base reply method for downstream.
func (ds *dStream) sendReplyEx(flushAndExit bool, deadLine time.Time, format string, a ...interface{}) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.sendReplyEx:") }()

	if err := ds.sendReplyCommon(flushAndExit, deadLine); err != nil {
		return err
	}

	h := ds.qtvHeader()
	s := fmt.Sprintf(format, a...)
	_, err = ds.wb.WriteFull([]byte(h + s + "\n\n"))
	return err
}

// If we could not fit data inside output buffer (demo list for example)
// then we have to use reply with strings builder as intermediate storage.
func (ds *dStream) sendReplyWithBuilder(flushAndExit bool, deadLine time.Time, b *strings.Builder) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.sendReplyEx:") }()

	if err := ds.sendReplyCommon(flushAndExit, deadLine); err != nil {
		return err
	}

	// Write header to the ring buffer.
	h := ds.qtvHeader()
	if _, err = ds.wb.WriteFull([]byte(h)); err != nil {
		return err
	}
	// Append request ending sequence bytes to the builder.
	for !strings.HasSuffix(b.String(), "\n\n") {
		b.WriteByte('\n')
	}
	// Assign flush data.
	ds.flushData = strings.NewReader(b.String())

	return err
}

// Send byte data to downstream.
func (ds *dStream) send(b []byte) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.send:") }()

	if ds.getState() <= dsClosing {
		return errors.New("could not send data, stream is about to quit")
	}

	_, err = ds.wb.WriteFull(b)
	return err
}

// Send data to downstream wrapped inside netMsgW.
func (ds *dStream) sendNetMsg(msg *netMsgW) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.sendNetMsg:") }()

	if msg.error {
		return errors.New("could not send data, netMsg with error")
	}

	return ds.send(msg.Bytes())
}

// linkUpstream links downstream with upstream.
func (ds *dStream) linkUpstream(us *uStream) (err error) {
	defer func() {
		err = multierror.Prefix(err, "dStream.linkUpstream:")
		if err != nil {
			// Unlink 'ds' from upstream if something went wrong.
			ds.linkedUs = nil
		}
	}()

	if ds.getState() <= dsClosing {
		return errors.New("could not link upstream, we about to quit")
	}

	// Link with upstream.
	ds.linkedUs = us

	// Setup downstream for streaming.
	ds.userInfo.FromStr(ds.headers["USERINFO"])
	if err := ds.sanitizeClientName(); err != nil {
		return err
	}
	if err := ds.sendReply("BEGIN: %s", us.server); err != nil {
		return err
	}
	if err := ds.setState(dsNeedInitialData); err != nil {
		return err
	}
	return ds.userListUpdateBroadCast(qulAdd)
}

// Downstream main loop.
func (ds *dStream) mainLoop(ctx context.Context) (err error) {
	defer func() {
		err = multierror.Prefix(err, "dStream.mainLoop:")
		log.Trace().Str("ctx", "dStream").Str("name", "mainLoop").Uint32("id", uint32(ds.id)).Str("event", "out").Err(err).Msg("")
	}()

	defer func() {
		ds.dssNotifyCh <- ds.id // Signaling dStreamStorage for clean up.
	}()

	g := multierror.Group{}
	defer g.Wait()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer ds.conn.Close()

	// Do not accept linking with uStream if we about to quit.
	defer ds.setState(dsClosing)

	if err := ds.setState(dsParsingHeader); err != nil {
		return err
	}

	g.Go(func() error { return ds.ioReader(ctx) })
	g.Go(func() error { return ds.ioWriter(ctx) })

	for {
		// Detect errors and context cancelation faster.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-ds.ioECh:
			return err
		default:
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-ds.ioECh:
			return err
		case <-ds.rb.WC:
			if ds.linkedUs != nil {
				// If we linked to upstream then notify upstream about input data and process it inside upstream mainLoop.
				// That SIGNIFICANTLY simplify concurency.
				ds.linkedUs.downstreamInputNotify(ds.id)
			} else if err := ds.processRead(); err != nil {
				// Ignore error in case of flusing (perform buffer flushing and only after that close connection).
				if ds.flushAndExit {
					log.Trace().Err(multierror.Prefix(err, "dStream.mainLoop:")).Uint32("id", uint32(ds.id)).Msg("")
				} else {
					return err
				}
			}
		case <-ds.wb.RC:
			if ds.linkedUs != nil {
				// Update dead line if we parsed header and successfully sending data.
				deadLine := durationBound(1, ds.qtv.qvs.Get("dstream_timeout").Dur, 999999) * time.Second
				ds.conn.SetDeadline(time.Now().Add(deadLine))
			} else {
				// If we have extra data to send append it to the ring buffer now.
				if ds.flushData != nil && ds.flushData.Len() > 0 {
					if _, err := ds.flushData.WriteTo(ds.wb); err != nil {
						if err != io.ErrShortWrite && err != ringbuffer.ErrTooManyDataToWrite {
							return err
						}
					}
				} else if ds.flushAndExit && ds.wb.IsEmpty() {
					return nil
				}
			}
		}

		// Sleep preventing reader to wake up us too frequently, that reduces CPU usage a lot.
		// We use centralized tick so all goroutines wake up more or less at the same time,
		// do required work and go to sleep again. If goroutines wake up at different times
		// that consumes a lot more CPU.
		ds.qtv.tick.L.Lock()
		ds.qtv.tick.Wait()
		ds.qtv.tick.L.Unlock()
	}
}

// Process initial downsteam client headers handshake.
func (ds *dStream) processRead() (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.processRead:") }()

	if ds.flushAndExit {
		return nil // Do not process input if we trying to flush output buffer since we want to close this connection.
	}

	switch ds.getState() {
	case dsParsingHeader:
		return ds.parseHeader()
	default:
		return errors.New("invalid state")
	}
}

// Perform initial headers handshake between this QTV and remote client (most of the time ezQuake or something compatible).
func (ds *dStream) parseHeader() (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.parseHeader:") }()

	// We does not know how much bytes to read until new line so we get whole buffer.
	bb := ds.rb.BytesOneReader()
	discard := 0
	endOfRequest := false

	for {
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

		// Empty 'b' means no more headers.
		if len(b) == 0 {
			endOfRequest = true
			break
		}

		if err := ds.parseOneHeader(string(b)); err != nil {
			return err
		}
	}

	if err := ds.rb.Discard(discard); err != nil {
		return err
	}

	if endOfRequest {
		if err := ds.validateVersion(); err != nil {
			return err
		}
		if err := ds.processRequest(); err != nil {
			return err
		}
	}

	return nil
}

var (
	authPriority = map[string]int{
		"SHA3_512": 100,
		"PLAIN":    10,
	}
)

// Parse one header line.
func (ds *dStream) parseOneHeader(s string) (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.parseOneHeader:") }()

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

	log.Trace().Str("ctx", "dStream").Str("event", "header").Uint32("id", uint32(ds.id)).Str("header", n).Str("value", qv).Msg("")

	switch n {
	case "QTV", "VERSION", "PASSWORD", "SOURCE", "RECEIVE", "SOURCELIST", "DEMOLIST", "USERINFO":
		ds.headers[n] = v
	case "DEMO": // converting demo request into source request, not like anyone using DEMO anyway.
		ds.headers["SOURCE"] = "file:" + v
	case "AUTH":
		oldAuthName := ds.headers[n]
		if authPriority[v] > authPriority[oldAuthName] {
			ds.headers[n] = v
		}
	case qtvEzQuakeExt:
		ext, _ := strconv.Atoi(v)
		ds.qtvEzQuakeExt = (uint32(ext) & qtvEzQuakeExtMask)
	case "COMPRESSION":
		return errors.New("QTV downstream wrongly used compression")
	default:
		log.Debug().Str("ctx", "dStream").Str("event", "headerUnrecognized").Uint32("id", uint32(ds.id)).Str("header", n).Str("value", qv).Msg("")
	}
	return nil
}

// Validate downsteam client version.
func (ds *dStream) validateVersion() (err error) {
	defer func() {
		err = multierror.Prefix(err, "dStream.validateVersion:")
		if err != nil {
			ds.sendPermanentError("requested protocol version not supported")
		}
	}()

	s := ds.headers["VERSION"]

	// Version is a float value, but we compare only major version number here.
	if version, err := strconv.ParseFloat(s, 64); err != nil {
		return err
	} else if int(version) != int(qtvVersion) {
		return fmt.Errorf("QTV downstream doesn't support a compatible protocol version, expected %.2f, got %.2f", qtvVersion, version)
	}

	return nil
}

// Reader goroutine, perform read from downstream TCP connection to downstream read buffer.
func (ds *dStream) ioReader(ctx context.Context) (err error) {
	defer func() {
		log.Trace().Str("ctx", "dStream").Str("name", "ioReader").Uint32("id", uint32(ds.id)).Str("event", "out").Msg("")
	}()
	defer func() { err = multierror.Prefix(err, "dStream.ioReader:") }()

	_, err = ds.rb.ReadFromWithContext(ctx, ds.conn)
	if err == nil {
		err = io.EOF
	}
	ds.ioECh <- multierror.Prefix(err, "dStream.ioReader:")
	return err
}

// Writer goroutine, perform write do downsteam TCP connection from dowstream write buffer.
func (ds *dStream) ioWriter(ctx context.Context) (err error) {
	defer func() {
		log.Trace().Str("ctx", "dStream").Str("name", "ioWriter").Uint32("id", uint32(ds.id)).Str("event", "out").Msg("")
	}()
	defer func() { err = multierror.Prefix(err, "dStream.ioWriter:") }()

	_, err = ds.wb.WriteToWithContext(ctx, ds.conn)
	if err == nil {
		err = io.EOF
	}
	ds.ioECh <- multierror.Prefix(err, "dStream.ioWriter:")
	return err
}
