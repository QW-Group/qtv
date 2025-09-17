package qtv

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/qw-group/qtv-go/pkg/info"
	"github.com/qw-group/qtv-go/pkg/qfs"
	"github.com/rs/zerolog/log"
	"github.com/soheilhy/cmux"
	"go.uber.org/atomic"
)

//
// Allocate main QTV object.
//

const (
	qtvVersion    = 1.0        // We support up to this QTV version.
	qtvRelease    = "1.16-dev" // Release version.
	qtvBuild      = "0"
	qtvProjectURL = "https://github.com/qw-group/qtv-go"
	qtvHelpURL    = qtvProjectURL
	qtvMaxClients = 2048
)

// Main QTV object.
type QTV struct {
	console      <-chan string      // Console read processed by goroutine and passed to us with channel.
	flagSet      *qtvFlagSet        // QTV system cmd line parser.
	cmd          *qCmd              // Console command processor.
	qvs          *qVarStorage       // QTV variables storage.
	uss          *uStreamStorage    // Upstream storage.
	dss          *dStreamStorage    // Downstream storage.
	udpSv        *udpSv             // UDP server.
	httpSv       *httpSv            // HTTP server.
	ctx          context.Context    // QTV main context.
	cancelFunc   context.CancelFunc // QTV main context cancel function.
	tick         *sync.Cond         // Centralized tick.
	demoList     atomic.Value       // copy-on-write.
	demoListLock sync.Mutex         // Used for updater only. Getters does not need it
	serverInfo   info.InfoTs        // Server info.
}

// Allocate new QTV object.
func NewQTV(ctx context.Context, console <-chan string, arguments []string) (qtv *QTV, err error) {
	defer func() {
		if err != nil {
			qtv.Stop()
			qtv = nil
		}
	}()

	ctx, cancel := context.WithCancel(ctx)
	qtv = &QTV{
		ctx:        ctx,
		cancelFunc: cancel,
		console:    console,
		tick:       sync.NewCond(&sync.Mutex{}),
	}
	if err := qtv.newFlagSet(arguments); err != nil {
		return qtv, err
	}
	qtv.setDemoList(make(demoList, 0))
	qtv.cmd = newQCmd(qtv)
	qtv.qvs = newQVarStorage(qtv)

	qtv.regVars()
	qtv.regCommands()

	qtv.uss = newUStreamStorage(qtv)
	qtv.dss = newDStreamStorage(qtv)
	qtv.udpSv = newUdpSv(qtv)
	qtv.httpSv = newHttpSv(qtv)
	if err := qtv.httpSv.prepare(); err != nil {
		return qtv, err
	}

	// Put cmd line and default config inside command buffer, but don't execute it yet.
	qtv.cmd.Prepend(qtv.flagSet.argsToStr())
	qtv.cmd.Prepend("exec qtv")

	return qtv, nil
}

type demoList []DemoListItem

type DemoListItem struct {
	FileInfo os.FileInfo
	Hash     Hashes
}

type Hashes struct {
	XXH3 string
}

type qtvFlagSet struct {
	flag.FlagSet
}

// Print usage if requested.
func (f *qtvFlagSet) defaultUsage() {
	fmt.Fprintf(f.Output(), "Usage of %s:\n", f.Name())
	fmt.Fprintf(f.Output(), "\n")
	fmt.Fprintf(f.Output(), "In order to pass commands you can do it like this:\n")
	fmt.Fprintf(f.Output(), "%s cmd1 arg1 arg2... \";\" cmd2 arg1 arg2...\n", f.Name())
	fmt.Fprintf(f.Output(), "For example:\n")
	fmt.Fprintf(f.Output(), "%s exec mycfg \";\" echo some text \";\" echo even more text\n", f.Name())
	fmt.Fprintf(f.Output(), "\n")

	f.PrintDefaults()
}

// Combine system cmd line arguments into command for QTV command processor.
func (f *qtvFlagSet) argsToStr() string {
	args := f.Args()
	if len(args) == 0 {
		return ""
	}

	// Quote all arguments except single semicolon which represents commands separation.
	quote := func(s string) string {
		if s != ";" {
			return "`" + s + "`"
		}
		return s
	}

	s := quote(args[0])
	for _, v := range args[1:] {
		s += " " + quote(v)
	}
	return s
}

// Allocate and parse system cmd line arguments for QTV.
func (qtv *QTV) newFlagSet(arguments []string) error {
	fs := new(qtvFlagSet)
	fs.Init(arguments[0], flag.ContinueOnError)
	fs.Usage = fs.defaultUsage

	if err := fs.Parse(arguments[1:]); err != nil {
		return err
	}
	qtv.flagSet = fs
	return nil
}

// Get error(s) as string with reduced verbosity.
// We skip "cmdExec: Exec: execLine: " prefix for error(s) since end-user most likely does not need it.
func getCmdErrorsAsString(err error) string {
	var es []error

	if mErr, ok := err.(*multierror.Error); ok {
		es = mErr.Errors
	} else {
		es = append(es, err)
	}

	points := make([]string, len(es))
	for i, err := range es {
		// Reduce verbosity for console.
		strErr := strings.TrimPrefix(err.Error(), "cmdExec: Exec: execLine: ")
		points[i] = fmt.Sprintf("* %s", strErr)
	}

	plural := "error"
	if len(es) != 1 {
		plural = "errors"
	}

	return fmt.Sprintf(
		"%d %v occurred:\n\t%s\n\n",
		len(es), plural, strings.Join(points, "\n\t"))
}

// cmdExec executes QTV command buffer and log errors if any.
func (qtv *QTV) cmdExec() {
	if err := qtv.cmd.Exec(); err != nil {
		// Log error with all verbosity we have.
		err = multierror.Prefix(err, "cmdExec:")
		log.Err(err).Msg("")
		// Print error(s) to console, probably with less verbosity.
		fmt.Print(getCmdErrorsAsString(err))
	}
}

// Cancel main context, eventually it should stop QTV.
func (qtv *QTV) Stop() error {
	if qtv.cancelFunc != nil {
		qtv.cancelFunc()
	}
	return nil
}

func (qtv *QTV) regVars() {
	qtv.qvs.RegEx("*version", "QTVGO "+qtvRelease, qVarFlagReadOnly|qVarFlagServerInfo, nil)

	qtv.qvs.RegEx("network", "", qVarFlagInitOnly, nil)
	qtv.qvs.RegEx("listen_address", ":28000", qVarFlagInitOnly, nil)
	qtv.qvs.Reg("address", "")
	qtv.qvs.RegEx("hostname", "unnamed", qVarFlagServerInfo, nil)
	qtv.qvs.Reg("tick_time", "100")
	qtv.qvs.Reg("demo_dir", "demos")
	qtv.regLogVars()
}

// Useful if user wants to explicitly specify IPv4 or IPv6 protocol family.
func (qtv *QTV) network() string {
	return qtv.qvs.Get("network").Str
}

// Protocol family for TCP.
func (qtv *QTV) networkTCP() string {
	return "tcp" + qtv.network()
}

// Protocol family for UDP.
func (qtv *QTV) networkUDP() string {
	return "udp" + qtv.network()
}

func (qtv *QTV) listenAddress() string {
	return qtv.qvs.Get("listen_address").Str
}

func (qtv *QTV) hostName() string {
	return qtv.qvs.Get("hostname").Str
}

func (qtv *QTV) demoDir() string {
	dd := qtv.qvs.Get("demo_dir")
	if !qfs.IsSimplePath(dd.Str) {
		return "demos"
	}
	return dd.Str
}

// Updates demo list time to time.
func (qtv *QTV) demoListUpdater() (err error) {
	defer func() { log.Trace().Str("ctx", "QTV").Str("name", "demoListUpdater").Str("event", "out").Msg("") }()
	defer func() { err = multierror.Prefix(err, "QTV.demoListUpdater:") }()

	ctx := qtv.ctx
	t := time.NewTimer(0)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := qtv.updateDemoList(); err != nil {
				// Don't be fatal.
				log.Err(multierror.Prefix(err, "QTV.demoListUpdater:")).Msg("")
			}

			// Check if we should remove some upload files.
			qtv.uploadCleanUp()

			t.Reset(60 * time.Second)
		}
	}
}

var (
	// Sensitive file extentions not allowed to be downloaded, both for normal QW protocol and HTTP.
	sensitiveDataExtensions = map[string]bool{
		".cfg": true,
		".key": true,
	}

	// Allowed file extensions for demos for download and listing. (HTTP could show more)
	demosAllowedExtentions = map[string]bool{
		".mvd": true,
		".gz":  true,
		".zip": true,
		".bz2": true,
	}
)

func fileNameHasSensitiveExtension(name string) bool {
	ext := filepath.Ext(name)
	return sensitiveDataExtensions[ext]
}

func demoNameHasValidExtension(name string) bool {
	ext := filepath.Ext(name)
	return demosAllowedExtentions[ext]
}

func (qtv *QTV) updateDemoList() error {
	demoDir := qtv.demoDir()
	f, err := os.Open(demoDir)
	if err != nil {
		qtv.setDemoList(make(demoList, 0))
		return err
	}
	defer f.Close()

	// Get file list in demo dir.
	infosFull, err := f.Readdir(-1)
	// Filter out unnecessary files.
	demoList := make(demoList, 0, len(infosFull))
	for _, info := range infosFull {
		name := info.Name()
		if strings.HasPrefix(name, ".") {
			continue // Ignore hidden files.
		}
		if !demoNameHasValidExtension(name) {
			continue // Unknown extension.
		}
		if info.Size() == 0 {
			continue // Empty file.
		}

		demoPath := filepath.Join(demoDir, info.Name())
		hashXXH3, err := XXH3Sum(demoPath)
		if err != nil {
			log.Trace().Str("ctx", "QTV").Str("event", "updateDemoList").Str("file", info.Name()).Int64("size", info.Size()).Msg("XXH3Sum")
			continue
		}

		demoList = append(demoList, DemoListItem{
			FileInfo: info,
			Hash: Hashes{
				XXH3: hashXXH3,
			},
		})
	}

	sort.Slice(demoList, func(i, j int) bool { return demoList[i].FileInfo.ModTime().After(demoList[j].FileInfo.ModTime()) })
	qtv.setDemoList(demoList)

	return err
}

func (qtv *QTV) setDemoList(demoList demoList) {
	qtv.demoListLock.Lock()
	qtv.demoList.Store(demoList)
	qtv.demoListLock.Unlock()
}

func (qtv *QTV) getDemoList() demoList {
	return qtv.demoList.Load().(demoList)
}

func (qtv *QTV) uploadCleanUp() {
	uploadMaxSize := qtv.httpSv.uploadTotalLimit()

	var uploadSize int64

	demos := qtv.getDemoList()

	// Calculate how much space occupied by uploads.
	for _, d := range demos {
		if strings.HasPrefix(d.FileInfo.Name(), "upload") {
			uploadSize += d.FileInfo.Size()
		}
	}

	// Attempt to remove oldest uploads in order to clean up space.
	for i := len(demos) - 1; i >= 0 && uploadSize > uploadMaxSize; i-- {
		d := demos[i]
		if strings.HasPrefix(d.FileInfo.Name(), "upload") {
			file := filepath.Join(qtv.demoDir(), d.FileInfo.Name())
			os.Remove(file)
			uploadSize -= d.FileInfo.Size()
			log.Trace().Str("ctx", "QTV").Str("event", "uploadCleanUp").Str("file", d.FileInfo.Name()).Int64("size", d.FileInfo.Size()).Int64("uploadSize", uploadSize).Msg("")
		}
	}
}

// Get ticker object for main loop.
func (qtv *QTV) getMainTicker() *time.Ticker {
	tickTime := qtv.qvs.Get("tick_time")
	if !tickTime.Modified.CAS(true, false) {
		return nil
	}

	tickTimeDuration := time.Millisecond * durationBound(100, tickTime.Dur, 1000)
	return time.NewTicker(tickTimeDuration)
}

func (qtv *QTV) mainLoop() error {
	defer func() { log.Trace().Str("ctx", "QTV").Str("name", "mainLoop").Str("event", "out").Msg("") }()

	ticker := qtv.getMainTicker()
	defer func() {
		ticker.Stop()
	}()

	for {
		select {
		case <-ticker.C:
			qtv.tick.Broadcast()
			if newTicker := qtv.getMainTicker(); newTicker != nil {
				ticker = newTicker
			}
		case <-qtv.ctx.Done():
			qtv.waitForStreamStorages()
			return multierror.Prefix(qtv.ctx.Err(), "QTV.mainLoop:")
		case s := <-qtv.console:
			qtv.cmd.Prepend(s)
			qtv.cmdExec()
		}
	}
}

// In case of graceful quit try to wait till streams clean up.
func (qtv *QTV) waitForStreamStorages() {
	defer func() { log.Trace().Str("ctx", "QTV").Str("name", "waitForStreamStorages").Str("event", "out").Msg("") }()
	// Wake up upstreams so they can detect we are quitting.
	qtv.tick.Broadcast()
	for i := 0; i < 100; i++ {
		if qtv.uss.count() == 0 && qtv.dss.count() == 0 {
			return // uStreamStorage and dStreamStorage closed all streams.
		}
		time.Sleep(time.Millisecond)
		qtv.tick.Broadcast()
	}
}

// Current time in milliseconds.
func curTime() uint64 {
	return uint64(time.Now().UnixMilli())
}

// Returns expirer timer.
func expiredTimer() *time.Timer {
	t := time.NewTimer(0)
	<-t.C
	return t
}

func (qtv *QTV) regCommands() {
	qtv.cmd.Register("status", statusCmd)
}

func statusCmd(qtv *QTV, cmdArgs *qCmdArgs) error {
	fmt.Printf("Status:\n")
	fmt.Printf(" servers: %4v/%v\n", qtv.uss.count(), qtv.uss.maxServers())
	fmt.Printf(" clients: %4v/%v\n", qtv.dss.count(), qtv.dss.maxClients())

	fmt.Printf("Options:\n")
	fmt.Printf("   hostname: %v\n", qtv.hostName())
	fmt.Printf("listen addr: %v\n", qtv.listenAddress())
	fmt.Printf("       http: %v\n", isEnabledFromBool(qtv.httpSv.isEnabled()))
	if qtv.httpSv.isEnabled() {
		fmt.Printf("http upload: %v\n", isEnabledFromBool(qtv.httpSv.uploadEnabled()))
	}
	return nil
}

// Allocate listen sockets and start serving incoming connections, process console input.
func (qtv *QTV) ListenAndServe() (err error) {
	defer func() { err = multierror.Prefix(err, "QTV.ListenAndServe:") }()

	// Execute command buffer.
	qtv.cmdExec()
	// Do not allow modify init only variables anymore.
	qtv.qvs.QtvWasInitializedNotify()

	// Listen UDP.
	if err := qtv.udpSv.listen(); err != nil {
		return err
	}
	defer qtv.udpSv.close()

	// Listen TCP.
	listener, err := net.Listen(qtv.networkTCP(), qtv.listenAddress())
	if err != nil {
		return err
	}

	defer listener.Close()

	// Since we listen QTV and HTTP protocol on the same port we have to do multiplexing.
	// In golang it somewhat tricky, we have to resort to third party module CMux.
	mux := cmux.New(listener)
	mux.SetReadTimeout(50 * time.Millisecond)
	downstreamListener := mux.Match(cmux.PrefixMatcher("QTV"))
	var httpListener net.Listener
	if qtv.httpSv.isEnabled() {
		httpListener = mux.Match(cmux.Any())
	}

	// Start all our goroutines.
	g := multierror.Group{}
	g.Go(func() error { return qtv.udpSv.serve() })
	g.Go(func() error { return qtv.demoListUpdater() })
	g.Go(func() error { return qtv.uss.serve() })
	g.Go(func() error { return qtv.dss.serve(downstreamListener) })
	if httpListener != nil {
		g.Go(func() error { return qtv.httpSv.serve(httpListener) })
	}
	g.Go(func() error { return qtv.mainLoop() })
	g.Go(func() error { return mux.Serve() })

	select {
	case <-qtv.ctx.Done():
		mux.Close()
		qtv.udpSv.close() // We have defer with close above but this will speed up things.
	}

	return g.Wait()
}
