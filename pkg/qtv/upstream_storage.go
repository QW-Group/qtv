package qtv

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/markphelps/optional"
	"github.com/qw-group/qtv-go/pkg/pools"
	"github.com/rs/zerolog/log"
)

//
// Allocate upstream connections, keep track of all upstreams, assign ids.
//

type uStreamStorage struct {
	mu sync.Mutex
	// True when we about to quit.
	closing bool
	// Both maps contains exactly the same streams, but we could find them by different key types.
	stream     map[string]*uStream    // key is ussName string.
	streamById map[uStreamId]*uStream // key is stream id.
	// upstream info map, updated periodically.
	streamInfo map[uStreamId]*uStreamInfo
	// upstream id pool.
	ids *pools.IDPool
	// This channel used for communication with uStream.
	// uStream notify storage with id when it wants to stop, so we can find uStream and remove reference from internal containers.
	// Also uStream periodically send us info which we use later inside udp/http modules, this way we significantly simplify concurency handling.
	notifyCh chan interface{}
	// Parent object.
	qtv *QTV
}

func newUStreamStorage(qtv *QTV) *uStreamStorage {
	uss := &uStreamStorage{
		stream:     map[string]*uStream{},
		streamById: map[uStreamId]*uStream{},
		streamInfo: map[uStreamId]*uStreamInfo{},
		qtv:        qtv,
		ids:        pools.NewIDPool(0),
		notifyCh:   make(chan interface{}, qtvMaxClients),
	}
	uss.regCommands(qtv)
	uss.regVars(qtv)

	return uss
}

func (uss *uStreamStorage) getStreamById(id uStreamId) *uStream {
	return uss.streamById[id]
}

func (uss *uStreamStorage) isClosing() bool {
	return uss.closing
}

func (uss *uStreamStorage) lock() {
	uss.mu.Lock()
}

func (uss *uStreamStorage) unlock() {
	uss.mu.Unlock()
}

func (uss *uStreamStorage) getUStreamInfo() []*uStreamInfo {
	uss.mu.Lock()
	defer uss.mu.Unlock()

	list := make([]*uStreamInfo, 0, len(uss.streamById))
	for _, uInfo := range uss.streamInfo {
		list = append(list, uInfo)
	}
	return list
}

func (uss *uStreamStorage) regVars(qtv *QTV) {
	qtv.qvs.Reg("parse_delay", "7")
	qtv.qvs.Reg("maxservers", "100")
	qtv.qvs.Reg("maxchains", "1")
	qtv.qvs.Regf("ustream_read_buf_size", "%v", 1024*320)
	qtv.qvs.Regf("ustream_write_buf_size", "%v", 1024*32)
	qtv.qvs.Reg("ustream_timeout", "60")
}

func (uss *uStreamStorage) serve() (err error) {
	defer func() { err = multierror.Prefix(err, "uStreamStorage.serve:") }()
	defer func() { log.Trace().Str("ctx", "uStreamStorage").Str("name", "serve").Str("event", "out").Msg("") }()

	ctx, cancel := context.WithCancel(uss.qtv.ctx)
	defer cancel()

	done := ctx.Done()
	doneTimeout := expiredTimer()
	defer doneTimeout.Stop()

	for {
		if done == nil {
			if uss.count() == 0 {
				return nil // All upstreams closed.
			}
		}
		select {
		case <-doneTimeout.C:
			return errors.New("could not gracefully close uStreamStorage")
		case <-done:
			uss.mu.Lock()
			uss.closing = true
			uss.mu.Unlock()
			// ctx is done, do not select on it anymore, just try to close all upstreams before timeout.
			done = nil
			doneTimeout.Reset(3 * time.Second)
		case iFace, ok := <-uss.notifyCh:
			if ok {
				switch v := iFace.(type) {
				case uStreamId:
					err := uss.remove(v, true)
					if err != nil {
						log.Err(multierror.Prefix(err, "uStreamStorage.serve:")).Msg("")
					}
				case *uStreamInfo:
					uss.updateUStreamInfo(v)
				}
			}
		}
	}

}

func (uss *uStreamStorage) regCommands(qtv *QTV) {
	qtv.cmd.Register("qtv", qtvCmd)
	qtv.cmd.Register("playdemo", playDemoCmd)
	qtv.cmd.Register("close", closeCmd)
	qtv.cmd.Register("list", listCmd)
}

func (uss *uStreamStorage) count() int {
	uss.mu.Lock()
	defer uss.mu.Unlock()
	return len(uss.stream)
}

func (uss *uStreamStorage) maxServers() int {
	return iBound(0, uss.qtv.qvs.Get("maxservers").Int, 1024)
}

func (uss *uStreamStorage) maxChains() int {
	return iBound(0, uss.qtv.qvs.Get("maxchains").Int, 5)
}

func (uss *uStreamStorage) open(server string, lock bool, options uStreamOptions) (us *uStream, new bool, err error) {
	defer func() { err = multierror.Prefix(err, "uStreamStorage.open:") }()

	if lock {
		uss.mu.Lock()
		defer uss.mu.Unlock()
	}

	if uss.closing {
		return nil, false, errors.New("could not open new upstream, we about to quit")
	}

	if strings.Count(server, "@") > uss.maxChains() {
		return nil, false, errors.New("invalid server address, maxchains reached")
	}

	if strings.HasPrefix(server, "(user)") {
		return nil, false, errors.New("invalid server address")
	}

	if id, err := isUsId(server); err == nil {
		if us := uss.getStreamById(id); us == nil {
			return nil, false, errors.New("no such stream id")
		} else {
			return us, false, nil
		}
	}

	for i := 0; ; i++ {
		var ussName string
		if i == 0 {
			// If we does not have duplicates then server string equals to ussName.
			ussName = server
		} else {
			// We have duplicates, lets use suffix in order to get uniq name.
			ussName = fmt.Sprintf("%v_#%v", server, i)
		}
		// isUser right now means it was created by downstream client request,
		// there is possibility what we have stream with same name created by server administrator
		// and that stream could be password protected, in order to not interfere with such streams
		// we use "(user)" prefix in the name of isUser streams.
		if options.isUser {
			ussName = "(user)" + ussName
		}

		// Check if we have stream with such name already.
		if us := uss.stream[ussName]; us != nil {
			if options.isUser {
				return us, false, nil // Reuse already existing stream.
			}
			// upstream with such ussName exists, lets try ussName with numeric suffix.
			continue
		}

		// We about to create new stream, check if we have capacity.
		if len(uss.stream) >= uss.maxServers() {
			return nil, false, errors.New("maxservers reached")
		}

		usId := uStreamId(uss.ids.Get())
		us, err := newUStream(uss.qtv, uss.notifyCh, server, ussName, usId, options)
		if err != nil {
			uss.ids.Put(uint32(usId)) // Return id back to the pool.
			return nil, false, err
		}
		uss.stream[ussName] = us
		uss.streamById[us.id] = us

		log.Info().Str("ctx", "uStreamStorage").Str("event", "open").Uint32("id", uint32(us.id)).Msg("")

		return us, true, nil
	}
}

func (uss *uStreamStorage) openAndRun(server string, lock bool, options uStreamOptions) (us *uStream, err error) {
	defer func() { err = multierror.Prefix(err, "uStreamStorage.openAndRun:") }()

	if lock {
		uss.mu.Lock()
		defer uss.mu.Unlock()
	}

	if us, new, err := uss.open(server, false, options); err != nil {
		return nil, err
	} else {
		if new {
			us.run()
		}
		return us, nil
	}
}

func (uss *uStreamStorage) openAndRunWithDs(server string, lock bool, options uStreamOptions, ds *dStream) (us *uStream, err error) {
	defer func() { err = multierror.Prefix(err, "uStreamStorage.openAndRunWithDs:") }()

	if lock {
		uss.mu.Lock()
		defer uss.mu.Unlock()
	}

	us, new, err := uss.open(server, false, options)
	if err != nil {
		return nil, err
	}

	if err := us.linkDownstream(ds); err != nil {
		// Remove upstream if it was created with this function since something went wrong.
		if new {
			// Upstream should not be running, so it should be safe to set state here.
			_ = us.setState(usClosing)
			uss.remove(us.id, false)
		}

		return nil, err
	}

	if new {
		us.run()
	}

	return us, nil
}

// Find upstream inside storage and cancel it (it should cause upstream notify storage when it actually dies).
// Useful for console commands.
func (uss *uStreamStorage) close(usId string) (err error) {
	defer func() { err = multierror.Prefix(err, "uStreamStorage.close:") }()

	uss.mu.Lock()
	defer uss.mu.Unlock()

	if id, err := isUsId(usId); err == nil {
		if us := uss.getStreamById(id); us != nil {
			return us.cancel()
		}
	}
	return errNoSuchId
}

// Find upstream inside storage, cancel it and remove from internal containers.
// Should be used ONLY on NOT running upstreams.
func (uss *uStreamStorage) remove(id uStreamId, lock bool) error {
	if lock {
		uss.mu.Lock()
		defer uss.mu.Unlock()
	}

	if us := uss.getStreamById(id); us != nil {
		log.Info().Str("ctx", "uStreamStorage").Str("event", "remove").Uint32("id", uint32(id)).Msg("")

		// Accessing state here is "fine" since upsream should not be running.
		if us.getState() > usClosing {
			log.Error().Msg("uStreamStorage.remove: invalid upstream state")
		}

		us.cancel()

		// Return id back to the pool.
		uss.ids.Put(uint32(us.id))

		delete(uss.stream, us.ussName)
		delete(uss.streamById, us.id)
		delete(uss.streamInfo, us.id)

		return nil
	}

	return fmt.Errorf("uStreamStorage.remove: no such id: %v", id)
}

func (uss *uStreamStorage) updateUStreamInfo(info *uStreamInfo) error {
	uss.mu.Lock()
	defer uss.mu.Unlock()

	if us := uss.getStreamById(info.Id); us != nil {
		uss.streamInfo[info.Id] = info
	}
	return nil
}

type uStreamListInfo struct {
	id   uStreamId
	name string
}

func (uss *uStreamStorage) list() (list []*uStreamListInfo) {
	uss.mu.Lock()
	defer uss.mu.Unlock()

	for name, us := range uss.stream {
		// Accessing uStream fields directly here is a bad idea since it can cause race condition.
		// Particularly id is fine.
		info := &uStreamListInfo{
			id:   us.id,
			name: name,
		}
		list = append(list, info)
	}
	return list
}

// Probably, should use flag.FlagSet instead of this? On the other hand we keep backward compatibility.
func (options *uStreamOptions) parseOptions(cmdArgs *qCmdArgs) {
	if cmdArgs.Argc() > 2 {
		options.usPassword = optional.NewString(cmdArgs.Argv(2))
	}

	for i := 3; i < cmdArgs.Argc()-1; i++ {
		argv := cmdArgs.Argv(i)
		switch argv {
		case "password":
			p := cmdArgs.Argv(i + 1)
			if p != "" {
				options.dsPassword = optional.NewString(p)
				fmt.Printf("... using custom password\n")
			}
			i++
		case "delay":
			delayStr := cmdArgs.Argv(i + 1)
			if delay, err := strconv.ParseFloat(delayStr, 64); err == nil {
				options.ingameDelay = optional.NewFloat64(delay)
				fmt.Printf("... using custom delay (%vs)\n", delay)
			}
			i++
		case "address":
			addr := cmdArgs.Argv(i + 1)
			// Allow this to be blank.
			options.address = optional.NewString(addr)
			fmt.Printf("... using custom address (%v)\n", addr)
			i++
		default:
			fmt.Printf("... unknown option %v\n", argv)
		}
	}
}

func qtvCmd(qtv *QTV, cmdArgs *qCmdArgs) (err error) {
	defer func() { err = multierror.Prefix(err, "qtvCmd:") }()

	name := cmdArgs.Name()
	if cmdArgs.Argc() < 2 {
		// Produce both log and error.
		fmt.Printf("Usage: %s <[streamId@]host:port> [password] [options]: open upstream connection\n", name)
		fmt.Printf("Options are:\n")
		fmt.Printf("  password <password>      // password for connecting clients\n")
		fmt.Printf("  delay <delay>            // in seconds (defaults to 'parse_delay')\n")
		fmt.Printf("  address <address>        // address mvdsv will broadcast to server-browsers\n")
		fmt.Printf("\n")
		fmt.Printf("Chaining usage is similar:\n")
		fmt.Printf("%s <[streamId@]host3:port3@host2:port2@host1:port1>\n", name)
		fmt.Printf("This will connect to host1, then host2 and finally host3\n\n")
		return fmt.Errorf("Usage: %s <[streamId@]host:port> [password] [options]: open upstream connection", name)
	}

	options := uStreamOptions{}
	options.parseOptions(cmdArgs)
	_, err = qtv.uss.openAndRun("tcp:"+cmdArgs.Argv(1), true, options)
	return err
}

func playDemoCmd(qtv *QTV, cmdArgs *qCmdArgs) (err error) {
	defer func() { err = multierror.Prefix(err, "playDemoCmd:") }()

	if cmdArgs.Argc() < 2 {
		return fmt.Errorf("Usage: %s demo.mvd: open demo file as upstream", cmdArgs.Name())
	}

	options := uStreamOptions{}
	_, err = qtv.uss.openAndRun("file:"+cmdArgs.Argv(1), true, options)
	return err
}

func closeCmd(qtv *QTV, cmdArgs *qCmdArgs) (err error) {
	defer func() { err = multierror.Prefix(err, "closeCmd:") }()

	if cmdArgs.Argc() != 2 {
		return fmt.Errorf("Usage: %s <streamId>: close upstream connection", cmdArgs.Name())
	}

	return qtv.uss.close(cmdArgs.Argv(1))
}

func listCmd(qtv *QTV, cmdArgs *qCmdArgs) error {
	infos := qtv.uss.list()
	sort.Slice(infos, func(i, j int) bool { return infos[i].id < infos[j].id })
	fmt.Println("upstream list:")
	fmt.Println(" #id", "name")
	for _, info := range infos {
		fmt.Printf("%4v %v\n", info.id, info.name)
	}
	fmt.Println("--------------------------------")
	fmt.Printf("%4v stream(s)\n", len(infos))
	return nil
}
