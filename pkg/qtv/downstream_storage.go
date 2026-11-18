package qtv

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/qw-group/qtv/pkg/pools"
	"github.com/rs/zerolog/log"
)

//
// Accept incoming downstream connections, keep track of all downstreams, assign ids.
//

// Bound the sockets waiting for a QTV header or reverse authentication even
// when the viewer limit is full.
const maxPendingQTVHandshakes = 128

type dStreamStorage struct {
	mu sync.Mutex
	// True when we about to quit.
	closing bool
	// Map of downstream objects.
	stream map[dStreamId]*dStream
	// Count TCP handshakes until they are rejected or handed off.
	pending int
	// Downstream id pool.
	ids *pools.IDPool
	// This channel used for communication with dStream.
	// dStream notify storage with id when it wants to stop, so we can find dStream and remove reference from internal container.
	notifyCh chan dStreamId
	// Parent object.
	qtv *QTV
}

func newDStreamStorage(qtv *QTV) *dStreamStorage {
	dss := &dStreamStorage{
		stream:   map[dStreamId]*dStream{},
		qtv:      qtv,
		ids:      pools.NewIDPool(0),
		notifyCh: make(chan dStreamId),
	}
	dss.regCommands(qtv)
	dss.regVars(qtv)

	return dss
}

func (dss *dStreamStorage) serve(l net.Listener) (err error) {
	defer func() { log.Trace().Str("ctx", "dStreamStorage").Str("name", "serve").Str("event", "out").Msg("") }()
	defer func() { err = multierror.Prefix(err, "dStreamStorage.serve:") }()

	g := multierror.Group{}
	defer g.Wait()

	ctx, cancel := context.WithCancel(dss.qtv.ctx)
	defer cancel()

	defer l.Close()

	g.Go(func() error { return dss.accept(ctx, l) })

	done := ctx.Done()
	doneTimeout := expiredTimer()
	defer doneTimeout.Stop()

	for {
		if done == nil && dss.count() == 0 {
			return nil // accept joins any remaining classifiers.
		}
		select {
		case <-doneTimeout.C:
			return errors.New("could not gracefully close dStreamStorage")
		case <-done:
			dss.mu.Lock()
			dss.closing = true
			dss.mu.Unlock()
			// ctx is done, do not select on it anymore, just try to close all downstreams before timeout.
			done = nil
			doneTimeout.Reset(3 * time.Second)
		case dsId, ok := <-dss.notifyCh:
			if ok {
				err := dss.remove(dsId)
				if err != nil {
					log.Err(multierror.Prefix(err, "dStreamStorage.serve:")).Msg("")
				}
			}
		}
	}
}

func (dss *dStreamStorage) accept(ctx context.Context, l net.Listener) error {
	var classifiers sync.WaitGroup
	defer classifiers.Wait()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn, err := l.Accept()
		if err != nil {
			return err
		}
		dss.mu.Lock()
		if dss.closing || dss.pending >= maxPendingQTVHandshakes {
			dss.mu.Unlock()
			conn.Close()
			continue
		}
		dss.pending++
		dss.mu.Unlock()
		acceptedAt, writer := admitQTVTCP(conn)
		deadline := acceptedAt.Add(reverseHandshakeTimeout)
		classifiers.Go(func() { dss.classifyTCP(ctx, conn, writer, deadline) })
	}
}

func (dss *dStreamStorage) maxClients() int {
	return iBound(0, dss.qtv.qvs.Get("maxclients").Int, qtvMaxClients)
}

func (dss *dStreamStorage) open(conn net.Conn) (err error) {
	defer func() { err = multierror.Prefix(err, "dStreamStorage.open:") }()
	dss.mu.Lock()
	defer dss.mu.Unlock()

	if dss.viewerAtCapacity() {
		conn.Close()
		return nil
	}
	return dss.openLocked(conn, conn)
}

func (dss *dStreamStorage) viewerAtCapacity() bool {
	return len(dss.stream) >= dss.maxClients() || dss.closing
}

func (dss *dStreamStorage) finishPending() {
	dss.mu.Lock()
	dss.pending--
	dss.mu.Unlock()
}

// openLocked requires dss.mu so checking the viewer limit and registering
// the viewer happen under the same lock.
func (dss *dStreamStorage) openLocked(conn net.Conn, writer io.Writer) error {
	dsId := dStreamId(dss.ids.Get())
	ds, err := newDStream(dss.qtv, dss.notifyCh, conn, writer, dsId)
	if err != nil {
		conn.Close()
		dss.ids.Put(uint32(dsId)) // Return id back to the pool.
		return err
	}
	dss.stream[dsId] = ds

	log.Debug().Str("ctx", "dStreamStorage").Str("event", "open").Uint32("id", uint32(ds.id)).Msg("")

	return nil
}

func (dss *dStreamStorage) count() int {
	dss.mu.Lock()
	defer dss.mu.Unlock()
	return len(dss.stream)
}

func (dss *dStreamStorage) close(dsId string) (err error) {
	defer func() { err = multierror.Prefix(err, "dStreamStorage.close:") }()
	dss.mu.Lock()
	defer dss.mu.Unlock()

	if id, err := isDsId(dsId); err == nil {
		if ds := dss.stream[id]; ds != nil {
			return ds.cancel()
		}
	}
	return errNoSuchId
}

func (dss *dStreamStorage) remove(dsId dStreamId) error {
	dss.mu.Lock()
	defer dss.mu.Unlock()

	if ds, ok := dss.stream[dsId]; ok {
		log.Debug().Str("ctx", "dStreamStorage").Str("event", "remove").Uint32("id", uint32(dsId)).Msg("")

		ds.cancel()

		dss.ids.Put(uint32(ds.id)) // Return id back to the pool.
		delete(dss.stream, dsId)
		return nil
	}

	return fmt.Errorf("dStreamStorage.remove: no such dsId: %v", dsId)
}

type dStreamListInfo struct {
	id   dStreamId
	name string
}

func (dss *dStreamStorage) list() (list []*dStreamListInfo) {
	dss.mu.Lock()
	defer dss.mu.Unlock()

	for _, ds := range dss.stream {
		// Accessing dStream fields directly here is a bad idea since it can cause race condition.
		// Particularly id and name is safe to use.
		info := &dStreamListInfo{
			id:   ds.id,
			name: ds.name(),
		}
		list = append(list, info)
	}
	return list
}

func (dss *dStreamStorage) regVars(qtv *QTV) {
	qtv.qvs.Reg("qtv_password", "")
	qtv.qvs.RegEx("qtv_reverse_secret", "", qVarFlagSecret, nil)
	qtv.qvs.RegEx("maxclients", "1000", qVarFlagServerInfo, nil)
	qtv.qvs.Reg("allow_download", "1")
	qtv.qvs.Reg("allow_download_skins", "1")
	qtv.qvs.Reg("allow_download_models", "1")
	qtv.qvs.Reg("allow_download_sounds", "1")
	qtv.qvs.Reg("allow_download_maps", "1")
	qtv.qvs.Reg("allow_download_demos", "1")
	qtv.qvs.Reg("allow_download_other", "1")
	qtv.qvs.Reg("fp_messages", "4")
	qtv.qvs.Reg("fp_persecond", "2")
	qtv.qvs.Reg("fp_secondsdead", "2")
	qtv.qvs.RegEx("dstream_read_buf_size", 1024*32, 0, onChangeQueueSize)
	qtv.qvs.RegEx("dstream_write_buf_size", 256*1024, 0, onChangeQueueSize)
	qtv.qvs.Reg("dstream_timeout", "30")
}

func (dss *dStreamStorage) regCommands(qtv *QTV) {
	qtv.cmd.Register("dclose", dCloseCmd)
	qtv.cmd.Register("dlist", dListCmd)
}

func dCloseCmd(qtv *QTV, cmdArgs *qCmdArgs) (err error) {
	defer func() { err = multierror.Prefix(err, "dCloseCmd:") }()

	if cmdArgs.Argc() != 2 {
		return fmt.Errorf("Usage: %s <id>: close downstream connection\n", cmdArgs.Name())
	}

	return qtv.dss.close(cmdArgs.Argv(1))
}

func dListCmd(qtv *QTV, cmdArgs *qCmdArgs) error {
	infos := qtv.dss.list()
	sort.Slice(infos, func(i, j int) bool { return infos[i].id < infos[j].id })
	fmt.Println("downstream list:")
	fmt.Println(" #id", "name")
	for _, info := range infos {
		fmt.Printf("%4v %v\n", info.id, info.name)
	}
	fmt.Println("--------------------------------")
	fmt.Printf("%4v stream(s)\n", len(infos))
	return nil
}
