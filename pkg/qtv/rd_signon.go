package qtv

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	rdSignonJoinVar     = "dstream_rd_signon_join_window"
	rdSignonTimeoutVar  = "dstream_rd_signon_timeout"
	maxRDSignonSnapshot = 8 * 1024 * 1024
)

func registerRDSignonVars(qtv *QTV) {
	qtv.qvs.RegEx(rdSignonTimeoutVar, 12, 0, checkRDSignonTime)
	qtv.qvs.RegEx(rdSignonJoinVar, 3, 0, checkRDSignonTime)
}

// The cvar setter holds its lock during validation.
func checkRDSignonTime(qtv *QTV, name string, _, next *qVar) bool {
	seconds, err := strconv.ParseFloat(next.Str, 64)
	ns := seconds * float64(time.Second)
	if err == nil && ns >= 0 && ns < float64(math.MaxInt64) {
		value := time.Duration(ns)
		other := qtv.qvs.Find(rdSignonTimeoutVar)
		if name == rdSignonTimeoutVar {
			other = qtv.qvs.Find(rdSignonJoinVar)
			if value > 0 && (other == nil || value > rdSignonDuration(other)) {
				return true
			}
		} else if value < rdSignonDuration(other) {
			return true
		}
	}
	log.Error().Str("cvar", name).Msg("expected finite seconds with 0 <= join window < timeout")
	return false
}

func rdSignonDuration(value *qVar) time.Duration {
	return time.Duration(value.Float * float64(time.Second))
}

type rdSignonIDs [maxRDEntities / 64]uint64

func (ids *rdSignonIDs) set(id uint32) {
	ids[id/64] |= uint64(1) << (id % 64)
}

func (ids *rdSignonIDs) has(id int) bool {
	return ids[id/64]&(uint64(1)<<(id%64)) != 0
}

// rdSignonSnapshot belongs to the source loop. All attached viewers use the
// same encoded bytes and the same set of changes since those bytes were built.
type rdSignonSnapshot struct {
	data        []byte
	bodyOffset  int // Extended clients already received serverdata and catalogs.
	dirty       rdSignonIDs
	baselines   rdSignonIDs
	staticCount int
	soundCount  int
	joinUntil   time.Time
	deadline    time.Time
}

type rdViewerSignon struct {
	capture   *rdSignonBuffer // Temporary synchronous encoder destination.
	requested bool            // Both forms of signon can wait for a new snapshot.
	attached  bool
	offset    int
}

type rdSignonBuffer struct {
	data  []byte
	limit int
}

func (buf *rdSignonBuffer) append(first, second []byte) error {
	if len(first)+len(second) > buf.limit-len(buf.data) {
		return errByteQueueFull
	}
	required := len(buf.data) + len(first) + len(second)
	if required > cap(buf.data) {
		// Keep the backing allocation bounded as well as the encoded length.
		capacity := min(buf.limit, max(required, max(4096, 2*cap(buf.data))))
		next := make([]byte, len(buf.data), capacity)
		copy(next, buf.data)
		buf.data = next
	}
	buf.data = append(buf.data, first...)
	buf.data = append(buf.data, second...)
	return nil
}

func (ds *dStream) captureRDSignon(limit int, write func() error) ([]byte, error) {
	out := rdSignonBuffer{limit: limit}
	ds.rdSignon.capture = &out
	defer func() { ds.rdSignon.capture = nil }()
	err := write()
	return out.data, err
}

func (ds *dStream) rdSignonHeadroom() int {
	// Leave room for live records on the tick after the viewer becomes active.
	return ds.wb.Capacity() / 4
}

func (us *uStream) startRDSignon(ds *dStream) error {
	ds.rdSignon.requested = true
	if ds.download.IsActive() {
		// Remember qtvspawn, but start signon only after the last file block is queued.
		return nil
	}
	return us.sendRDSignon(ds, time.Now())
}

func (us *uStream) sendRDSignon(ds *dStream, now time.Time) error {
	snapshot := &us.rdSignon
	if ds.rdSignon.attached {
		if !now.Before(snapshot.deadline) {
			return errors.New("shared RD signon deadline expired")
		}
		return us.pageRDSignon(ds)
	}
	if snapshot.data != nil && !now.Before(snapshot.joinUntil) {
		return nil
	}

	if snapshot.data == nil {
		// The small path uses the latest state and needs no shared snapshot.
		limit := ds.wb.Capacity() - ds.rdSignonHeadroom()
		full, err := ds.captureRDSignon(limit, func() error {
			if ds.getState() == dsNeedInitialData {
				if err := us.sendInitialMVDCatalogs(ds); err != nil {
					return err
				}
			}
			if err := ds.sendSpawnSnapshot(true); err != nil {
				return err
			}
			return ds.sendSpawnEnd()
		})
		if err == nil {
			if len(full)+ds.rdSignonHeadroom() > ds.wb.Available() {
				return nil
			}
			if _, err := ds.wb.WriteFull(full); err != nil {
				return err
			}
			return ds.setState(dsActive)
		}
		if !errors.Is(err, errByteQueueFull) {
			return err
		}
		var bodyOffset int
		data, err := ds.captureRDSignon(maxRDSignonSnapshot, func() error {
			if err := us.sendInitialMVDCatalogs(ds); err != nil {
				return err
			}
			bodyOffset = len(ds.rdSignon.capture.data)
			// A slot can change occupants or become vacant while these pages
			// are queued. Send HUD stats with the final userinfo instead.
			return ds.sendSpawnSnapshot(false)
		})
		if err != nil {
			return fmt.Errorf("shared RD snapshot exceeds %d bytes or failed to encode: %w", maxRDSignonSnapshot, err)
		}
		settings := us.qtv.qvs.v.Load().(qvarMap)
		*snapshot = rdSignonSnapshot{
			data:        data,
			bodyOffset:  bodyOffset,
			staticCount: len(us.qp.rd.statics),
			soundCount:  us.qp.staticSoundCount,
			joinUntil:   now.Add(rdSignonDuration(settings[rdSignonJoinVar])),
			deadline:    now.Add(rdSignonDuration(settings[rdSignonTimeoutVar])),
		}
	}
	if ds.getState() == dsNeedSpawn {
		ds.rdSignon.offset = snapshot.bodyOffset
	}
	ds.rdSignon.attached = true
	return us.pageRDSignon(ds)
}

func (us *uStream) pageRDSignon(ds *dStream) error {
	if ds.download.denialPending {
		return nil
	}
	snapshot := &us.rdSignon
	start := ds.rdSignon.offset
	end := start
	available := ds.wb.Available()
	cursor := byteViewOf(snapshot.data[start:]).cursor(0)
	for cursor.left > 0 {
		header, status := inspectMVDRecord(&cursor, mvdContainerTime)
		if status != mvdRecordComplete {
			return errors.New("invalid generated RD snapshot record")
		}
		if header.size > ds.wb.Capacity() {
			return errors.New("RD snapshot record exceeds viewer queue")
		}
		// Keep room for small command replies between snapshot pages. A single
		// large record may need the entire queue to make progress.
		reserve := min(512, ds.wb.Capacity()-header.size)
		if header.size+reserve > available-(end-start) {
			break
		}
		end += header.size
		cursor.skip(header.size)
	}
	if end != start {
		if _, err := ds.wb.WriteFull(snapshot.data[start:end]); err != nil {
			return err
		}
		ds.rdSignon.offset = end
	}
	if end != len(snapshot.data) || ds.wb.Available() <= ds.rdSignonHeadroom() {
		return nil
	}

	// Encode against the world at this boundary. Retry from current state if
	// the viewer's TCP tail leaves too little room for the atomic correction.
	limit := ds.wb.Capacity() - ds.rdSignonHeadroom()
	correction, err := ds.captureRDSignon(limit, func() error {
		// Serverinfo keys may have changed or disappeared since serverdata.
		msg := us.qp.w.Clear()
		us.qp.putFullServerInfo(msg)
		if err := ds.sendMVDMessage(msg, mvdMsgRead, playersMaskAll); err != nil {
			return err
		}
		if err := us.qp.sendRDSignonMetadata(ds, &snapshot.baselines, snapshot.staticCount, snapshot.soundCount); err != nil {
			return err
		}
		// Some client stats have nonzero defaults, including ezQuake view height.
		if err := us.qp.sendPlayerStatsWithZeros(ds, true); err != nil {
			return err
		}
		if err := us.qp.sendRDWorld(ds, &snapshot.dirty); err != nil {
			return err
		}
		if us.qp.hasCDTrack {
			msg := us.qp.w.Clear()
			msg.PutSVC(svc_cdtrack)
			msg.PutByte(us.qp.cdTrack)
			if err := ds.sendMVDMessage(msg, mvdMsgRead, playersMaskAll); err != nil {
				return err
			}
		}
		return ds.sendSpawnEnd()
	})
	if err != nil {
		return fmt.Errorf("RD signon correction exceeds %d bytes or failed to encode: %w", limit, err)
	}
	if len(correction)+ds.rdSignonHeadroom() > ds.wb.Available() {
		return nil
	}
	if _, err := ds.wb.WriteFull(correction); err != nil {
		return err
	}
	return ds.setState(dsActive)
}

// pumpRDSignons runs after forwarding the last parsed record. New viewers
// become active after the snapshot reflects that record's retained state.
func (us *uStream) pumpRDSignons() {
	if !us.qp.usesRD() || us.getState() != usActive || us.qp.rd == nil || !us.qp.rd.ready {
		return
	}
	us.mu.Lock()
	defer us.mu.Unlock()
	now := time.Now()
	for _, ds := range us.linkedDs {
		if ds.isCanceled() || ds.download.IsActive() {
			continue
		}
		var err error
		switch {
		case ds.rdSignon.requested:
			err = us.sendRDSignon(ds, now)
		case ds.getState() == dsNeedInitialData:
			err = us.sendInitialMVDData(ds)
		}
		if err != nil {
			log.Warn().Err(err).Uint32("source", uint32(us.id)).Uint32("viewer", uint32(ds.id)).
				Str("map", us.qp.mapName).Int("limit", ds.wb.Capacity()).Msg("RD signon failed")
			ds.cancel()
		}
	}
	for _, ds := range us.linkedDs {
		if ds.rdSignon.attached && !ds.isCanceled() {
			return
		}
	}
	if us.rdSignon.data != nil {
		us.rdSignon = rdSignonSnapshot{}
	}
}
