package qtv

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/rs/zerolog/log"
)

//
// UDP protocol related things.
// At the moment support "status" connectionless feature only.
// Also communicating with master servers.
//

// Out of band message id bytes (check protocol.h in QW server for full list).
// M = master, S = server, C = client, A = any.
const (
	a2c_print     = "n" // print a message on client
	a2a_ping      = "k" // respond with an A2A_ACK
	a2a_ack       = "l" // general acknowledgement without info
	s2m_heartBeat = "a" //
)

const (
	qwDefaultMasters    = "master.quakeworld.nu master.quakeservers.net qwmaster.fodquake.net"
	qwDefaultMasterPort = "27000"
)

const (
	masterUpdateTime       = time.Minute * 5 // Heart beat interval.
	masterForcedUpdateTime = time.Hour * 24  // Update masters list once per day since DNS address could change.
)

type udpSv struct {
	qtv             *QTV
	conn            net.PacketConn
	msg             *netMsgW
	masters         map[string]*masterSv
	mastersSequence int
}

type masterSv struct {
	addr net.Addr
}

func newUdpSv(qtv *QTV) *udpSv {
	sv := &udpSv{
		qtv:     qtv,
		masters: map[string]*masterSv{},
		msg:     newNetMsgW(make([]byte, msgBufSize), false),
	}

	sv.regVars(qtv)

	return sv
}

func (sv *udpSv) regVars(qtv *QTV) {
	qtv.qvs.Reg("masters", qwDefaultMasters)
}

func (sv *udpSv) listen() (err error) {
	defer func() { err = multierror.Prefix(err, "udpSv.listen:") }()

	conn, err := net.ListenPacket(sv.qtv.networkUDP(), sv.qtv.listenAddress())
	if err != nil {
		return err
	}
	sv.conn = conn
	return nil
}

func (sv *udpSv) close() (err error) {
	if sv.conn != nil {
		return sv.conn.Close()
	}
	return nil
}

func (sv *udpSv) serve() (err error) {
	defer func() { log.Trace().Str("ctx", "udpSv").Str("name", "serve").Str("event", "out").Msg("") }()
	defer func() { err = multierror.Prefix(err, "udpSv.serve:") }()

	g := multierror.Group{}
	defer g.Wait()

	ctx, cancel := context.WithCancel(sv.qtv.ctx)
	defer cancel()

	// Spawn masters goroutine.
	g.Go(func() error { return sv.mastersLoop(ctx) })

	rBuf := make([]byte, msgBufSize)
	msg := newNetMsgR(nil, false)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Set pretty long dead line for read.
		sv.conn.SetDeadline(time.Now().Add(1 * time.Second))
		n, addr, err := sv.conn.ReadFrom(rBuf)
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				continue
			}
			log.Err(multierror.Prefix(err, "udpSv.serve:")).Msg("")
			continue
		}
		// Set short dead line for write.
		sv.conn.SetDeadline(time.Now().Add(10 * time.Millisecond))

		msg.Reset(rBuf[:n], false)
		if msg.GetUint32() == math.MaxUint32 {
			sv.parseConnectionLessPacket(addr, msg)
		}
	}
}

func (sv *udpSv) parseConnectionLessPacket(addr net.Addr, msg *netMsgR) {
	// Strings are not nul terminated here so this will set msg.error to true but we ignore it.
	s := msg.GetString()
	if s == "" {
		return
	}
	tr, _ := tokenizeString(s)

	cmd := tr.Argv(0)
	switch cmd {
	case "ping", a2a_ping:
		sv.pingCmd(addr)
	case "status":
		sv.statusCmd(addr, &tr)
	}
}

func (sv *udpSv) pingCmd(addr net.Addr) {
	sv.conn.WriteTo([]byte(a2a_ack), addr)
}

const (
	statusOldStyle   = 0
	statusServerInfo = (1 << 0)
	statusPlayers    = (1 << 1)
	statusSpectators = (1 << 2)
)

func (sv *udpSv) statusCmd(addr net.Addr, tr *tokenizerResult) {
	opt := tr.ArgvAtoi(1)

	msg := sv.msg.Clear()

	msg.PutUint32(math.MaxUint32) // -1 sequence means out of band
	msg.PutByte(a2c_print[0])
	if opt == statusOldStyle || (opt&statusServerInfo) != 0 {
		msg.PutString2f("%s\n", sv.qtv.serverInfo.String())
	}

	if opt == statusOldStyle || (opt&(statusPlayers|statusSpectators)) != 0 {
		top := 0
		bottom := 0
		ping := 9
		frags := "0"
		skin := ""

		cTime := curTime()

	outer:
		for _, uInfo := range sv.qtv.uss.getUStreamInfo() {
			for _, dInfo := range uInfo.Ds {
				if msg.WPos()+100 > msg.BLen() {
					break outer // Try to not to overflow.
				}
				connectTime := (cTime - dInfo.ConnectStart) / 1000 / 60 // milliseconds to seconds and then to minutes.
				msg.PutString2f("%v %v %v %v \"%v\" \"%v\" %v %v\n", dInfo.Id, frags, connectTime, ping, dInfo.Name, skin, top, bottom)
			}
		}
	}

	if msg.error {
		return
	}

	sv.conn.WriteTo(msg.Bytes(), addr)
}

func (sv *udpSv) mastersLoop(ctx context.Context) (err error) {
	defer func() { log.Trace().Str("ctx", "udpSv").Str("name", "mastersLoop").Str("event", "out").Msg("") }()
	defer func() { err = multierror.Prefix(err, "udpSv.mastersLoop:") }()

	updateTimer := time.NewTimer(0) // First update is right now.
	forcedUpdateTimer := time.NewTimer(masterForcedUpdateTime)
	defer func() {
		updateTimer.Stop()
		forcedUpdateTimer.Stop()
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-updateTimer.C:
			updateTimer.Reset(masterUpdateTime)
			sv.mastersUpdate(false)
			sv.mastersHeartBeat()
		case <-forcedUpdateTimer.C:
			forcedUpdateTimer.Reset(masterForcedUpdateTime)
			sv.mastersUpdate(true)
		}
	}
}

// Check if masters variable changed and apply changes.
func (sv *udpSv) mastersUpdate(forceUpdate bool) {
	masters := sv.qtv.qvs.Get("masters")
	if !masters.Modified.CAS(true, false) && !forceUpdate {
		return
	}

	sv.masters = map[string]*masterSv{}

	network := sv.qtv.networkUDP()

	for _, masterStrAddr := range strings.Fields(masters.Str) {
		if addr, err := net.ResolveUDPAddr(network, masterStrAddr+":"+qwDefaultMasterPort); err != nil {
			log.Err(multierror.Prefix(err, "udpSv.mastersUpdate:")).Msg("")
			continue
		} else {
			if sv.masters[addr.String()] != nil {
				continue // Ignore duplicate.
			}
			master := &masterSv{
				addr: addr,
			}
			sv.masters[addr.String()] = master
			log.Info().Str("ctx", "udpSv").Str("event", "masterAdd").Str("addr", master.addr.String()).Msg("")
		}
	}
}

func (sv *udpSv) mastersHeartBeat() {
	// Bump sequence.
	sv.mastersSequence++

	s := fmt.Sprintf("%s\n%v\n%v\n", s2m_heartBeat, sv.mastersSequence, sv.qtv.dss.count())
	b := []byte(s)

	for _, master := range sv.masters {
		sv.conn.WriteTo(b, master.addr)
	}
}
