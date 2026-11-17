package qtv

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/soheilhy/cmux"
)

// tcpIngressListener bounds sockets waiting for cmux to distinguish QTV from
// HTTP. It reserves a slot before accepting so cmux cannot start an unbounded
// number of classifier goroutines. QTV applies its handshake limit after cmux
// identifies the protocol.
type tcpIngressListener struct {
	net.Listener
	ctx   context.Context
	slots chan struct{}
}

func (l *tcpIngressListener) Accept() (net.Conn, error) {
	select {
	case l.slots <- struct{}{}:
	case <-l.ctx.Done():
		return nil, l.ctx.Err()
	}
	conn, err := l.Listener.Accept()
	if err != nil {
		<-l.slots
		return nil, err
	}
	return &tcpIngressConn{Conn: conn, acceptedAt: time.Now(), slots: l.slots}, nil
}

// tcpIngressConn holds a classification slot until handoff or close. Either
// can release it first, so the shared slot must be returned only once.
type tcpIngressConn struct {
	net.Conn
	acceptedAt time.Time // Include time spent in cmux in the QTV handshake deadline.
	slots      chan struct{}
	released   sync.Once
}

func (c *tcpIngressConn) release() {
	c.released.Do(func() { <-c.slots })
}

func (c *tcpIngressConn) Close() error {
	c.release()
	return c.Conn.Close()
}

// cmux can queue a matched connection before net/http accepts it. Keep the
// slot until that handoff so sockets waiting in cmux still count.
type httpIngressListener struct {
	net.Listener
}

func (l httpIngressListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	conn.(*cmux.MuxConn).Conn.(*tcpIngressConn).release()
	return conn, nil
}

// newTCPMux gives QTV the first match because HTTP is the catch-all.
func newTCPMux(ctx context.Context, listener net.Listener, allowHTTP bool, classificationLimit int) (cmux.CMux, net.Listener, net.Listener) {
	ingress := &tcpIngressListener{Listener: listener, ctx: ctx, slots: make(chan struct{}, classificationLimit)}
	mux := cmux.New(ingress)
	mux.SetReadTimeout(50 * time.Millisecond)
	qtvListener := mux.Match(cmux.PrefixMatcher("QTV"))
	var httpListener net.Listener
	if allowHTTP {
		httpListener = httpIngressListener{mux.Match(cmux.Any())}
	}
	return mux, qtvListener, httpListener
}

// admitQTVTCP releases the classification slot after QTV has reserved a
// handshake slot. It returns the native socket for batched viewer writes
// while the caller keeps the wrapper chain for reads and close.
func admitQTVTCP(conn net.Conn) (time.Time, io.Writer) {
	acceptedAt := time.Now()
	if muxed, ok := conn.(*cmux.MuxConn); ok {
		conn = muxed.Conn
	}
	if ingress, ok := conn.(*tcpIngressConn); ok {
		acceptedAt = ingress.acceptedAt
		ingress.release()
		conn = ingress.Conn
	}
	return acceptedAt, conn
}
