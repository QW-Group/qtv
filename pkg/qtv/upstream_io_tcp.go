package qtv

import (
	"context"
	"net"
	"time"

	"github.com/hashicorp/go-multierror"
)

//
// Abstraction around TCP IO for upstream.
//

type uStreamTCP struct {
	server string
	qtv    *QTV
	conn   net.Conn
}

func newUStreamTCP(qtv *QTV, server string) (*uStreamTCP, error) {
	if !isTcpProtocolOrRemoteFile(server) {
		return nil, multierror.Prefix(errInvalidProtocol, "newUStreamTCP:")
	}

	return &uStreamTCP{
		server: server,
		qtv:    qtv,
	}, nil
}

func (ust *uStreamTCP) Open(ctx context.Context) (err error) {
	defer func() { err = multierror.Prefix(err, "uStreamTCP.Open:") }()

	ust.Close()

	protocol := protocolFromServerStr(ust.server)
	if protocol == "" {
		return errInvalidProtocol
	}
	addr, err := addressFromServerStr(protocol, ust.server)
	if err != nil {
		return err
	}

	var d net.Dialer
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	conn, err := d.DialContext(ctx, ust.qtv.networkTCP(), addr)
	if err != nil {
		return err
	}
	ust.conn = conn
	return nil
}

func (ust *uStreamTCP) Close() error {
	if ust.conn == nil {
		return nil
	}

	return multierror.Prefix(ust.conn.Close(), "uStreamTCP.Close:")
}

func (ust *uStreamTCP) Read(b []byte) (n int, err error) {
	deadLine := durationBound(1, ust.qtv.qvs.Get("ustream_timeout").Dur, 999999) * time.Second
	ust.conn.SetDeadline(time.Now().Add(deadLine))

	n, err = ust.conn.Read(b)
	return n, multierror.Prefix(err, "uStreamTCP.read:")
}

func (ust *uStreamTCP) Write(p []byte) (n int, err error) {
	n, err = ust.conn.Write(p)
	return n, multierror.Prefix(err, "uStreamTCP.write:")
}
