package qtv

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
)

//
// Abstraction around TCP IO for upstream.
//

type uStreamTCP struct {
	server        string
	qtv           *QTV
	conn          net.Conn
	reverseReader *bufio.Reader // Retains any MVD bytes read ahead during the handshake.
	reverseReply  string        // Sent by Open once viewers can resolve the returned source ID.
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

// newReverseUStreamTCP keeps the handshake reader so any bytes it buffered
// are still available when the upstream sends REVERSED and begins reading MVD.
func newReverseUStreamTCP(qtv *QTV, conn net.Conn, reader *bufio.Reader, userInfo string) *uStreamTCP {
	return &uStreamTCP{
		qtv:           qtv,
		conn:          conn,
		reverseReader: reader,
		reverseReply:  fmt.Sprintf("QTVSV 1\nVERSION: 1\nREVERSED\nUSERINFO: \"%s\"\n\n", userInfo),
	}
}

func (ust *uStreamTCP) Open(ctx context.Context) (err error) {
	defer func() { err = multierror.Prefix(err, "uStreamTCP.Open:") }()

	if ust.reverseReader != nil {
		return ust.openReverse(ctx)
	}
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

// Confirm the registered source within the authentication deadline. Give the
// sender a fresh deadline for BEGIN before entering MVD mode.
func (ust *uStreamTCP) openReverse(ctx context.Context) error {
	stopCancel := context.AfterFunc(ctx, func() { ust.conn.Close() })
	defer stopCancel()

	if err := writeQTVHeader(ust.conn, ust.reverseReply); err != nil {
		return err
	}
	if err := ust.conn.SetDeadline(time.Now().Add(reverseBeginTimeout)); err != nil {
		return err
	}
	budget := reverseHeaderLimit
	block, err := readQTVHeader(ust.reverseReader, &budget)
	if err != nil {
		return err
	}
	if strings.ReplaceAll(string(block), "\r\n", "\n") != "QTVSV 1\nBEGIN\n\n" {
		return errors.New("invalid reverse BEGIN")
	}
	return ust.conn.SetDeadline(time.Time{})
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

	if ust.reverseReader != nil {
		n, err = ust.reverseReader.Read(b)
	} else {
		n, err = ust.conn.Read(b)
	}
	return n, multierror.Prefix(err, "uStreamTCP.read:")
}

func (ust *uStreamTCP) Write(p []byte) (n int, err error) {
	n, err = ust.conn.Write(p)
	return n, multierror.Prefix(err, "uStreamTCP.write:")
}
