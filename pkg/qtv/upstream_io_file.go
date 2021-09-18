package qtv

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/hashicorp/go-multierror"
	"github.com/qqshka/qtv-go/pkg/qfs"
)

//
// Abstraction around file IO for upstream.
//

type uStreamFile struct {
	server string
	qtv    *QTV
	file   *os.File
}

func newUStreamFile(qtv *QTV, server string) (ust *uStreamFile, err error) {
	defer func() { err = multierror.Prefix(err, "newUStreamFile:") }()

	addr, err := addressFromServerStr("file:", server)
	if err != nil {
		return nil, err
	}
	if filepath.Ext(addr) != ".mvd" {
		return nil, errors.New("only mvd file extension supported")
	}

	return &uStreamFile{
		server: server,
		qtv:    qtv,
	}, nil
}

func (ust *uStreamFile) Open(ctx context.Context) (err error) {
	defer func() { err = multierror.Prefix(err, "uStreamFile.Open:") }()

	ust.Close()

	addr, err := addressFromServerStr("file:", ust.server)
	if err != nil {
		return err
	}

	f, _, err := qfs.Open(ust.qtv.demoDir(), addr)
	if err != nil {
		return err
	}
	ust.file = f
	return nil
}

func (ust *uStreamFile) Close() error {
	if ust.file == nil {
		return nil
	}
	return multierror.Prefix(ust.file.Close(), "uStreamFile.Close:")
}

func (ust *uStreamFile) Read(b []byte) (n int, err error) {
	n, err = ust.file.Read(b)
	return n, multierror.Prefix(err, "uStreamFile.read:")
}

func (ust *uStreamFile) Write(p []byte) (n int, err error) {
	// In case of the file we does not really write anything.
	return len(p), nil
}
