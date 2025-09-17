package qtv

import (
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"golang.org/x/crypto/sha3"
)

//
// This file contains pending downstream processing.
// With pending we consider initial headers handshake before actual binary data streaming begins.
//

// processRequest process request after all headers parsed.
// It may be called few times in order to complete authentication.
func (ds *dStream) processRequest() (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.processRequest:") }()

	uss := ds.qtv.uss

	uss.lock()
	defer uss.unlock()

	if uss.isClosing() {
		return errors.New("could not process downstream request, we about to quit")
	}

	cmd := ds.getRequestCmd()
	if cmd == "RECEIVE" {
		if err := ds.convertReceiveToSourceReq(); err != nil {
			return err
		}
		cmd = "SOURCE"
	}
	if ok, err := ds.authenticateRequest(cmd); err != nil {
		return err
	} else if !ok {
		return nil // We sent challenge, waiting for reply, get it and repeat auth.
	}

	switch cmd {
	case "SOURCE":
		return ds.sourceRequest()
	case "SOURCELIST":
		return ds.sourceListRequest()
	case "DEMOLIST":
		return ds.demoListRequest()
	}

	ds.sendPermanentError("invalid request")
	return errors.New("invalid request")
}

var (
	// List of commands we know.
	// Valid request should contain only one of it, but we does not validate it and instead
	// just accept first one we found in the list, so it some sort of priority.
	requestCommands = []string{
		"SOURCE",
		"RECEIVE",
		"SOURCELIST",
		"DEMOLIST",
	}
)

// Returns command for request or empty string.
func (ds *dStream) getRequestCmd() string {
	for _, cmd := range requestCommands {
		if _, ok := ds.headers[cmd]; ok {
			return cmd
		}
	}
	return ""
}

// Return true if authenticateRequest succeed.
// Return false with nil err if we wait for password from downstream.
func (ds *dStream) authenticateRequest(cmd string) (ok bool, err error) {
	defer func() { err = multierror.Prefix(err, "dStream.authenticateRequest:") }()

	// Check if downstream should provide password for command in request.
	localPass := ds.getPasswordForReqCmd(cmd)
	if localPass == "" {
		return true, nil // We are not password protected.
	}

	// If downstream did not provided password - ask for it now and wait for downstream reply.
	remotePass := ds.headers["PASSWORD"]
	if remotePass == "" {
		return ds.sendReqChallenge()
	}

	// Compare passwords.
	return ds.validateReqAuth(remotePass, localPass)
}

// Returns password to request command or empty string.
// Password could be different for different commands.
func (ds *dStream) getPasswordForReqCmd(cmd string) string {
	uss := ds.qtv.uss

	// upstream may have own password besides qtv_password.
	if cmd == "SOURCE" {
		if id, err := isUsId(ds.headers[cmd]); err == nil {
			if us := uss.getStreamById(id); us != nil {
				// Accessing uStream fields directly may cause race condition here.
				// Particularly password is fine since it does not changed after stream creation.
				return us.password()
			}
		}
	}

	return uss.qtv.qvs.Get("qtv_password").Str
}

// Compare password provided by downstream and QTV password, if equals return true.
func (ds *dStream) validateReqAuth(remotePass string, localPass string) (ok bool, err error) {
	defer func() { err = multierror.Prefix(err, "dStream.validateReqAuth:") }()

	authMethod := ds.headers["AUTH"]

	switch authMethod {
	case "SHA3_512":
		challenge, err := ds.getAuthChallenge()
		if err != nil {
			return false, err
		}
		hash := sha3.Sum512([]byte(string(challenge) + localPass))
		hexHash := fmt.Sprintf("%x", hash)
		if remotePass == hexHash {
			return true, nil
		}
	case "PLAIN":
		if remotePass == localPass {
			return true, nil
		}
	}

	ds.sendPermanentError("authentication failure")
	return false, errors.New("authentication failure")
}

// If QTV need password from downstream then send request challenge to downstream.
// Always returns false.
func (ds *dStream) sendReqChallenge() (ok bool, err error) {
	defer func() { err = multierror.Prefix(err, "dStream.sendReqChallenge:") }()

	authMethod := ds.headers["AUTH"]

	switch authMethod {
	case "SHA3_512":
		if challenge, err := ds.getAuthChallenge(); err != nil {
			return false, err
		} else if err := ds.sendReply("AUTH: %v\nCHALLENGE: %s", authMethod, challenge); err != nil {
			return false, err
		}
		return false, nil
	}

	ds.sendPermanentError("authentication failure")
	return false, errors.New("authentication failure")
}

const (
	maxAuthChallengeSize = 63 // C implementation define it as 64 since it reserve 1 byte for NUL terminator.
)

// Get (or reuse) random challenge for downstream.
func (ds *dStream) getAuthChallenge() (b []byte, err error) {
	defer func() { err = multierror.Prefix(err, "dStream.getAuthChallenge:") }()

	if len(ds.authChallenge) != 0 {
		return ds.authChallenge, nil // Reuse.
	}
	b = make([]byte, maxAuthChallengeSize)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	for i := 0; i < len(b); i++ {
		b[i] = b[i]%(127-33) + 33
	}
	ds.authChallenge = b // Save for future reuse.
	return ds.authChallenge, nil
}

// Convert "RECEIVE" command to "SOURCE" command request.
func (ds *dStream) convertReceiveToSourceReq() (err error) {
	uss := ds.qtv.uss

	var (
		lastUs *uStream
	)

	for _, us := range uss.streamById {
		if us.options.isUser {
			continue // Ignore user created streams.
		}
		if lastUs != nil {
			ds.sendPermanentError("multiple streams are currently playing")
			return errors.New("multiple streams are currently playing")
		}
		lastUs = us
	}

	if lastUs == nil {
		ds.sendPermanentError("no stream selected")
		return errors.New("no stream selected")
	}

	// Actually converting request.
	ds.headers["SOURCE"] = fmt.Sprintf("%v", lastUs.id)

	return nil
}

// Process "SOURCE" command request.
func (ds *dStream) sourceRequest() (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.sourceRequest:") }()

	uss := ds.qtv.uss
	src := ds.headers["SOURCE"]

	options := uStreamOptions{
		isUser: true,
	}

	if _, err := uss.openAndRunWithDs(src, false, options, ds); err != nil {
		ds.sendPermanentError("no stream selected")
		return err
	}
	return nil
}

// Process "SOURCELIST" command request.
func (ds *dStream) sourceListRequest() (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.sourceRequest:") }()

	uss := ds.qtv.uss

	if len(uss.streamById) == 0 {
		ds.sendPermanentError("no sources currently available")
		return errors.New("no sources currently available")
	}

	// List could be huge and would overflow ring buffer so we use intermediate storage.
	var b strings.Builder

	for id, us := range uss.streamById {
		fmt.Fprintf(&b, "ASOURCE: %v: %15s: %15s\n", id, us.server, us.qp.remoteHostName.Load())
	}

	return ds.sendReplyWithBuilder(true, time.Now().Add(60*time.Second), &b)
}

// Process "DEMOLIST" command request.
func (ds *dStream) demoListRequest() (err error) {
	defer func() { err = multierror.Prefix(err, "dStream.demoListRequest:") }()

	demoList := ds.qtv.getDemoList()

	if len(demoList) == 0 {
		ds.sendPermanentError("no demos currently available")
		return errors.New("no demos currently available")
	}

	// List could be huge and would overflow ring buffer so we use intermediate storage.
	var b strings.Builder

	for _, demo := range demoList {
		fmt.Fprintf(&b, "ADEMO: %v: %15s\n", demo.FileInfo.Size(), demo.FileInfo.Name())
	}

	return ds.sendReplyWithBuilder(true, time.Now().Add(60*time.Second), &b)
}
