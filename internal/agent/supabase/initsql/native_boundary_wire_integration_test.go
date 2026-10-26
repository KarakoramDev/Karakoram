//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package initsql

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// boundaryWire is a transparent PostgreSQL wire relay, not a Conn substitute.
// All startup/authentication, SQL, transactions and receipts reach native PG17.
// The only perturbation is withholding the selected frontend COMMIT, or its
// genuine backend CommandComplete(COMMIT). It never synthesizes a server reply.
// TLS is disabled ONLY on this dedicated disposable test connection so frames
// can be reviewed; the server can still use SCRAM authentication unchanged.
// Apply's no-argument Exec("COMMIT") uses a simple Query message. Extended
// protocol for the receipt INSERT is forwarded opaquely, byte for byte.
type boundaryWire struct {
	reached chan struct{}
	done    chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (w *boundaryWire) release() { w.once.Do(func() { close(w.resume) }) }

func boundaryConnect(t *testing.T, ctx context.Context, original *pgx.ConnConfig, commit int, after bool) (*pgx.Conn, *boundaryWire) {
	t.Helper()
	wire := &boundaryWire{reached: make(chan struct{}), done: make(chan struct{}), resume: make(chan struct{})}
	config := original.Copy()
	config.TLSConfig = nil
	config.Fallbacks = nil
	config.ConnectTimeout = 10 * time.Second
	// A single dedicated connection is mandatory: reconnect/fallback must not
	// accidentally route a retry outside the observed boundary.
	var dialed bool
	config.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		if dialed {
			return nil, errors.New("boundary relay permits one native connection")
		}
		dialed = true
		server, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		client, relay := net.Pipe()
		stop := func() {
			client.Close()
			relay.Close()
			server.Close()
		}
		t.Cleanup(func() {
			wire.release()
			stop()
		})
		go func() {
			defer stop()
			defer close(wire.done)
			// StartupMessage has a length but no message-type byte. With TLS
			// disabled pgx sends protocol 3.0 directly, then ordinary frames.
			var size [4]byte
			if _, err := io.ReadFull(relay, size[:]); err != nil {
				return
			}
			n := int(binary.BigEndian.Uint32(size[:]))
			if n < 8 || n > 1<<20 {
				return
			}
			startup := make([]byte, n)
			copy(startup, size[:])
			if _, err := io.ReadFull(relay, startup[4:]); err != nil || binary.BigEndian.Uint32(startup[4:8]) != 196608 {
				return
			}
			if _, err := server.Write(startup); err != nil {
				return
			}
			// Each direction is read by exactly one goroutine. A buffered
			// result avoids leaking the second copier when its peer closes.
			ended := make(chan struct{}, 2)
			go func() {
				defer func() { ended <- struct{}{} }()
				boundaryForward(relay, server, commit, false, !after, wire)
			}()
			go func() {
				defer func() { ended <- struct{}{} }()
				boundaryForward(server, relay, commit, true, after, wire)
			}()
			select {
			case <-ended:
			case <-wire.resume:
			}
			stop()
		}()
		return client, nil
	}
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal("cannot connect native PostgreSQL through the test wire relay")
	}
	return conn, wire
}

func boundaryForward(src, dst net.Conn, target int, backend, intercept bool, wire *boundaryWire) {
	commits := 0
	for {
		var header [5]byte
		if _, err := io.ReadFull(src, header[:]); err != nil {
			return
		}
		n := int(binary.BigEndian.Uint32(header[1:]))
		if n < 4 || n > 16<<20 {
			return
		}
		frame := make([]byte, n+1)
		copy(frame, header[:])
		if _, err := io.ReadFull(src, frame[5:]); err != nil {
			return
		}
		typeByte := byte('Q')
		if backend {
			typeByte = 'C'
		}
		if intercept && frame[0] == typeByte && bytes.Equal(frame[5:], []byte("COMMIT\x00")) {
			commits++
			if commits == target {
				close(wire.reached)
				<-wire.resume
				return // Never forward the selected command/response.
			}
		}
		if _, err := io.Copy(dst, bytes.NewReader(frame)); err != nil {
			return
		}
	}
}
