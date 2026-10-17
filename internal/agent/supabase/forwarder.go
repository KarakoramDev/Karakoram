// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
)

// forwarder preserves the PostgreSQL wire protocol, including TLS, replication
// and LISTEN/NOTIFY. Every old session is closed before a new leader is used.
type forwarder struct {
	mu                  sync.Mutex
	listener            net.Listener
	primary             string
	generation          uint64
	changed             chan struct{}
	closed              bool
	connections         map[net.Conn]struct{}
	cancel              context.CancelFunc
	client              *http.Client
	peers               []string
	username, password  string
	single              bool
	pgPort, patroniPort string
	network             string
	dialer              net.Dialer
	interval            time.Duration
	waitTimeout         time.Duration
}

func newForwarder(s sbspec.Spec, address string) (*forwarder, error) {
	roots := x509.NewCertPool()
	if s.Topology == "ha" && !roots.AppendCertsFromPEM([]byte(s.CAPEM)) {
		return nil, errors.New("invalid Patroni CA")
	}
	l, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	network := "tcp4"
	if s.DatabaseIPv6 {
		network = "tcp6"
	}
	f := &forwarder{listener: l, connections: make(map[net.Conn]struct{}), changed: make(chan struct{}), peers: s.DataPeers, username: s.PatroniUsername, password: s.PatroniPassword, single: s.Topology == "single", pgPort: "5432", patroniPort: "8008", network: network, dialer: net.Dialer{Timeout: 3 * time.Second}, interval: time.Second, waitTimeout: 10 * time.Second}
	f.client = &http.Client{Timeout: 750 * time.Millisecond, Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, address string) (net.Conn, error) {
			return f.dialer.DialContext(ctx, f.network, address)
		},
		TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
	}}
	return f, nil
}
func (f *forwarder) start(ctx context.Context) {
	ctx, f.cancel = context.WithCancel(ctx)
	if f.single {
		f.setPrimary(net.JoinHostPort(f.peers[0], f.pgPort))
	}
	if !f.single {
		f.probe(ctx)
	}
	go func() { <-ctx.Done(); f.close() }()
	go func() {
		t := time.NewTicker(f.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if !f.single {
					f.probe(ctx)
				}
			}
		}
	}()
	go func() {
		for {
			c, err := f.listener.Accept()
			if err != nil {
				return
			}
			go f.serve(ctx, c)
		}
	}()
}
func (f *forwarder) probe(ctx context.Context) {
	// Probe concurrently so an unreachable peer cannot delay a healthy primary.
	type answer struct {
		address string
		primary bool
	}
	answers := make(chan answer, len(f.peers))
	for _, peer := range f.peers {
		go func(peer string) {
			result := answer{address: net.JoinHostPort(peer, f.pgPort)}
			req, err := http.NewRequestWithContext(ctx, "GET", "https://"+net.JoinHostPort(peer, f.patroniPort)+"/primary", nil)
			if err == nil {
				req.SetBasicAuth(f.username, f.password)
				response, e := f.client.Do(req)
				if e == nil {
					response.Body.Close()
					if response.StatusCode == 200 {
						result.primary = true
						answers <- result
						return
					}
				}
			}
			answers <- result
		}(peer)
	}
	primary := ""
	count := 0
	for range f.peers {
		result := <-answers
		if result.primary {
			primary = result.address
			count++
			if count == 2 {
				// Ambiguity is already known: revoke the selected writer
				// before waiting for any remaining slow peer.
				f.setPrimary("")
			}
		} else {
			// Revoke a disproven writer immediately, without waiting for an
			// unrelated unreachable peer's probe timeout.
			f.mu.Lock()
			if f.primary == result.address {
				f.setPrimaryLocked("")
			}
			f.mu.Unlock()
		}
	}
	if count != 1 {
		primary = ""
	}
	f.setPrimary(primary)
}
func (f *forwarder) setPrimary(primary string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	f.setPrimaryLocked(primary)
}

func (f *forwarder) setPrimaryLocked(primary string) {
	if f.primary == primary {
		return
	}
	f.primary = primary
	f.generation++
	close(f.changed)
	f.changed = make(chan struct{})
	for c := range f.connections {
		c.Close()
		delete(f.connections, c)
	}
}

// waitPrimary holds a new session through a bounded leader-election gap instead
// of turning an absent writer into an immediate EOF/reconnect loop. It never
// replays traffic: existing sessions are still closed on every leadership loss.
func (f *forwarder) waitPrimary(ctx context.Context) (string, uint64, bool) {
	var deadline *time.Timer
	defer func() {
		if deadline != nil {
			deadline.Stop()
		}
	}()
	for {
		f.mu.Lock()
		primary, generation, changed, closed := f.primary, f.generation, f.changed, f.closed
		f.mu.Unlock()
		if closed || ctx.Err() != nil {
			return "", 0, false
		}
		if primary != "" {
			return primary, generation, true
		}
		if deadline == nil {
			deadline = time.NewTimer(f.waitTimeout)
		}
		select {
		case <-ctx.Done():
			return "", 0, false
		case <-deadline.C:
			return "", 0, false
		case <-changed:
		}
	}
}
func (f *forwarder) serve(ctx context.Context, c net.Conn) {
	defer c.Close()
	primary, generation, ok := f.waitPrimary(ctx)
	if !ok {
		return
	}
	upstream, err := f.dialer.DialContext(ctx, f.network, primary)
	if err != nil {
		return
	}
	defer upstream.Close()
	f.mu.Lock()
	if f.closed || f.primary != primary || f.generation != generation {
		f.mu.Unlock()
		return
	}
	f.connections[c] = struct{}{}
	f.connections[upstream] = struct{}{}
	f.mu.Unlock()
	defer func() { f.mu.Lock(); delete(f.connections, c); delete(f.connections, upstream); f.mu.Unlock() }()
	done := make(chan struct{})
	go func() { io.Copy(upstream, c); upstream.Close(); close(done) }()
	io.Copy(c, upstream)
	c.Close()
	<-done
}
func (f *forwarder) close() {
	f.listener.Close()
	f.mu.Lock()
	if !f.closed {
		f.closed = true
		f.primary = ""
		for c := range f.connections {
			c.Close()
			delete(f.connections, c)
		}
		close(f.changed)
	}
	f.mu.Unlock()
	f.client.CloseIdleConnections()
}
func (f *forwarder) stop() {
	f.close()
	if f.cancel != nil {
		f.cancel()
	}
}
