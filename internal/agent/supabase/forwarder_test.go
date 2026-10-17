// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
)

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func echoListener(t *testing.T, address string) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			go func() { defer c.Close(); io.Copy(c, c) }()
		}
	}()
	return l
}
func TestForwarderFailover(t *testing.T) {
	first := echoListener(t, "127.0.0.2:0")
	_, port, _ := net.SplitHostPort(first.Addr().String())
	echoListener(t, net.JoinHostPort("127.0.0.3", port))
	var primary atomic.Int32
	primary.Store(1)
	server := func(index int32) *httptest.Server {
		return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			u, p, ok := r.BasicAuth()
			if !ok || u != "patroni" || p != "secret" {
				t.Error("missing Patroni credentials")
				w.WriteHeader(401)
				return
			}
			if r.URL.Path != "/primary" {
				t.Error("wrong Patroni endpoint")
			}
			if primary.Load() != index {
				w.WriteHeader(503)
			}
		}))
	}
	one, two := server(1), server(2)
	defer one.Close()
	defer two.Close()
	f, err := newForwarder(sbspec.Spec{Topology: "single", DataPeers: []string{"127.0.0.2", "127.0.0.3"}}, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer f.stop()
	f.single = false
	f.username = "patroni"
	f.password = "secret"
	f.pgPort = port
	f.interval = 20 * time.Millisecond
	f.client = &http.Client{Transport: roundTripper(func(r *http.Request) (*http.Response, error) {
		s := one
		if r.URL.Hostname() == "127.0.0.3" {
			s = two
		}
		destination, _ := url.Parse(s.URL)
		copy := r.Clone(r.Context())
		copy.URL.Scheme = destination.Scheme
		copy.URL.Host = destination.Host
		return s.Client().Do(copy)
	}), Timeout: time.Second}
	f.start(context.Background())
	dial := func() net.Conn {
		c, e := net.DialTimeout("tcp", f.listener.Addr().String(), time.Second)
		if e != nil {
			t.Fatal(e)
		}
		c.SetDeadline(time.Now().Add(2 * time.Second))
		return c
	}
	c := dial()
	defer c.Close()
	if _, err := c.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 5)
	if _, err := io.ReadFull(c, b); err != nil || string(b) != "hello" {
		t.Fatal("initial forward failed")
	}
	primary.Store(2)
	deadline := time.Now().Add(2 * time.Second)
	for {
		f.mu.Lock()
		p := f.primary
		f.mu.Unlock()
		if p == net.JoinHostPort("127.0.0.3", port) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("primary did not change")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := c.Read(b); err == nil {
		t.Fatal("old connection survived primary change")
	}
	next := dial()
	defer next.Close()
	next.Write([]byte("world"))
	if _, err := io.ReadFull(next, b); err != nil || string(b) != "world" {
		t.Fatal("new primary forward failed")
	}
	primary.Store(0)
	f.probe(context.Background())
	f.mu.Lock()
	p := f.primary
	f.mu.Unlock()
	if p != "" {
		t.Fatal("forwarder retained unavailable primary")
	}
}

func TestForwarderPrivateAddressFamily(t *testing.T) {
	for _, ipv6 := range []bool{false, true} {
		name := "ipv4"
		if ipv6 {
			name = "ipv6"
		}
		t.Run(name, func(t *testing.T) {
			v4, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer v4.Close()
			_, port, _ := net.SplitHostPort(v4.Addr().String())
			v6, err := net.Listen("tcp6", net.JoinHostPort("::1", port))
			if err != nil {
				t.Skip("IPv6 loopback is unavailable")
			}
			defer v6.Close()
			for listener, marker := range map[net.Listener]string{v4: "ipv4", v6: "ipv6"} {
				go func(l net.Listener, marker string) {
					for {
						c, err := l.Accept()
						if err != nil {
							return
						}
						io.WriteString(c, marker)
						c.Close()
					}
				}(listener, marker)
			}
			f, err := newForwarder(sbspec.Spec{Topology: "single", DataPeers: []string{"localhost"}, DatabaseIPv6: ipv6}, "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer f.stop()
			f.pgPort = port
			f.start(context.Background())
			check := func(c net.Conn, err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				c.SetReadDeadline(time.Now().Add(time.Second))
				marker := make([]byte, len(name))
				if _, err := io.ReadFull(c, marker); err != nil || string(marker) != name {
					t.Fatalf("private forwarding used the wrong address family: %q, %v", marker, err)
				}
			}
			check(net.DialTimeout("tcp", f.listener.Addr().String(), time.Second))
			transport := f.client.Transport.(*http.Transport)
			check(transport.DialContext(context.Background(), "tcp", net.JoinHostPort("localhost", port)))
		})
	}
}

func TestForwarderWaitsForConfirmedPrimary(t *testing.T) {
	upstream := echoListener(t, "127.0.0.1:0")
	f, err := newForwarder(sbspec.Spec{Topology: "single", DataPeers: []string{"127.0.0.1"}}, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer f.stop()
	server, client := net.Pipe()
	defer client.Close()
	go f.serve(context.Background(), server)
	client.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	b := make([]byte, 5)
	if _, err := client.Read(b); err == nil {
		t.Fatal("session reached an unconfirmed primary")
	} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatalf("no-leader session was closed instead of held: %v", err)
	}
	f.setPrimary(upstream.Addr().String())
	client.SetDeadline(time.Now().Add(time.Second))
	if _, err := client.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(client, b); err != nil || string(b) != "hello" {
		t.Fatalf("held session did not reach confirmed primary: %q, %v", b, err)
	}
}

func TestForwarderPrimaryWaitEnds(t *testing.T) {
	for _, mode := range []string{"timeout", "cancel", "stop"} {
		t.Run(mode, func(t *testing.T) {
			f, err := newForwarder(sbspec.Spec{Topology: "single", DataPeers: []string{"127.0.0.1"}}, "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer f.stop()
			if mode == "timeout" {
				f.waitTimeout = 50 * time.Millisecond
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			server, client := net.Pipe()
			defer client.Close()
			go f.serve(ctx, server)
			switch mode {
			case "cancel":
				cancel()
			case "stop":
				f.stop()
				// An in-flight probe must not restore a writer after shutdown.
				f.setPrimary("127.0.0.1:5432")
			}
			client.SetReadDeadline(time.Now().Add(time.Second))
			if _, err := client.Read(make([]byte, 1)); err != io.EOF {
				t.Fatalf("%s did not close held session: %v", mode, err)
			}
			f.mu.Lock()
			primary := f.primary
			f.mu.Unlock()
			if primary != "" {
				t.Fatalf("%s restored an unconfirmed writer", mode)
			}
		})
	}
}

func TestForwarderRevokesPrimaryBeforeSlowPeer(t *testing.T) {
	for _, failure := range []string{"not-primary", "unreachable"} {
		t.Run(failure, func(t *testing.T) {
			f, err := newForwarder(sbspec.Spec{Topology: "single", DataPeers: []string{"127.0.0.2", "127.0.0.3"}}, "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer f.stop()
			old := net.JoinHostPort("127.0.0.2", f.pgPort)
			f.setPrimary(old)
			f.mu.Lock()
			changed := f.changed
			f.mu.Unlock()
			release := make(chan struct{})
			defer close(release)
			f.client = &http.Client{Transport: roundTripper(func(r *http.Request) (*http.Response, error) {
				if r.URL.Hostname() == "127.0.0.3" {
					<-release
				}
				if failure == "unreachable" {
					return nil, io.ErrUnexpectedEOF
				}
				return &http.Response{StatusCode: 503, Body: http.NoBody}, nil
			})}
			done := make(chan struct{})
			go func() {
				f.probe(context.Background())
				close(done)
			}()
			select {
			case <-changed:
			case <-time.After(time.Second):
				t.Fatal("old writer retained while unrelated peer probe was blocked")
			}
			f.mu.Lock()
			primary := f.primary
			f.mu.Unlock()
			if primary != "" {
				t.Fatal("negative old-primary probe did not revoke the writer")
			}
			select {
			case <-done:
				t.Fatal("probe unexpectedly completed before slow peer responded")
			default:
			}
		})
	}
}

func TestForwarderRejectsAmbiguousPrimary(t *testing.T) {
	f, err := newForwarder(sbspec.Spec{Topology: "single", DataPeers: []string{"127.0.0.2", "127.0.0.3"}}, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer f.stop()
	f.setPrimary("127.0.0.2:5432")
	f.client = &http.Client{Transport: roundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
	})}
	f.probe(context.Background())
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.primary != "" {
		t.Fatal("more than one confirmed primary must fail closed")
	}
}

func TestForwarderRevokesAmbiguousPrimaryBeforeSlowPeer(t *testing.T) {
	upstream := echoListener(t, "127.0.0.1:0")
	f, err := newForwarder(sbspec.Spec{Topology: "single", DataPeers: []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"}}, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer f.stop()
	_, f.pgPort, _ = net.SplitHostPort(upstream.Addr().String())
	f.setPrimary(upstream.Addr().String())
	server, client := net.Pipe()
	defer client.Close()
	go f.serve(context.Background(), server)
	client.SetDeadline(time.Now().Add(time.Second))
	if _, err := client.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 5)
	if _, err := io.ReadFull(client, b); err != nil || string(b) != "hello" {
		t.Fatalf("initial session did not reach selected writer: %q, %v", b, err)
	}
	f.mu.Lock()
	changed := f.changed
	f.mu.Unlock()
	release := make(chan struct{})
	defer close(release)
	f.client = &http.Client{Transport: roundTripper(func(r *http.Request) (*http.Response, error) {
		status := http.StatusOK
		if r.URL.Hostname() == "127.0.0.3" {
			<-release
			status = http.StatusServiceUnavailable
		}
		return &http.Response{StatusCode: status, Body: http.NoBody}, nil
	})}
	done := make(chan struct{})
	go func() {
		f.probe(context.Background())
		close(done)
	}()
	select {
	case <-changed:
	case <-time.After(time.Second):
		t.Fatal("ambiguous writer retained while third peer probe was blocked")
	}
	f.mu.Lock()
	primary := f.primary
	f.mu.Unlock()
	if primary != "" {
		t.Fatal("second positive probe did not revoke selected writer")
	}
	client.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := client.Read(b); err != io.EOF {
		t.Fatalf("existing session survived known ambiguous leadership: %v", err)
	}
	select {
	case <-done:
		t.Fatal("probe unexpectedly completed before slow peer responded")
	default:
	}
	release <- struct{}{}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("probe did not finish after slow peer responded")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.primary != "" {
		t.Fatal("completed ambiguous probe cycle restored a writer")
	}
}

func TestForwarderRejectsDialAcrossLeadershipGap(t *testing.T) {
	upstream := echoListener(t, "127.0.0.1:0")
	f, err := newForwarder(sbspec.Spec{Topology: "single", DataPeers: []string{"127.0.0.1"}}, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer f.stop()
	primary := upstream.Addr().String()
	f.setPrimary(primary)
	dialing, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	f.dialer.Control = func(string, string, syscall.RawConn) error {
		close(dialing)
		<-release
		return nil
	}
	server, client := net.Pipe()
	defer client.Close()
	done := make(chan struct{})
	go func() {
		f.serve(context.Background(), server)
		close(done)
	}()
	select {
	case <-dialing:
	case <-time.After(time.Second):
		t.Fatal("upstream dial did not start")
	}
	f.setPrimary("")
	f.setPrimary(primary)
	release <- struct{}{}
	client.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := client.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("a pre-gap dial survived same-address leader confirmation: %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stale dial did not terminate")
	}
}
