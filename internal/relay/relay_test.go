// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package relay

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fixture struct {
	t          *testing.T
	ca, key    []byte
	hubCert    tls.Certificate
	o          Options
	hub        *Hub
	server     *httptest.Server
	echo       net.Listener
	hubOptions HubOptions
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ca, key, err := GenerateCA(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	cert, certKey, err := IssueHubCertificate(ca, key, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(cert, certKey)
	if err != nil {
		t.Fatal(err)
	}
	client, clientKey, err := IssueClientCertificate(ca, key, "proj-test", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	o := Options{Namespace: "proj-test", Hostname: "relayhost", CertPEM: client, KeyPEM: clientKey, CAPEM: ca, Logger: logger, HandshakeTimeout: time.Second, DialTimeout: time.Second, BackoffMin: 10 * time.Millisecond, BackoffMax: 20 * time.Millisecond, Backoff: func(d time.Duration) time.Duration { return d }, PingInterval: 100 * time.Millisecond}
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := echo.Accept()
			if err != nil {
				return
			}
			go func() { defer conn.Close(); io.Copy(conn, conn) }()
		}
	}()
	o.Resolver = func(_ context.Context, host string) ([]netip.Addr, error) {
		if host == "loopback" {
			return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
		}
		return []netip.Addr{netip.MustParseAddr("192.0.2.10")}, nil
	}
	o.DialContext = func(ctx context.Context, _, addr string) (net.Conn, error) {
		if !strings.HasPrefix(addr, "192.0.2.10:") {
			return nil, errors.New("unexpected resolved address")
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", echo.Addr().String())
	}
	f := &fixture{t: t, ca: ca, key: key, hubCert: pair, o: o, echo: echo, hubOptions: HubOptions{Certificate: pair, ClientCAs: roots, Authorize: func(context.Context, string, *x509.Certificate) error { return nil }, Logger: logger, HandshakeTimeout: time.Second, PingInterval: 100 * time.Millisecond, PolicyInterval: 50 * time.Millisecond}}
	f.startHub("")
	t.Cleanup(func() { f.hub.Close(); f.server.Close(); echo.Close() })
	return f
}
func (f *fixture) startHub(addr string) {
	f.hub = NewHub(f.hubOptions)
	srv := httptest.NewUnstartedServer(f.hub.Handler())
	if addr != "" {
		srv.Listener.Close()
		l, err := net.Listen("tcp", addr)
		if err != nil {
			f.t.Fatal(err)
		}
		srv.Listener = l
	}
	srv.StartTLS()
	f.server = srv
	f.o.URL = srv.URL
	f.o.HTTPClient = srv.Client()
}
func localAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}
func (f *fixture) runRelay(o Options) (context.CancelFunc, <-chan error) {
	f.t.Helper()
	if o.ListenAgent == "" {
		o.ListenAgent = localAddress(f.t)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, o) }()
	f.t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				f.t.Error(err)
			}
		case <-time.After(2 * time.Second):
			f.t.Error("relay shutdown timed out")
		}
	})
	return cancel, done
}
func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
func (f *fixture) connected() {
	eventually(f.t, func() bool { return f.hub.Status("proj-test").Connected })
}
func tunnelEcho(h *Hub, target string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := h.DialContext(ctx, "proj-test", "tcp", target)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(time.Second))
	if conn.RemoteAddr().String() != target {
		return errors.New("incorrect remote address")
	}
	if _, err = conn.Write([]byte("echo")); err != nil {
		return err
	}
	out := make([]byte, 4)
	if _, err = io.ReadFull(conn, out); err != nil {
		return err
	}
	if string(out) != "echo" {
		return errors.New("incorrect echo")
	}
	return nil
}

func TestTunnelPolicyAndConcurrentStreams(t *testing.T) {
	f := newFixture(t)
	f.hub.SetPolicy("proj-test", []string{"member:27017"})
	f.runRelay(f.o)
	f.connected()
	if err := tunnelEcho(f.hub, "member:27017"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for range 32 {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- tunnelEcho(f.hub, "member:27017") }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	status := f.hub.Status("proj-test")
	if status.Hello.Namespace != "proj-test" || status.ConnectedSince.IsZero() || status.LastSeen.IsZero() {
		t.Fatal("missing status metadata")
	}
	leaf, err := ParseCertificatePEM(f.o.CertPEM)
	if err != nil {
		t.Fatal(err)
	}
	if status.CertificateSerial != leaf.SerialNumber.String() {
		t.Fatal("session snapshot lacks authenticated enrollment identity")
	}
}
func TestTargetRefusals(t *testing.T) {
	f := newFixture(t)
	targets := []string{"member:22", "192.0.2.1:27017", "member.example:27017", "core:27017", "relayhost:9443", "loopback:27017"}
	f.hub.SetPolicy("proj-test", targets)
	f.runRelay(f.o)
	f.connected()
	for _, target := range append(targets, "unknown:27017") {
		t.Run(target, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := f.hub.DialContext(ctx, "proj-test", "tcp", target)
			var refusal *ErrRefused
			if !errors.As(err, &refusal) {
				t.Fatal("expected refusal", err)
			}
		})
	}
}
func TestAuthenticationFailures(t *testing.T) {
	for _, kind := range []string{"wrong-client-ca", "revoked-serial", "untrusted-hub"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			otherCA, otherKey, err := GenerateCA(time.Now())
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "wrong-client-ca":
				f.o.CertPEM, f.o.KeyPEM, err = IssueClientCertificate(otherCA, otherKey, "proj-test", time.Now())
			case "revoked-serial":
				f.hub.o.Authorize = func(_ context.Context, _ string, cert *x509.Certificate) error {
					if cert.SerialNumber != nil {
						return errors.New("certificate revoked")
					}
					return nil
				}
			case "untrusted-hub":
				f.o.CAPEM = otherCA
			}
			if err != nil {
				t.Fatal(err)
			}
			o := defaults(f.o)
			cfg, err := clientTLS(o)
			if err != nil {
				t.Fatal(err)
			}
			endpoint, err := connectionURL(o.URL)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			r := &runtime{o: o}
			if err = r.connect(ctx, endpoint, cfg); err == nil {
				t.Fatal("untrusted tunnel accepted")
			}
			if f.hub.Status("proj-test").Connected {
				t.Fatal("failed authentication published session")
			}
		})
	}
}
func TestHubRestartAndPolicyRepush(t *testing.T) {
	f := newFixture(t)
	f.hub.SetPolicy("proj-test", []string{"member:27017"})
	f.runRelay(f.o)
	f.connected()
	addr := f.server.Listener.Addr().String()
	f.hub.Close()
	f.server.Close()
	f.startHub(addr)
	f.hub.SetPolicy("proj-test", []string{"replacement:9443"})
	f.connected()
	if err := tunnelEcho(f.hub, "replacement:9443"); err != nil {
		t.Fatal(err)
	}
	_, err := f.hub.DialContext(context.Background(), "proj-test", "tcp", "member:27017")
	var refusal *ErrRefused
	if !errors.As(err, &refusal) {
		t.Fatal("old policy remained active", err)
	}
}
func TestRelayRestartAndNoSession(t *testing.T) {
	f := newFixture(t)
	if _, err := f.hub.DialContext(context.Background(), "proj-test", "tcp", "member:27017"); !errors.Is(err, ErrNoSession) {
		t.Fatal(err)
	}
	f.hub.SetPolicy("proj-test", []string{"member:27017"})
	cancel, _ := f.runRelay(f.o)
	f.connected()
	cancel()
	eventually(t, func() bool { return !f.hub.Status("proj-test").Connected })
	if _, err := f.hub.DialContext(context.Background(), "proj-test", "tcp", "member:27017"); !errors.Is(err, ErrNoSession) {
		t.Fatal(err)
	}
	f.runRelay(f.o)
	f.connected()
	if err := tunnelEcho(f.hub, "member:27017"); err != nil {
		t.Fatal(err)
	}
	if f.hub.Status("proj-test").Reconnects != 1 {
		t.Fatal("reconnect count not retained")
	}
}
func TestSessionReplacement(t *testing.T) {
	f := newFixture(t)
	f.hub.SetPolicy("proj-test", []string{"member:27017"})
	first := f.o
	first.BackoffMin = time.Second
	first.BackoffMax = time.Second
	cancel, _ := f.runRelay(first)
	f.connected()
	oldSince := f.hub.Status("proj-test").ConnectedSince
	second := f.o
	second.Hostname = "secondrelay"
	f.runRelay(second)
	eventually(t, func() bool { return f.hub.Status("proj-test").Hello.Hostname == "secondrelay" })
	cancel()
	status := f.hub.Status("proj-test")
	if !status.ConnectedSince.After(oldSince) || status.Reconnects != 1 {
		t.Fatal("session did not replace prior connection")
	}
	if err := tunnelEcho(f.hub, "member:27017"); err != nil {
		t.Fatal(err)
	}
}
func TestInboundAgentTLS(t *testing.T) {
	f := newFixture(t)
	o := f.o
	o.ListenAgent = localAddress(t)
	f.runRelay(o)
	f.connected()
	serverDone := make(chan error, 1)
	go func() {
		raw, err := f.hub.Listener().Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer raw.Close()
		conn := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{f.hubCert}, MinVersion: tls.VersionTLS13})
		if err = conn.Handshake(); err != nil {
			serverDone <- err
			return
		}
		payload, err := io.ReadAll(conn)
		if err == nil {
			_, err = conn.Write(payload)
		}
		if err == nil {
			err = conn.CloseWrite()
		}
		serverDone <- err
	}()
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(f.ca)
	raw, err := net.DialTimeout("tcp", o.ListenAgent, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	raw.SetDeadline(time.Now().Add(time.Second))
	conn := tls.Client(raw, &tls.Config{RootCAs: roots, ServerName: hubServerName, MinVersion: tls.VersionTLS13})
	if err = conn.Handshake(); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Write([]byte("agent")); err != nil {
		t.Fatal(err)
	}
	if err = conn.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	reply, err := io.ReadAll(conn)
	if err != nil || string(reply) != "agent" {
		t.Fatal(string(reply), err)
	}
	if err = <-serverDone; err != nil {
		t.Fatal(err)
	}
}
func TestReadinessWithoutTunnel(t *testing.T) {
	f := newFixture(t)
	o := f.o
	o.ListenAgent = localAddress(t)
	o.URL = "https://127.0.0.1:1"
	f.runRelay(o)
	eventually(t, func() bool { return Readiness(context.Background(), o) == nil })
	if f.hub.Status("proj-test").Connected {
		t.Fatal("unexpected tunnel")
	}
}
func TestListenerCloseAndUpgradeValidation(t *testing.T) {
	f := newFixture(t)
	r := httptest.NewRequest(http.MethodGet, "/relay/v1/connect", nil)
	w := httptest.NewRecorder()
	f.hub.Handler().ServeHTTP(w, r)
	if w.Code == http.StatusSwitchingProtocols {
		t.Fatal("non-upgrade accepted")
	}
	listener := f.hub.Listener()
	done := make(chan error, 1)
	go func() { _, err := listener.Accept(); done <- err }()
	listener.Close()
	if err := <-done; !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
	if listener.Addr().String() == "" {
		t.Fatal("missing listener address")
	}
}
func TestPolicyUpdatesAndLastSeen(t *testing.T) {
	f := newFixture(t)
	f.runRelay(f.o)
	f.connected()
	initial := f.hub.Status("proj-test").LastSeen
	f.hub.SetPolicy("proj-test", []string{"member:27017"})
	eventually(t, func() bool { return f.hub.Status("proj-test").LastSeen.After(initial) })
	if err := tunnelEcho(f.hub, "member:27017"); err != nil {
		t.Fatal(err)
	}
	last := f.hub.Status("proj-test").LastSeen
	eventually(t, func() bool { return f.hub.Status("proj-test").LastSeen.After(last) })
}
func TestResolvedAddressRestrictions(t *testing.T) {
	o := defaults(Options{Hostname: "relayhost"})
	for _, ip := range []string{"127.0.0.1", "::1", "169.254.1.2", "fe80::1", "0.0.0.0", "::", "224.0.0.1", "ff02::1", "::ffff:127.0.0.1"} {
		t.Run(ip, func(t *testing.T) {
			var dialed atomic.Bool
			o.Resolver = func(context.Context, string) ([]netip.Addr, error) {
				return []netip.Addr{netip.MustParseAddr("192.0.2.10"), netip.MustParseAddr(ip)}, nil
			}
			o.DialContext = func(context.Context, string, string) (net.Conn, error) {
				dialed.Store(true)
				return nil, errors.New("unexpected dial")
			}
			r := runtime{o: o}
			s := &relaySession{targets: map[string]struct{}{"member:27017": {}}}
			if _, err := r.target(context.Background(), s, "member:27017"); err == nil || dialed.Load() {
				t.Fatal("prohibited resolution dialed")
			}
		})
	}
}
