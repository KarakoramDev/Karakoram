// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package relay

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/xtaci/smux"
)

func pendingControl(t *testing.T, f *fixture, o Options) (*smux.Session, *smux.Stream) {
	t.Helper()
	endpoint, err := connectionURL(f.server.URL)
	if err != nil {
		t.Fatal(err)
	}
	ws, _, err := websocket.Dial(t.Context(), endpoint, &websocket.DialOptions{HTTPClient: f.server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ws.CloseNow() })
	config, err := clientTLS(o)
	if err != nil {
		t.Fatal(err)
	}
	conn := tls.Client(websocketConn(t.Context(), ws), config)
	if err = conn.HandshakeContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	mux, err := smux.Client(conn, muxConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mux.Close() })
	control, err := mux.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	control.SetDeadline(time.Now().Add(2 * time.Second))
	if err = WriteControlHeader(control, Hello{Type: "hello", Namespace: o.Namespace, Hostname: o.Hostname}); err != nil {
		t.Fatal(err)
	}
	if _, err = ReadControlHeader(control); err != nil {
		t.Fatal(err)
	}
	return mux, control
}

func expectPeerClosed(t *testing.T, mux *smux.Session) {
	t.Helper()
	// smux records peer EOF as a socket error; IsClosed describes local Close,
	// not remote transport shutdown.
	mux.SetDeadline(time.Now().Add(2 * time.Second))
	stream, err := mux.AcceptStream()
	if stream != nil {
		stream.Close()
		t.Fatal("revoked peer opened a stream")
	}
	if err == nil || errors.Is(err, smux.ErrTimeout) {
		t.Fatalf("peer transport did not close: %v", err)
	}
}

func TestRevokedPendingSessionCannotReplaceCurrent(t *testing.T) {
	f := newFixture(t)
	old, err := ParseCertificatePEM(f.o.CertPEM)
	if err != nil {
		t.Fatal(err)
	}
	cert, key, err := IssueClientCertificate(f.ca, f.key, f.o.Namespace, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	current, err := ParseCertificatePEM(cert)
	if err != nil {
		t.Fatal(err)
	}
	f.hub.Enroll(f.o.Namespace, current.SerialNumber.String(), old.SerialNumber.String(), old.NotAfter)
	oldMux, oldControl := pendingControl(t, f, f.o)
	next := f.o
	next.CertPEM, next.KeyPEM, next.Hostname = cert, key, "newrelay"
	f.runRelay(next)
	eventually(t, func() bool { return f.hub.Status(next.Namespace).Hello.Hostname == "newrelay" })
	// The old peer has held the policy acknowledgement across current admission.
	_ = WriteHeader(oldControl, Reply{OK: true})
	expectPeerClosed(t, oldMux)
	f.hub.Enroll(next.Namespace, current.SerialNumber.String(), old.SerialNumber.String(), old.NotAfter)
	if f.hub.AcceptsSerial(next.Namespace, old.SerialNumber.String()) {
		t.Fatal("stale enrollment snapshot restored revoked overlap")
	}
	status := f.hub.Status(next.Namespace)
	if !status.Connected || status.CertificateSerial != current.SerialNumber.String() {
		t.Fatal("revoked admission replaced current session", status)
	}
}

func TestConditionalRevocationPreservesSessionAndOverlapFence(t *testing.T) {
	f := newFixture(t)
	old, err := ParseCertificatePEM(f.o.CertPEM)
	if err != nil {
		t.Fatal(err)
	}
	cert, key, err := IssueClientCertificate(f.ca, f.key, f.o.Namespace, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	current, err := ParseCertificatePEM(cert)
	if err != nil {
		t.Fatal(err)
	}
	f.hub.SetPolicy(f.o.Namespace, []string{"member:27017"})
	oldMux, oldControl := pendingControl(t, f, f.o)
	if err = WriteHeader(oldControl, Reply{OK: true}); err != nil {
		t.Fatal(err)
	}
	f.connected()
	f.hub.Enroll(f.o.Namespace, current.SerialNumber.String(), old.SerialNumber.String(), old.NotAfter)
	snapshot := f.hub.Status(f.o.Namespace)
	next := f.o
	next.CertPEM, next.KeyPEM = cert, key
	currentMux, currentControl := pendingControl(t, f, next)
	if err = WriteHeader(currentControl, Reply{OK: true}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return f.hub.Status(next.Namespace).CertificateSerial == current.SerialNumber.String() })
	expectPeerClosed(t, oldMux)
	status := f.hub.RevokeIfSerial(next.Namespace, snapshot.CertificateSerial)
	if !status.Connected || status.CertificateSerial != current.SerialNumber.String() {
		t.Fatal("stale revocation closed the new session", status)
	}
	f.hub.Enroll(next.Namespace, current.SerialNumber.String(), old.SerialNumber.String(), old.NotAfter)
	if f.hub.AcceptsSerial(next.Namespace, old.SerialNumber.String()) {
		t.Fatal("stale revocation discarded the overlap fence")
	}
	status = f.hub.RevokeIfSerial(next.Namespace, current.SerialNumber.String())
	if status != (SessionStatus{}) {
		t.Fatal("matching revocation retained session status", status)
	}
	expectPeerClosed(t, currentMux)
	f.hub.Enroll(next.Namespace, current.SerialNumber.String(), old.SerialNumber.String(), old.NotAfter)
	if f.hub.AcceptsSerial(next.Namespace, old.SerialNumber.String()) {
		t.Fatal("matching revocation discarded the overlap fence")
	}
	f.hub.mu.Lock()
	targets := len(f.hub.states[next.Namespace].policy.Targets)
	f.hub.mu.Unlock()
	if targets != 1 {
		t.Fatal("session reconciliation discarded namespace policy")
	}
}

func TestPublicationReauthorizesEnrollment(t *testing.T) {
	f := newFixture(t)
	f.hub.Close()
	f.server.Close()
	allowed := true
	f.hubOptions.Authorize = func(context.Context, string, *x509.Certificate) error {
		if !allowed {
			return errors.New("revoked")
		}
		return nil
	}
	f.startHub("")
	mux, control := pendingControl(t, f, f.o)
	f.hub.mu.Lock()
	allowed = false
	f.hub.mu.Unlock()
	if err := WriteHeader(control, Reply{OK: true}); err != nil {
		t.Fatal(err)
	}
	expectPeerClosed(t, mux)
	if f.hub.Status(f.o.Namespace).Connected {
		t.Fatal("revoked certificate published")
	}
}

func TestRevokeClearsActivePendingAndPolicy(t *testing.T) {
	f := newFixture(t)
	f.hub.SetPolicy(f.o.Namespace, []string{"member:27017"})
	active, control := pendingControl(t, f, f.o)
	if err := WriteHeader(control, Reply{OK: true}); err != nil {
		t.Fatal(err)
	}
	f.connected()
	pending, _ := pendingControl(t, f, f.o)
	f.hub.Revoke(f.o.Namespace)
	expectPeerClosed(t, active)
	expectPeerClosed(t, pending)
	status := f.hub.Status(f.o.Namespace)
	if status != (SessionStatus{}) {
		t.Fatal("revoked session status survived", status)
	}
	f.hub.mu.Lock()
	targets := len(f.hub.state(f.o.Namespace).policy.Targets)
	f.hub.mu.Unlock()
	if targets != 0 {
		t.Fatal("revoked policy survived")
	}
}

func TestEnrollmentChangeClosesUnacceptedActiveSession(t *testing.T) {
	f := newFixture(t)
	mux, control := pendingControl(t, f, f.o)
	if err := WriteHeader(control, Reply{OK: true}); err != nil {
		t.Fatal(err)
	}
	f.connected()
	f.hub.Enroll(f.o.Namespace, "replacement", "", time.Time{})
	expectPeerClosed(t, mux)
	if f.hub.Status(f.o.Namespace).Connected {
		t.Fatal("unenrolled session stayed active")
	}
}

func TestActiveStreamCapacityAndRelease(t *testing.T) {
	f := newFixture(t)
	f.hub.SetPolicy(f.o.Namespace, []string{"member:27017"})
	f.runRelay(f.o)
	f.connected()
	var connections []io.Closer
	defer func() {
		for _, c := range connections {
			c.Close()
		}
	}()
	for range MaxActiveStreams {
		conn, err := f.hub.DialContext(t.Context(), f.o.Namespace, "tcp", "member:27017")
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, conn)
	}
	if _, err := f.hub.DialContext(t.Context(), f.o.Namespace, "tcp", "member:27017"); !errors.Is(err, ErrStreamCapacity) {
		t.Fatal("excess stream was not refused", err)
	}
	connections[0].Close()
	connections[0].Close()
	// Wait for the remote stream handler to finish too.
	var conn io.Closer
	eventually(t, func() bool {
		c, err := f.hub.DialContext(t.Context(), f.o.Namespace, "tcp", "member:27017")
		if err != nil {
			return false
		}
		conn = c
		return true
	})
	conn.Close()
}

func TestInboundAgentStreamCapacity(t *testing.T) {
	f := newFixture(t)
	mux, control := pendingControl(t, f, f.o)
	if err := WriteHeader(control, Reply{OK: true}); err != nil {
		t.Fatal(err)
	}
	f.connected()
	for range MaxActiveStreams {
		s, err := mux.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if err = WriteHeader(s, Agent{Type: "agent"}); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, func() bool {
		f.hub.mu.Lock()
		defer f.hub.mu.Unlock()
		return f.hub.state(f.o.Namespace).session.streams.active.Load() == MaxActiveStreams
	})
	excess, err := mux.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	excess.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, err = excess.Read(b[:]); !errors.Is(err, io.EOF) {
		t.Fatal("excess agent stream was not closed", err)
	}
}

func TestWebSocketCarrierReadLimit(t *testing.T) {
	result := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			result <- err
			return
		}
		defer ws.CloseNow()
		_, err = io.Copy(io.Discard, websocketConn(r.Context(), ws))
		result <- err
	}))
	defer server.Close()
	ws, _, err := websocket.Dial(t.Context(), "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.CloseNow()
	_ = ws.Write(t.Context(), websocket.MessageBinary, make([]byte, MaxWebSocketMessageSize+1))
	select {
	case err = <-result:
		if err == nil || !strings.Contains(err.Error(), "read limited") {
			t.Fatal("oversized carrier message accepted", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("oversized message was not refused")
	}
}

func TestRelayRefusesDialAndLocalAgentAtStreamCapacity(t *testing.T) {
	f := newFixture(t)
	f.hub.SetPolicy(f.o.Namespace, []string{"member:27017"})
	ctx, cancel := context.WithCancel(t.Context())
	r := &runtime{o: defaults(f.o)}
	config, err := clientTLS(r.o)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := connectionURL(f.o.URL)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- r.connect(ctx, endpoint, config) }()
	t.Cleanup(func() { cancel(); <-done })
	eventually(t, func() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.session != nil })
	r.mu.Lock()
	s := r.session
	s.streams.active.Store(MaxActiveStreams)
	r.mu.Unlock()
	if conn, err := f.hub.DialContext(ctx, f.o.Namespace, "tcp", "member:27017"); err == nil {
		conn.Close()
		t.Fatal("relay accepted excess dial stream")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan error, 1)
	go func() { accepted <- r.acceptLocal(ctx, listener) }()
	t.Cleanup(func() { cancel(); listener.Close(); <-accepted })
	local, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	local.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, err = local.Read(b[:]); !errors.Is(err, io.EOF) {
		t.Fatal("excess local agent was not refused", err)
	}
}
