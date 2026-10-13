// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package relay

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/xtaci/smux"
)

// ErrNoSession means that the namespace has no authenticated, ready relay.
var ErrNoSession = errors.New("relay namespace is disconnected")

// ErrRefused reports a relay-side target refusal; Reason is safe to expose in events.
type ErrRefused struct{ Reason string }

func (e *ErrRefused) Error() string { return "relay target refused: " + e.Reason }

// HubOptions defines trust, enrollment and transport limits. Authorize is required:
// it must reject absent/default namespaces and unenrolled or revoked certificate serials.
type HubOptions struct {
	Certificate          tls.Certificate
	ClientCAs            *x509.CertPool
	Authorize            func(context.Context, string, *x509.Certificate) error
	Logger               *slog.Logger
	OnSession            func(string, SessionEvent)
	HandshakeTimeout     time.Duration
	MaxPendingHandshakes int
	SmuxConfig           *smux.Config
	PingInterval         time.Duration
	PolicyInterval       time.Duration
}

// SessionEvent reports connected, disconnected or refused transitions without credentials.
// Callbacks may run concurrently and must not block; Type is connected, disconnected or refused.
type SessionEvent struct {
	// CertificateSerial is enrollment-only metadata on connected events; never expose it in public events or logs.
	CertificateSerial string
	Type              string
	Reason            string
	Hello             Hello
	Target            string
}

// SessionStatus is a snapshot of the latest authenticated namespace session.
type SessionStatus struct {
	// CertificateSerial identifies the latest authenticated leaf for enrollment reconciliation only.
	// Never expose it in public status responses, events or logs.
	CertificateSerial    string
	Connected            bool
	ConnectedSince       time.Time
	LastSeen             time.Time
	Hello                Hello
	Reconnects           uint64
	LastDisconnectReason string
}
type hubSession struct {
	mux           *smux.Session
	control       *smux.Stream
	hello         Hello
	policyChanged chan struct{}
	cancel        context.CancelFunc
}
type namespaceState struct {
	session       *hubSession
	status        SessionStatus
	policy        Policy
	everConnected bool
}

// Hub routes namespace TCP streams and exposes inbound connections as a listener.
// The first relay-opened stream is reserved for hello and hub-pushed policy acknowledgments.
type Hub struct {
	o        HubOptions
	mu       sync.Mutex
	states   map[string]*namespaceState
	pending  chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	listener *inboundListener
	closed   bool
}

// NewHub creates a hub. Mount Handler on the public root /relay/v1/connect endpoint.
func NewHub(o HubOptions) *Hub {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.HandshakeTimeout <= 0 {
		o.HandshakeTimeout = 10 * time.Second
	}
	if o.MaxPendingHandshakes <= 0 {
		o.MaxPendingHandshakes = 32
	}
	if o.PingInterval <= 0 {
		o.PingInterval = 20 * time.Second
	}
	if o.PolicyInterval <= 0 {
		o.PolicyInterval = 30 * time.Second
	}
	o.SmuxConfig = muxConfig(o.SmuxConfig)
	ctx, cancel := context.WithCancel(context.Background())
	return &Hub{o: o, states: make(map[string]*namespaceState), pending: make(chan struct{}, o.MaxPendingHandshakes), ctx: ctx, cancel: cancel, listener: newInboundListener()}
}
func (h *Hub) state(ns string) *namespaceState {
	s := h.states[ns]
	if s == nil {
		s = &namespaceState{policy: Policy{Type: "policy", Targets: []string{}}}
		h.states[ns] = s
	}
	return s
}
func (h *Hub) event(ns string, e SessionEvent) {
	if h.o.OnSession != nil {
		h.o.OnSession(ns, e)
	}
}

// Handler accepts only GET upgrades at /relay/v1/connect. Authentication is inner TLS 1.3,
// never cookies or Origin; disabling the browser-origin check does not bypass mTLS.
func (h *Hub) Handler() http.Handler { return http.HandlerFunc(h.serve) }
func (h *Hub) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || r.URL.Path != "/relay/v1/connect" {
		http.NotFound(w, r)
		return
	}
	h.mu.Lock()
	closed := h.closed
	h.mu.Unlock()
	if closed {
		http.Error(w, "relay hub closed", http.StatusServiceUnavailable)
		return
	}
	select {
	case h.pending <- struct{}{}:
	default:
		http.Error(w, "relay handshake capacity reached", http.StatusServiceUnavailable)
		return
	}
	released := false
	release := func() {
		if !released {
			<-h.pending
			released = true
		}
	}
	defer release()
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(h.ctx, cancel)
	defer stop()
	// The WebSocket is only a carrier; credentials are verified inside the encrypted tunnel.
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer ws.CloseNow()
	raw := websocket.NetConn(ctx, ws, websocket.MessageBinary)
	conn := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{h.o.Certificate}, ClientCAs: h.o.ClientCAs, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})
	deadline := time.Now().Add(h.o.HandshakeTimeout)
	conn.SetDeadline(deadline)
	handshakeCtx, handshakeCancel := context.WithDeadline(ctx, deadline)
	defer handshakeCancel()
	if err = conn.HandshakeContext(handshakeCtx); err != nil {
		return
	}
	cert := conn.ConnectionState().PeerCertificates[0]
	ns, err := NamespaceFromCert(cert)
	if err != nil {
		return
	}
	if h.o.Authorize == nil {
		return
	}
	if err = h.o.Authorize(handshakeCtx, ns, cert); err != nil {
		return
	}
	conn.SetDeadline(time.Time{})
	mux, err := smux.Server(conn, h.o.SmuxConfig)
	if err != nil {
		return
	}
	defer mux.Close()
	mux.SetDeadline(deadline)
	control, err := mux.AcceptStream()
	if err != nil {
		return
	}
	mux.SetDeadline(time.Time{})
	control.SetDeadline(deadline)
	header, err := ReadControlHeader(control)
	if err != nil {
		return
	}
	hello, ok := header.(*Hello)
	if !ok || hello.Namespace != ns {
		return
	}
	hs := &hubSession{mux: mux, control: control, hello: *hello, policyChanged: make(chan struct{}, 1), cancel: cancel}
	h.mu.Lock()
	policy := h.state(ns).policy
	h.mu.Unlock()
	if err = pushPolicy(ctx, control, policy, h.o.HandshakeTimeout); err != nil {
		return
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	s := h.state(ns)
	old := s.session
	if s.everConnected {
		s.status.Reconnects++
	}
	s.everConnected = true
	s.session = hs
	s.status.Connected = true
	s.status.ConnectedSince = time.Now()
	s.status.LastSeen = time.Now()
	s.status.Hello = *hello
	s.status.CertificateSerial = cert.SerialNumber.String()
	// Catch a policy change between the initial snapshot and publication.
	if s.policy.Generation != policy.Generation {
		hs.policyChanged <- struct{}{}
	}
	h.mu.Unlock()
	release()
	if old != nil {
		old.cancel()
		old.mux.Close()
	}
	h.event(ns, SessionEvent{Type: "connected", Hello: *hello, CertificateSerial: cert.SerialNumber.String()})
	go pingLoop(ctx, ws, h.o.PingInterval)
	reason := make(chan string, 1)
	go func() { reason <- h.policyLoop(ctx, ns, hs) }()
	acceptDone := make(chan struct{})
	go func() { defer close(acceptDone); h.acceptAgents(ctx, ns, hs) }()
	why := "tunnel closed"
	select {
	case why = <-reason:
	case <-mux.CloseChan():
	case <-ctx.Done():
		why = "tunnel context canceled"
	}
	cancel()
	mux.Close()
	<-acceptDone
	h.mu.Lock()
	s = h.state(ns)
	current := s.session == hs
	if current {
		s.session = nil
		s.status.Connected = false
		s.status.LastDisconnectReason = why
	}
	h.mu.Unlock()
	if current {
		h.event(ns, SessionEvent{Type: "disconnected", Reason: why, Hello: *hello})
	}
}
func pushPolicy(ctx context.Context, s *smux.Stream, p Policy, timeout time.Duration) error {
	return boundedStreamOperation(ctx, s, timeout, func() error {
		if err := WriteControlHeader(s, p); err != nil {
			return err
		}
		reply, err := ReadReply(s)
		if err != nil {
			return err
		}
		if !reply.OK {
			return errors.New(reply.Error)
		}
		return nil
	})
}
func (h *Hub) policyLoop(ctx context.Context, ns string, s *hubSession) string {
	t := time.NewTicker(h.o.PolicyInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return "tunnel context canceled"
		case <-s.mux.CloseChan():
			return "tunnel closed"
		case <-t.C:
		case <-s.policyChanged:
		}
		h.mu.Lock()
		p := h.state(ns).policy
		h.mu.Unlock()
		if err := pushPolicy(ctx, s.control, p, h.o.HandshakeTimeout); err != nil {
			s.mux.Close()
			return "policy exchange failed"
		}
		h.mu.Lock()
		if h.state(ns).session == s {
			h.state(ns).status.LastSeen = time.Now()
		}
		h.mu.Unlock()
	}
}
func (h *Hub) acceptAgents(ctx context.Context, ns string, s *hubSession) {
	for {
		stream, err := s.mux.AcceptStream()
		if err != nil {
			return
		}
		go func() {
			err := boundedStreamOperation(ctx, stream, h.o.HandshakeTimeout, func() error {
				v, err := ReadHeader(stream)
				if err != nil {
					return err
				}
				if _, ok := v.(*Agent); !ok {
					return errors.New("expected agent stream")
				}
				return nil
			})
			if err != nil {
				stream.Close()
				return
			}
			h.mu.Lock()
			current := h.state(ns).session == s
			h.mu.Unlock()
			if !current {
				stream.Close()
				return
			}
			h.listener.deliver(ctx, &streamConn{Conn: stream, remote: relayAddr("relay:" + ns)})
		}()
	}
}

// DialContext opens a policy-checked TCP tunnel. RemoteAddr reports addr, not the WebSocket peer.
// Cancellation bounds stream opening and the dial reply; successful connections are caller-owned.
func (h *Hub) DialContext(ctx context.Context, namespace, network, addr string) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("unsupported relay network %q", network)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	h.mu.Lock()
	s := h.state(namespace).session
	h.mu.Unlock()
	if s == nil || s.mux.IsClosed() {
		return nil, ErrNoSession
	}
	// OpenStream may block on the smux send queue; cancellation closes any late-opened stream.
	type opened struct {
		s   *smux.Stream
		err error
	}
	ch := make(chan opened)
	go func() {
		stream, err := s.mux.OpenStream()
		select {
		case ch <- opened{stream, err}:
		case <-ctx.Done():
			if stream != nil {
				stream.Close()
			}
		case <-s.mux.CloseChan():
			if stream != nil {
				stream.Close()
			}
		}
	}()
	var stream *smux.Stream
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.mux.CloseChan():
		return nil, ErrNoSession
	case result := <-ch:
		if result.err != nil {
			return nil, result.err
		}
		stream = result.s
	}
	err := boundedStreamOperation(ctx, stream, h.o.HandshakeTimeout, func() error {
		if err := WriteHeader(stream, Dial{Type: "dial", Addr: addr}); err != nil {
			return err
		}
		reply, err := ReadReply(stream)
		if err != nil {
			return err
		}
		if !reply.OK {
			return &ErrRefused{Reason: reply.Error}
		}
		return nil
	})
	if err != nil {
		stream.Close()
		var refused *ErrRefused
		if errors.As(err, &refused) {
			h.event(namespace, SessionEvent{Type: "refused", Reason: refused.Reason, Target: addr, Hello: s.hello})
		}
		return nil, err
	}
	return &streamConn{Conn: stream, remote: relayAddr(addr)}, nil
}

// SetPolicy stores a complete allowlist and pushes it now, on reconnect, and every thirty seconds by default.
// Targets must fit a MaxControlSize frame and MaxPolicyTargets entries; malformed or oversized policies fail closed.
func (h *Hub) SetPolicy(namespace string, targets []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.state(namespace)
	s.policy = Policy{Type: "policy", Targets: append([]string{}, targets...), Generation: s.policy.Generation + 1}
	if s.session != nil {
		select {
		case s.session.policyChanged <- struct{}{}:
		default:
		}
	}
}

// Listener exposes relay-opened agent streams. Serve the existing internal TLS server on this listener.
func (h *Hub) Listener() net.Listener { return h.listener }

// Status returns a copy of the latest session state, including disconnect history.
func (h *Hub) Status(namespace string) SessionStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.state(namespace).status
}

// Close stops handshakes, closes all sessions and unblocks listener Accept. It is idempotent.
func (h *Hub) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	h.cancel()
	sessions := make(map[string]*hubSession)
	for ns, s := range h.states {
		if s.session != nil {
			sessions[ns] = s.session
			s.session = nil
			s.status.Connected = false
			s.status.LastDisconnectReason = "hub closed"
		}
	}
	h.mu.Unlock()
	for ns, s := range sessions {
		s.cancel()
		s.mux.Close()
		h.event(ns, SessionEvent{Type: "disconnected", Reason: "hub closed", Hello: s.hello})
	}
	return h.listener.Close()
}

type inboundListener struct {
	mu     sync.Mutex
	queue  chan net.Conn
	done   chan struct{}
	closed bool
}

func newInboundListener() *inboundListener {
	return &inboundListener{queue: make(chan net.Conn), done: make(chan struct{})}
}
func (l *inboundListener) deliver(ctx context.Context, c net.Conn) {
	select {
	case <-ctx.Done():
		c.Close()
	case <-l.done:
		c.Close()
	case l.queue <- c:
	}
}
func (l *inboundListener) Accept() (net.Conn, error) {
	select {
	case <-l.done:
		return nil, net.ErrClosed
	default:
	}
	select {
	case <-l.done:
		return nil, net.ErrClosed
	case c := <-l.queue:
		l.mu.Lock()
		closed := l.closed
		l.mu.Unlock()
		if closed {
			c.Close()
			return nil, net.ErrClosed
		}
		return c, nil
	}
}
func (l *inboundListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.closed {
		l.closed = true
		close(l.done)
		for {
			select {
			case c := <-l.queue:
				c.Close()
			default:
				return nil
			}
		}
	}
	return nil
}
func (l *inboundListener) Addr() net.Addr { return relayAddr("karakoram-relay-agents") }
