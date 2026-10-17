// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package relay

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/version"
	"github.com/coder/websocket"
	"github.com/xtaci/smux"
)

// Options configures a relay with pinned inner TLS credentials. Credential bytes are secrets.
// HTTPClient controls outer HTTPS trust. Resolver and DialContext may override project networking;
// production callers should leave them nil to retain standard DNS and TCP behavior.
type Options struct {
	URL, Namespace, Hostname                    string
	CertPEM, KeyPEM, CAPEM                      []byte
	ListenAgent                                 string
	Version, Commit, ExecutableSHA256           string
	Logger                                      *slog.Logger
	HTTPClient                                  *http.Client
	Resolver                                    func(context.Context, string) ([]netip.Addr, error)
	DialContext                                 func(context.Context, string, string) (net.Conn, error)
	SmuxConfig                                  *smux.Config
	HandshakeTimeout, DialTimeout, PingInterval time.Duration
	BackoffMin, BackoffMax                      time.Duration
	// Backoff, when supplied, replaces full jitter for the given exponential upper bound.
	Backoff func(time.Duration) time.Duration
}

// OptionsFromEnv reads relay secret PEM values directly from KARAKORAM_RELAY_* environment variables.
// The executable digest is computed from the current executable, never from caller-supplied metadata.
func OptionsFromEnv() (Options, error) {
	o := Options{URL: os.Getenv("KARAKORAM_RELAY_URL"), Namespace: os.Getenv("KARAKORAM_RELAY_NAMESPACE"), CertPEM: []byte(os.Getenv("KARAKORAM_RELAY_CERT")), KeyPEM: []byte(os.Getenv("KARAKORAM_RELAY_KEY")), CAPEM: []byte(os.Getenv("KARAKORAM_RELAY_CA")), ListenAgent: os.Getenv("KARAKORAM_RELAY_LISTEN"), Hostname: os.Getenv("hostname"), Version: version.Version, Commit: version.Commit}
	if o.ListenAgent == "" {
		o.ListenAgent = ":9443"
	}
	if o.Hostname == "" {
		var err error
		o.Hostname, err = os.Hostname()
		if err != nil {
			return o, err
		}
	}
	if o.URL == "" || o.Namespace == "" || len(o.CertPEM) == 0 || len(o.KeyPEM) == 0 || len(o.CAPEM) == 0 {
		return o, errors.New("KARAKORAM_RELAY_URL, KARAKORAM_RELAY_NAMESPACE, KARAKORAM_RELAY_CERT, KARAKORAM_RELAY_KEY and KARAKORAM_RELAY_CA are required")
	}
	path, err := os.Executable()
	if err != nil {
		return o, err
	}
	f, err := os.Open(path)
	if err != nil {
		return o, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return o, err
	}
	o.ExecutableSHA256 = hex.EncodeToString(h.Sum(nil))
	return o, nil
}
func defaults(o Options) Options {
	if o.ListenAgent == "" {
		o.ListenAgent = ":9443"
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.HandshakeTimeout <= 0 {
		o.HandshakeTimeout = 10 * time.Second
	}
	if o.DialTimeout <= 0 {
		o.DialTimeout = 10 * time.Second
	}
	if o.PingInterval <= 0 {
		o.PingInterval = 20 * time.Second
	}
	if o.BackoffMin <= 0 {
		o.BackoffMin = time.Second
	}
	if o.BackoffMax <= 0 {
		o.BackoffMax = 30 * time.Second
	}
	if o.BackoffMax < o.BackoffMin {
		o.BackoffMax = o.BackoffMin
	}
	if o.Resolver == nil {
		o.Resolver = func(ctx context.Context, host string) ([]netip.Addr, error) {
			return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		}
	}
	if o.DialContext == nil {
		o.DialContext = (&net.Dialer{}).DialContext
	}
	o.SmuxConfig = muxConfig(o.SmuxConfig)
	return o
}
func connectionURL(origin string) (string, error) {
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return "", errors.New("relay URL must be an https origin")
	}
	u.Scheme = "wss"
	u.Path = "/relay/v1/connect"
	return u.String(), nil
}
func clientTLS(o Options) (*tls.Config, error) {
	pair, err := tls.X509KeyPair(o.CertPEM, o.KeyPEM)
	if err != nil {
		return nil, errors.New("invalid relay client credentials")
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, err
	}
	ns, err := NamespaceFromCert(leaf)
	if err != nil || ns != o.Namespace {
		return nil, errors.New("relay certificate namespace does not match configuration")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(o.CAPEM) {
		return nil, errors.New("invalid relay CA")
	}
	return &tls.Config{Certificates: []tls.Certificate{pair}, RootCAs: roots, ServerName: hubServerName, MinVersion: tls.VersionTLS13}, nil
}

type relaySession struct {
	mux        *smux.Session
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.RWMutex
	targets    map[string]struct{}
	generation uint64
	streams    streamLimit
}
type runtime struct {
	o       Options
	mu      sync.Mutex
	session *relaySession
	lived   time.Duration
}

// Run keeps the private agent listener alive while reconnecting the authenticated tunnel.
// Readiness does not depend on tunnel connectivity. Cancellation closes listener, streams and local sockets.
func Run(ctx context.Context, o Options) error {
	if o.Hostname == "" {
		var err error
		o.Hostname, err = os.Hostname()
		if err != nil {
			return err
		}
	}
	o = defaults(o)
	endpoint, err := connectionURL(o.URL)
	if err != nil {
		return err
	}
	tlsConfig, err := clientTLS(o)
	if err != nil {
		return err
	}
	if err = smux.VerifyConfig(o.SmuxConfig); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", o.ListenAgent)
	if err != nil {
		return err
	}
	defer listener.Close()
	stop := context.AfterFunc(ctx, func() { listener.Close() })
	defer stop()
	r := &runtime{o: o}
	acceptErr := make(chan error, 1)
	go func() { acceptErr <- r.acceptLocal(ctx, listener) }()
	bound := o.BackoffMin
	for ctx.Err() == nil {
		r.lived = 0
		err = r.connect(ctx, endpoint, tlsConfig)
		if ctx.Err() != nil {
			break
		}
		o.Logger.Warn("relay tunnel disconnected", "namespace", o.Namespace, "error", err)
		if r.lived > 60*time.Second {
			bound = o.BackoffMin
		}
		delay := jitter(bound)
		if o.Backoff != nil {
			delay = o.Backoff(bound)
		}
		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
		case err := <-acceptErr:
			timer.Stop()
			return err
		case <-timer.C:
		}
		if bound < o.BackoffMax {
			bound *= 2
			if bound > o.BackoffMax {
				bound = o.BackoffMax
			}
		}
	}
	listener.Close()
	<-acceptErr
	return nil
}
func jitter(bound time.Duration) time.Duration {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(bound)+1))
	if err != nil {
		return bound
	}
	return time.Duration(n.Int64())
}
func (r *runtime) connect(ctx context.Context, endpoint string, tlsConfig *tls.Config) error {
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	handshakeCtx, handshakeCancel := context.WithTimeout(sessionCtx, r.o.HandshakeTimeout)
	defer handshakeCancel()
	ws, response, err := websocket.Dial(handshakeCtx, endpoint, &websocket.DialOptions{HTTPClient: r.o.HTTPClient})
	if err != nil {
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		return errors.New("relay WebSocket connection failed")
	}
	defer ws.CloseNow()
	raw := websocketConn(sessionCtx, ws)
	conn := tls.Client(raw, tlsConfig)
	if err = conn.HandshakeContext(handshakeCtx); err != nil {
		return errors.New("relay inner TLS handshake failed")
	}
	mux, err := smux.Client(conn, r.o.SmuxConfig)
	if err != nil {
		return err
	}
	defer mux.Close()
	s := &relaySession{mux: mux, ctx: sessionCtx, cancel: cancel, targets: make(map[string]struct{})}
	control, err := mux.OpenStream()
	if err != nil {
		return err
	}
	hello := Hello{Type: "hello", Namespace: r.o.Namespace, Hostname: r.o.Hostname, Version: r.o.Version, Commit: r.o.Commit, ExecutableSHA256: r.o.ExecutableSHA256}
	err = boundedStreamOperation(handshakeCtx, control, r.o.HandshakeTimeout, func() error {
		if err := WriteControlHeader(control, hello); err != nil {
			return err
		}
		return s.readPolicy(control)
	})
	if err != nil {
		return err
	}
	handshakeCancel()
	started := time.Now()
	r.mu.Lock()
	r.session = s
	r.mu.Unlock()
	defer func() {
		r.lived = time.Since(started)
		r.mu.Lock()
		if r.session == s {
			r.session = nil
		}
		r.mu.Unlock()
	}()
	go pingLoop(sessionCtx, ws, r.o.PingInterval)
	go func() {
		for {
			if err := s.readPolicy(control); err != nil {
				cancel()
				mux.Close()
				return
			}
		}
	}()
	for {
		stream, err := mux.AcceptStream()
		if err != nil {
			return err
		}
		if !s.streams.acquire() {
			stream.Close()
			continue
		}
		go func() {
			defer s.streams.release()
			r.handleDial(s, stream)
		}()
	}
}
func (s *relaySession) readPolicy(control *smux.Stream) error {
	v, err := ReadControlHeader(control)
	if err != nil {
		return err
	}
	p, ok := v.(*Policy)
	if !ok {
		return errors.New("expected relay policy")
	}
	s.mu.Lock()
	if p.Generation >= s.generation {
		targets := make(map[string]struct{}, len(p.Targets))
		for _, target := range p.Targets {
			targets[target] = struct{}{}
		}
		s.targets = targets
		s.generation = p.Generation
	}
	s.mu.Unlock()
	return WriteHeader(control, Reply{OK: true})
}
func (r *runtime) acceptLocal(ctx context.Context, l net.Listener) error {
	for {
		local, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		r.mu.Lock()
		s := r.session
		r.mu.Unlock()
		if s == nil || s.mux.IsClosed() {
			local.Close()
			continue
		}
		if !s.streams.acquire() {
			local.Close()
			continue
		}
		go func() {
			defer s.streams.release()
			defer local.Close()
			stream, err := s.mux.OpenStream()
			if err != nil {
				return
			}
			err = boundedStreamOperation(s.ctx, stream, r.o.HandshakeTimeout, func() error { return WriteHeader(stream, Agent{Type: "agent"}) })
			if err != nil {
				stream.Close()
				return
			}
			pipe(s.ctx, local, stream)
		}()
	}
}

var hostnamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*[a-z0-9]$|^[a-z0-9]$`)
var instanceHostPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

// Fixed dial refusal reasons a relay reports to the hub.
const (
	refusedPolicy     = "target is not in namespace policy"
	refusedAddress    = "invalid target address"
	refusedPort       = "target port is not allowed"
	refusedHostname   = "target must be a bare hostname"
	refusedReserved   = "target hostname is reserved"
	refusedUnresolved = "target hostname could not be resolved"
	refusedProhibited = "target resolves to a prohibited address"
	refusedConnect    = "target TCP connection failed"
)

// RefusalReason returns reason when it is one of the fixed relay refusal
// reasons; any other relay-supplied text is replaced with a generic reason.
func RefusalReason(reason string) string {
	switch reason {
	case refusedPolicy, refusedAddress, refusedPort, refusedHostname, refusedReserved, refusedUnresolved, refusedProhibited, refusedConnect:
		return reason
	}
	return "target refused"
}

// relayPorts bounds every engine's namespace policy as defense in depth: the
// node agent (9443), MongoDB (27017), RabbitMQ AMQP/AMQPS and management
// (5672, 5671, 15672, 15671) and Elasticsearch, Kibana, Logstash, APM and
// Beats (9200, 5601, 9600, 8200, 5044, 8080). Exact host:port policy
// membership is still required. Cluster-internal ports such as Erlang
// distribution are never relayed.
var relayPorts = map[string]bool{"9443": true, "27017": true, "5672": true, "5671": true, "15672": true, "15671": true, "9200": true, "5601": true, "9600": true, "8200": true, "5044": true, "8080": true}

func (r *runtime) target(ctx context.Context, s *relaySession, addr, instance string) (net.Conn, error) {
	s.mu.RLock()
	_, allowed := s.targets[addr]
	s.mu.RUnlock()
	if !allowed {
		return nil, errors.New(refusedPolicy)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, errors.New(refusedAddress)
	}
	if !relayPorts[port] {
		return nil, errors.New(refusedPort)
	}
	if _, err = netip.ParseAddr(host); err == nil || strings.Contains(host, ".") || !hostnamePattern.MatchString(host) {
		return nil, errors.New(refusedHostname)
	}
	if host == "core" || strings.EqualFold(host, r.o.Hostname) {
		return nil, errors.New(refusedReserved)
	}
	var ips []netip.Addr
	if instance != "" {
		// The authenticated hub checks its namespace's live backend inventory.
		// Policy remains bound to the logical member, never an arbitrary IP.
		if port != "9443" || instance == host || instance == "core" || strings.EqualFold(instance, r.o.Hostname) {
			return nil, errors.New(refusedAddress)
		}
		if ip, parseErr := netip.ParseAddr(instance); parseErr == nil {
			ips = []netip.Addr{ip}
		} else {
			if len(instance) > 253 || !instanceHostPattern.MatchString(instance) {
				return nil, errors.New(refusedAddress)
			}
			host = instance
		}
	}
	if len(ips) == 0 {
		ips, err = r.o.Resolver(ctx, host)
		if err != nil || len(ips) == 0 {
			return nil, errors.New(refusedUnresolved)
		}
	}
	for _, ip := range ips {
		ip = ip.Unmap()
		if !ip.IsValid() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast() {
			return nil, errors.New(refusedProhibited)
		}
	}
	// Dial the addresses just checked, rather than resolving twice and permitting DNS rebinding.
	for _, ip := range ips {
		conn, err := r.o.DialContext(ctx, "tcp", net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
		if ctx.Err() != nil {
			break
		}
	}
	return nil, errors.New(refusedConnect)
}
func (r *runtime) handleDial(s *relaySession, stream *smux.Stream) {
	defer stream.Close()
	var dial *Dial
	err := boundedStreamOperation(s.ctx, stream, r.o.HandshakeTimeout, func() error {
		v, err := ReadHeader(stream)
		if err != nil {
			return err
		}
		var ok bool
		dial, ok = v.(*Dial)
		if !ok {
			return errors.New("expected dial stream")
		}
		return nil
	})
	if err != nil {
		return
	}
	dialCtx, cancel := context.WithTimeout(s.ctx, r.o.DialTimeout)
	target, err := r.target(dialCtx, s, dial.Addr, dial.Instance)
	cancel()
	if err != nil {
		r.o.Logger.Warn("relay target refused", "namespace", r.o.Namespace, "target", dial.Addr, "reason", err.Error())
		boundedStreamOperation(s.ctx, stream, r.o.HandshakeTimeout, func() error { return WriteHeader(stream, Reply{Error: err.Error()}) })
		return
	}
	defer target.Close()
	if err = boundedStreamOperation(s.ctx, stream, r.o.HandshakeTimeout, func() error { return WriteHeader(stream, Reply{OK: true}) }); err != nil {
		return
	}
	pipe(s.ctx, stream, target)
}

// Readiness succeeds when the configured private agent listener accepts a local TCP connection.
// It reads only KARAKORAM_RELAY_LISTEN when ListenAgent is empty and requires no credentials or hub.
func Readiness(ctx context.Context, o Options) error {
	addr := o.ListenAgent
	if addr == "" {
		addr = os.Getenv("KARAKORAM_RELAY_LISTEN")
	}
	if addr == "" {
		addr = ":9443"
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("relay listener address: %w", err)
	}
	if host == "" || host == "0.0.0.0" {
		host = "127.0.0.1"
	} else if host == "::" {
		host = "::1"
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(host, port))
	if err != nil {
		return err
	}
	return conn.Close()
}
