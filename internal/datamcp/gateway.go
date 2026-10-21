package datamcp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Limits bounds requests and results. Zero fields select defaults: 60 calls per
// minute, burst 10 per token, four concurrent calls per cluster, 1 MiB bodies,
// 256 KiB results and a 60-second timeout. MaxResultBytes must be at least 512
// bytes to accommodate the JSON envelope and untrusted-data boundary.
type Limits struct {
	CallsPerMinute, Burst, MaxConcurrentPerCluster int
	MaxBodyBytes                                   int64
	MaxResultBytes                                 int
	CallTimeout                                    time.Duration
}

// Options binds the gateway to authentication, policy, storage and upstreams.
// Authenticate, Authorize, Clusters, Upstreams and Audit are required. A nil
// Writes denies writes; a nil Enabled leaves cluster opt-in as the read switch.
// A nil Confirm uses the built-in signed multi-round-trip confirmation gate.
type Options struct {
	Authenticate Authenticator
	Authorize    Authorizer
	Writes       WriteSwitch
	Enabled      func() bool
	Clusters     Clusters
	Upstreams    UpstreamFactory
	Manifests    map[string]Manifest
	Audit        Auditor
	Confirm      Confirmer
	PublicURL    string
	Limits       Limits
	Logger       *slog.Logger
	Now          func() time.Time
}
type cachedUpstream struct {
	upstream    Upstream
	fingerprint string
	used        time.Time
	active      int
	closed      bool
}
type openingUpstream struct {
	done   chan struct{}
	cancel context.CancelFunc
	err    error
}
type bucket struct {
	tokens float64
	at     time.Time
}

// Gateway is the per-cluster MCP HTTP handler and upstream connection owner.
type Gateway struct {
	o             Options
	origin        string
	secret        [32]byte
	mu            sync.Mutex
	handlers      map[domain.ClusterRef]*mcp.StreamableHTTPHandler
	servers       map[domain.ClusterRef]*mcp.Server
	retired       map[*cachedUpstream]bool
	sessions      map[string]string
	upstreams     map[string]*cachedUpstream
	opening       map[string]*openingUpstream
	rates         map[string]bucket
	concurrent    map[domain.ClusterRef]int
	continuations map[string]time.Time
	closed        bool
	stop          chan struct{}
	done          chan struct{}
}

// New validates configuration and starts the idle upstream reaper.
func New(o Options) (*Gateway, error) {
	if o.Authenticate == nil || o.Authorize == nil || o.Clusters == nil || o.Upstreams == nil || o.Audit == nil {
		return nil, errors.New("data MCP requires authentication, authorization, clusters, upstreams and audit")
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Writes == nil {
		o.Writes = func() bool { return false }
	}
	if o.Enabled == nil {
		o.Enabled = func() bool { return true }
	}
	if o.Limits.MaxBodyBytes == 0 {
		o.Limits.MaxBodyBytes = 1 << 20
	}
	if o.Limits.MaxResultBytes == 0 {
		o.Limits.MaxResultBytes = 256 << 10
	}
	if o.Limits.CallTimeout == 0 {
		o.Limits.CallTimeout = time.Minute
	}
	if o.Limits.CallsPerMinute == 0 {
		o.Limits.CallsPerMinute = 60
	}
	if o.Limits.Burst == 0 {
		o.Limits.Burst = 10
	}
	if o.Limits.MaxConcurrentPerCluster == 0 {
		o.Limits.MaxConcurrentPerCluster = 4
	}
	if o.Limits.MaxBodyBytes < 1 || o.Limits.MaxResultBytes < 512 || o.Limits.CallTimeout < 0 || o.Limits.CallsPerMinute < 1 || o.Limits.Burst < 1 || o.Limits.MaxConcurrentPerCluster < 1 {
		return nil, errors.New("invalid data MCP limits")
	}
	origin := ""
	if o.PublicURL != "" {
		u, e := url.Parse(o.PublicURL)
		if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
			return nil, errors.New("invalid public URL")
		}
		origin = u.Scheme + "://" + u.Host
	}
	manifests := make(map[string]Manifest, len(o.Manifests))
	for n, m := range o.Manifests {
		if e := validateManifest(m); e != nil {
			return nil, e
		}
		b, e := json.Marshal(m)
		if e != nil {
			return nil, e
		}
		var copy Manifest
		if e = json.Unmarshal(b, &copy); e != nil {
			return nil, e
		}
		manifests[n] = copy
	}
	o.Manifests = manifests
	g := &Gateway{o: o, origin: origin, handlers: map[domain.ClusterRef]*mcp.StreamableHTTPHandler{}, sessions: map[string]string{}, upstreams: map[string]*cachedUpstream{}, rates: map[string]bucket{}, concurrent: map[domain.ClusterRef]int{}, continuations: map[string]time.Time{}, stop: make(chan struct{}), done: make(chan struct{})}
	g.servers = map[domain.ClusterRef]*mcp.Server{}
	g.retired = map[*cachedUpstream]bool{}
	if _, e := rand.Read(g.secret[:]); e != nil {
		return nil, e
	}
	go g.reap()
	return g, nil
}
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := strings.Split(r.URL.Path, "/")
	if len(p) != 5 || p[1] != "mcp" || p[2] != "clusters" {
		http.NotFound(w, r)
		return
	}
	ref := domain.ClusterRef{Namespace: p[3], Name: p[4]}
	if domain.ValidateNamespaceName(ref.Namespace) != nil || domain.ValidateClusterName(ref.Name) != nil || !g.o.Enabled() {
		http.NotFound(w, r)
		return
	}
	if r.Header.Get("Cookie") != "" {
		http.Error(w, "token authentication required", http.StatusUnauthorized)
		return
	}
	caller, e := g.o.Authenticate(r)
	if e != nil || caller.TokenID == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if origin := r.Header.Get("Origin"); origin != "" && (g.origin == "" || origin != g.origin) {
		http.Error(w, "forbidden origin", http.StatusForbidden)
		return
	}
	if !readPermission(caller, ref, g.o.Authorize) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	c, e := g.o.Clusters.GetCluster(r.Context(), ref)
	if e != nil || c == nil || c.Spec.DataMcp == nil || !c.Spec.DataMcp.Enabled {
		http.NotFound(w, r)
		return
	}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		http.Error(w, "gateway closed", http.StatusServiceUnavailable)
		return
	}
	id := r.Header.Get("Mcp-Session-Id")
	if id != "" && g.sessions[ref.Namespace+"/"+ref.Name+":"+id] != caller.TokenID {
		g.mu.Unlock()
		http.Error(w, "session belongs to another token", http.StatusForbidden)
		return
	}
	h := g.handlers[ref]
	if h == nil {
		s := g.server(ref)
		g.servers[ref] = s
		h = mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return s }, &mcp.StreamableHTTPOptions{MaxRequestBodyBytes: g.o.Limits.MaxBodyBytes, SessionTimeout: 10 * time.Minute, PropagateRequestCancellation: true})
		g.handlers[ref] = h
	}
	g.mu.Unlock()
	b, _ := json.Marshal(caller)
	r = r.Clone(r.Context())
	r.Header = r.Header.Clone()
	r.Header.Set("X-Karakoram-Data-Caller", string(b))
	r.Body = http.MaxBytesReader(w, r.Body, g.o.Limits.MaxBodyBytes)
	h.ServeHTTP(&sessionWriter{ResponseWriter: w, g: g, key: ref.Namespace + "/" + ref.Name + ":", token: caller.TokenID}, r)
	if r.Method == http.MethodDelete {
		g.mu.Lock()
		g.pruneSessionsLocked()
		g.mu.Unlock()
	}
}

type sessionWriter struct {
	http.ResponseWriter
	g          *Gateway
	key, token string
	wrote      bool
}

func (w *sessionWriter) WriteHeader(code int) {
	if w.wrote {
		return
	}
	w.wrote = true
	if id := w.Header().Get("Mcp-Session-Id"); id != "" {
		w.g.mu.Lock()
		w.g.sessions[w.key+id] = w.token
		w.g.mu.Unlock()
	}
	w.ResponseWriter.WriteHeader(code)
}
func (w *sessionWriter) Write(b []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	return w.ResponseWriter.Write(b)
}
func (w *sessionWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (g *Gateway) server(ref domain.ClusterRef) *mcp.Server {
	s := mcp.NewServer(&mcp.Implementation{Name: "karakoram-data", Version: "1"}, &mcp.ServerOptions{Capabilities: &mcp.ServerCapabilities{Tools: &mcp.ToolCapabilities{}}})
	registered := map[string]bool{}
	for _, m := range g.o.Manifests {
		for n, r := range m.Tools {
			names := []string{n}
			if r.WriteAlias != "" {
				names = append(names, r.WriteAlias)
			}
			for _, name := range names {
				if registered[name] {
					continue
				}
				registered[name] = true
				s.AddTool(&mcp.Tool{Name: name, InputSchema: map[string]any{"type": "object"}}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
					return g.call(ctx, ref, req)
				})
			}
		}
	}
	s.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			if method == "tools/list" {
				return g.list(ctx, ref, req)
			}
			if method == "tools/call" {
				// Include legacy SDK-managed elicitation round trips in the timeout.
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, g.o.Limits.CallTimeout)
				defer cancel()
				p := req.GetParams().(*mcp.CallToolParamsRaw)
				if !registered[p.Name] {
					return g.call(ctx, ref, req.(*mcp.CallToolRequest))
				}
				progress := &callAuditProgress{started: g.o.Now()}
				ctx = context.WithValue(ctx, callAuditKey{}, progress)
				result, err := next(ctx, method, req)
				if err != nil {
					err = g.auditLegacyFailure(ctx, progress, err)
				}
				return result, err
			}
			return next(ctx, method, req)
		}
	})
	return s
}
func callerOf(req mcp.Request) Caller {
	var c Caller
	if req.GetExtra() != nil {
		_ = json.Unmarshal([]byte(req.GetExtra().Header.Get("X-Karakoram-Data-Caller")), &c)
	}
	return c
}
func (g *Gateway) access(ctx context.Context, ref domain.ClusterRef, caller Caller) (*domain.Cluster, AccessDecision, Manifest, error) {
	if !g.o.Enabled() || !readPermission(caller, ref, g.o.Authorize) {
		return nil, AccessDecision{}, Manifest{}, errors.New("data access denied")
	}
	c, e := g.o.Clusters.GetCluster(ctx, ref)
	if e != nil {
		return nil, AccessDecision{}, Manifest{}, errors.New("cluster unavailable")
	}
	a := DecideAccess(caller, ref, c, g.o.Authorize, g.o.Writes())
	if !a.Read {
		return c, a, Manifest{}, errors.New("data access disabled")
	}
	n, e := g.o.Upstreams.Manifest(ctx, c)
	if e != nil {
		return c, a, Manifest{}, errors.New("manifest unavailable")
	}
	m, ok := g.o.Manifests[n]
	if !ok {
		return c, a, m, errors.New("unknown manifest")
	}
	return c, a, m, nil
}
func (g *Gateway) upstream(ctx context.Context, ref domain.ClusterRef, c *domain.Cluster, l engine.DataAccessLevel) (Upstream, func(), error) {
	key := ref.Namespace + "/" + ref.Name + ":" + string(l)
	for {
		fp, err := g.o.Upstreams.Fingerprint(ctx, c, l)
		if err != nil {
			return nil, nil, errors.New("upstream unavailable")
		}
		g.mu.Lock()
		if g.closed {
			g.mu.Unlock()
			return nil, nil, errors.New("gateway closed")
		}
		entry := g.upstreams[key]
		if entry != nil && entry.fingerprint == fp {
			entry.active++
			entry.used = g.o.Now()
			g.mu.Unlock()
			return entry.upstream, func() { g.releaseUpstream(key, entry) }, nil
		}
		if pending := g.opening[key]; pending != nil {
			g.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-pending.done:
				if pending.err != nil {
					return nil, nil, pending.err
				}
				continue
			}
		}
		openCtx, cancel := context.WithCancel(ctx)
		pending := &openingUpstream{done: make(chan struct{}), cancel: cancel}
		if g.opening == nil {
			g.opening = map[string]*openingUpstream{}
		}
		g.opening[key] = pending
		g.mu.Unlock()
		u, openErr := g.o.Upstreams.Open(openCtx, c, l)
		var current string
		if openErr == nil {
			current, openErr = g.o.Upstreams.Fingerprint(openCtx, c, l)
		}
		cancel()
		var stale Upstream
		g.mu.Lock()
		if g.closed {
			openErr = errors.New("gateway closed")
		}
		if openErr == nil && current != fp {
			openErr = errors.New("upstream changed while opening")
		}
		if openErr == nil && u == nil {
			openErr = errors.New("upstream unavailable")
		}
		if openErr == nil {
			if old := g.upstreams[key]; old != nil {
				if old.active == 0 {
					old.closed = true
					stale = old.upstream
				} else {
					g.retired[old] = true
				}
			}
			entry = &cachedUpstream{upstream: u, fingerprint: fp, active: 1, used: g.o.Now()}
			g.upstreams[key] = entry
		} else {
			pending.err = errors.New("upstream unavailable")
		}
		delete(g.opening, key)
		close(pending.done)
		g.mu.Unlock()
		if stale != nil {
			_ = stale.Close()
		}
		if openErr != nil {
			if u != nil {
				_ = u.Close()
			}
			return nil, nil, pending.err
		}
		return entry.upstream, func() { g.releaseUpstream(key, entry) }, nil
	}
}

func (g *Gateway) releaseUpstream(key string, entry *cachedUpstream) {
	g.mu.Lock()
	entry.active--
	entry.used = g.o.Now()
	closeEntry := g.upstreams[key] != entry && entry.active == 0 && !entry.closed
	if closeEntry {
		entry.closed = true
		delete(g.retired, entry)
	}
	g.mu.Unlock()
	if closeEntry {
		_ = entry.upstream.Close()
	}
}

// pruneSessionsLocked removes token bindings once their SDK session has closed,
// including idle expiry. The caller holds g.mu.
func (g *Gateway) pruneSessionsLocked() {
	active := make(map[string]bool, len(g.sessions))
	for ref, server := range g.servers {
		for session := range server.Sessions() {
			if id := session.ID(); id != "" {
				active[ref.Namespace+"/"+ref.Name+":"+id] = true
			}
		}
	}
	for key := range g.sessions {
		if !active[key] {
			delete(g.sessions, key)
		}
	}
}
func (g *Gateway) reap() {
	defer close(g.done)
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-g.stop:
			return
		case <-t.C:
			g.mu.Lock()
			now := g.o.Now()
			g.pruneSessionsLocked()
			var idle []Upstream
			for k, u := range g.upstreams {
				if u.active == 0 && now.Sub(u.used) > 10*time.Minute {
					u.closed = true
					idle = append(idle, u.upstream)
					delete(g.upstreams, k)
				}
			}
			for k, b := range g.rates {
				if now.Sub(b.at) > 10*time.Minute {
					delete(g.rates, k)
				}
			}
			for k, expiry := range g.continuations {
				if !now.Before(expiry) {
					delete(g.continuations, k)
				}
			}
			g.mu.Unlock()
			for _, u := range idle {
				_ = u.Close()
			}
		}
	}
}
func (g *Gateway) Close() error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil
	}
	g.closed = true
	close(g.stop)
	for _, pending := range g.opening {
		pending.cancel()
	}
	g.sessions = map[string]string{}
	var upstreams []Upstream
	for _, u := range g.upstreams {
		u.closed = true
		upstreams = append(upstreams, u.upstream)
	}
	for u := range g.retired {
		u.closed = true
		upstreams = append(upstreams, u.upstream)
	}
	var sessions []*mcp.ServerSession
	for _, s := range g.servers {
		for session := range s.Sessions() {
			sessions = append(sessions, session)
		}
	}
	g.upstreams = map[string]*cachedUpstream{}
	g.retired = map[*cachedUpstream]bool{}
	g.mu.Unlock()
	var es []error
	for _, s := range sessions {
		es = append(es, s.Close())
	}
	for _, u := range upstreams {
		es = append(es, u.Close())
	}
	<-g.done
	return errors.Join(es...)
}
func nonce() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(fmt.Sprintf("randomness unavailable: %v", e))
	}
	return hex.EncodeToString(b[:])
}
