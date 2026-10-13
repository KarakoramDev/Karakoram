// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package server exposes the public API and private, CA-pinned agent configuration endpoint.
// Public traffic is served on :8080 behind Zerops HTTPS routing; private agent traffic
// uses :9443. The SQLite database and sealed internal PKI survive runtime deployments
// in KARAKORAM_DATA_DIR. Back up this directory only after a clean server shutdown,
// and retain KARAKORAM_MASTER_KEY separately to recover its encrypted secrets.
package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/events"
	"github.com/KarakoramDev/Karakoram/internal/monitoring"
	"github.com/KarakoramDev/Karakoram/internal/rbac"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/KarakoramDev/Karakoram/internal/ui"
)

// Options supplies the real control-plane dependencies; providers remain independently testable.
type Options struct {
	Store        *store.Store
	Auth         *auth.Service
	RBAC         *rbac.Enforcer
	Registry     *engine.Registry
	Context      engine.Context
	Trigger      interface{ Trigger(domain.ClusterRef) }
	Hub          *events.Hub
	Monitoring   *monitoring.Collector
	PublicURL    string
	Logger       *slog.Logger
	Namespaces   engine.Namespaces
	RelayHandler http.Handler
}

// Server implements all generated strict operations.
type Server struct {
	Options
	namespaces engine.Namespaces
}

// New assembles HTTP handlers over initialized dependencies.
func New(o Options) (*Server, error) {
	if o.Store == nil || o.Auth == nil || o.RBAC == nil || o.Registry == nil || o.Hub == nil {
		return nil, errors.New("store, auth, RBAC, registry and events hub are required")
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Namespaces == nil {
		o.Namespaces = engine.StaticNamespaces{DefaultBackend: o.Context.Backend, DefaultAgentURL: o.Context.AgentURL}
	}
	return &Server{Options: o, namespaces: o.Namespaces}, nil
}

type requestKey struct{}
type identityKey struct{}
type input struct {
	Body                                                                                                           any
	Cluster, Account, Token, Storage, Backup, Database, Role, Username, Member, Restore, User, Operation, Provider string
}
type response struct {
	status  int
	body    any
	serve   func(http.ResponseWriter) error
	noStore bool
}

func (r response) write(w http.ResponseWriter) error {
	if r.noStore {
		w.Header().Set("Cache-Control", "no-store")
	}
	if r.serve != nil {
		return r.serve(w)
	}
	if r.status == 0 {
		r.status = 200
	}
	if r.status == 204 || r.body == nil {
		w.WriteHeader(r.status)
		return nil
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(r.status)
	return json.NewEncoder(w).Encode(r.body)
}
func ok(body any) response { return response{status: 200, body: body} }
func fail(status int, code, message string) response {
	return response{status: status, body: api.Error{Code: code, Message: message}}
}
func (s *Server) failure(err error) response {
	switch {
	case errors.Is(err, store.ErrNotFound), errors.Is(err, engine.ErrNotFound):
		return fail(404, "not_found", "Resource not found")
	case errors.Is(err, store.ErrConflict), errors.Is(err, engine.ErrConflict), errors.Is(err, engine.ErrNotSupported):
		return fail(409, "conflict", "Request conflicts with the resource state")
	case errors.Is(err, engine.ErrInvalid), errors.Is(err, auth.ErrInvalid):
		var v *engine.ValidationError
		if errors.As(err, &v) {
			return fail(400, "invalid_request", v.Message)
		}
		return fail(400, "invalid_request", "Invalid request")
	case errors.Is(err, auth.ErrCSRF):
		return fail(403, "forbidden", "CSRF validation failed")
	case errors.Is(err, auth.ErrUnauthenticated):
		return fail(401, "unauthorized", "Authentication required")
	case errors.Is(err, auth.ErrPasswordChangeRequired):
		return fail(403, "password_change_required", "Change your password before continuing")
	default:
		s.Logger.Error("API operation failed")
		return fail(500, "internal_error", "Internal server error")
	}
}
func principal(ctx context.Context) *auth.Principal {
	p, _ := ctx.Value(identityKey{}).(*auth.Principal)
	return p
}
func request(ctx context.Context) *http.Request {
	r, _ := ctx.Value(requestKey{}).(*http.Request)
	if r == nil {
		return &http.Request{URL: nil}
	}
	return r
}
func (s *Server) allowed(ctx context.Context, res, act, obj string) bool {
	p := principal(ctx)
	return p != nil && s.RBAC.Can(p.Subjects, res, act, obj)
}
func operationID(name string) string { return strings.ToLower(name[:1]) + name[1:] }
func selfService(id string) bool {
	switch id {
	case "getCurrentUser", "logout", "refreshSession", "changePassword", "getPermissions", "checkPermission", "createAPIToken", "listAPITokens", "getAPIToken", "replaceAPIToken", "deleteAPIToken":
		return true
	}
	return false
}
func collection(id string) bool {
	switch id {
	case "listClusters", "listOrphanedVolumes", "listOperations", "listEvents", "streamEvents", "getPrometheusMetrics", "getOperation", "listBackupStorages", "listNamespaces":
		return true
	}
	return false
}
func (s *Server) middleware(next api.StrictHandlerFunc, name string) api.StrictHandlerFunc {
	return func(ctx context.Context, w http.ResponseWriter, r *http.Request, v any) (any, error) {
		id := operationID(name)
		rule, exists := api.OperationRBAC[id]
		if !exists {
			return fail(403, "forbidden", "Unknown API operation"), nil
		}
		ctx = context.WithValue(ctx, requestKey{}, r)
		if id == "login" || id == "refreshSession" || id == "logout" || id == "oidcLogin" {
			if !s.sameOrigin(r) {
				return fail(403, "forbidden", "Cross-origin authentication request rejected"), nil
			}
			if id == "login" {
				kind, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
				if err != nil || kind != "application/json" {
					return fail(415, "invalid_request", "Login requires application/json"), nil
				}
			}
		}
		if !rule.Public {
			// Cookie logout and refresh authenticate their refresh family and CSRF
			// themselves; bearer logout still requires a valid token.
			if id != "refreshSession" && !(id == "logout" && r.Header.Get("Authorization") == "") {
				p, err := s.Auth.Authenticate(r)
				if err != nil {
					if errors.Is(err, auth.ErrCSRF) {
						return fail(403, "forbidden", "CSRF validation failed"), nil
					}
					return fail(401, "unauthorized", "Authentication required"), nil
				}
				ctx = context.WithValue(ctx, identityKey{}, p)
				if p.MustChangePassword && id != "changePassword" && id != "getCurrentUser" && id != "logout" {
					return fail(403, "password_change_required", "Change your password before continuing"), nil
				}
				namespace := r.URL.Query().Get("namespace")
				if namespace == "" {
					namespace = "default"
				}
				obj := "*"
				switch rule.ObjectParam {
				case "cluster":
					obj = namespace + "/" + r.PathValue("cluster")
				case "namespace":
					obj = r.PathValue("namespace")
				case "storage":
					obj = namespace + "/" + r.PathValue("storage")
				}
				if id == "createCluster" {
					if q, yes := v.(api.CreateClusterRequestObject); yes && q.Body != nil {
						obj = namespace + "/" + q.Body.Name
					}
				}
				if id == "createBackupStorage" {
					if q, yes := v.(api.CreateBackupStorageRequestObject); yes && q.Body != nil {
						obj = namespace + "/" + q.Body.Name
					}
				}
				if !selfService(id) && !collection(id) && !s.RBAC.Can(p.Subjects, rule.Resource, rule.Action, obj) {
					return fail(403, "forbidden", "Permission denied"), nil
				}
			}
		}
		return next(ctx, w, r.WithContext(ctx), v)
	}
}

// sameOrigin permits non-browser clients, but rejects browser authentication
// requests from another origin. OIDC callbacks instead validate state and PKCE.
func (s *Server) sameOrigin(r *http.Request) bool {
	if site := r.Header.Get("Sec-Fetch-Site"); site != "" && site != "same-origin" && site != "none" {
		return false
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	expected, err := url.Parse(s.PublicURL)
	if err != nil || expected.Host == "" {
		return false
	}
	got, err := url.Parse(origin)
	return err == nil && got.User == nil && got.RawQuery == "" && got.Fragment == "" && got.Path == "" && strings.EqualFold(got.Scheme, expected.Scheme) && strings.EqualFold(got.Host, expected.Host)
}

// operationMux accounts for operation-level OpenAPI servers, which the generated
// stdlib router otherwise applies only at the document's /api/v1 base URL.
type operationMux struct{ *http.ServeMux }

func (m operationMux) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	switch pattern {
	case "GET /api/v1/healthz":
		pattern = "GET /healthz"
	case "GET /api/v1/metrics":
		pattern = "GET /metrics"
	}
	m.ServeMux.HandleFunc(pattern, handler)
}

// Handler returns the public router, including root health/metrics and SPA history fallback.
func (s *Server) Handler() http.Handler {
	strict := api.NewStrictHandlerWithOptions(s, []api.StrictMiddlewareFunc{s.middleware}, api.StrictHTTPServerOptions{
		RequestErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			status := 400
			var large *http.MaxBytesError
			if errors.As(err, &large) {
				status = 413
			}
			_ = fail(status, "invalid_request", "Invalid request body").write(w)
		},
		ResponseErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			// Streaming headers have already been sent; never append JSON to SSE
			// or Prometheus text after a transport or persistence failure.
			kind := w.Header().Get("Content-Type")
			if kind == "text/event-stream" || strings.HasPrefix(kind, "text/plain; version=") {
				if !errors.Is(err, context.Canceled) {
					s.Logger.Error("HTTP stream terminated")
				}
				return
			}
			_ = s.failure(err).write(w)
		},
	})
	mux := http.NewServeMux()
	// Relay authentication is performed by its inner mutual-TLS tunnel, not API RBAC.
	if s.RelayHandler != nil {
		mux.Handle("GET /relay/v1/connect", s.RelayHandler)
	}
	api.HandlerWithOptions(strict, api.StdHTTPServerOptions{BaseURL: "/api/v1", BaseRouter: operationMux{mux}, ErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
		_ = fail(400, "invalid_request", "Invalid request parameters").write(w)
	}})
	mux.HandleFunc("/api/v1/", func(w http.ResponseWriter, r *http.Request) {
		_ = fail(404, "not_found", "API route not found").write(w)
	})
	mux.Handle("/", ui.Handler())
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; font-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
		if strings.HasPrefix(s.PublicURL, "https://") {
			w.Header().Set("Strict-Transport-Security", "max-age=31536000")
		}
		if r.ContentLength > 2<<20 {
			_ = fail(413, "invalid_request", "Request body exceeds the size limit").write(w)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
		mux.ServeHTTP(w, r)
		// The route pattern has no path values, query strings, cookies or credentials.
		s.Logger.Info("HTTP request", "method", r.Method, "route", r.Pattern, "duration", time.Since(started))
	})
}

type page[T any] struct {
	Items      []T     `json:"items"`
	NextCursor *string `json:"nextCursor,omitempty"`
}

func paginate[T any](ctx context.Context, items []T) (page[T], error) {
	q := request(ctx).URL.Query()
	limit := 100
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 500 {
			return page[T]{}, engine.ErrInvalid
		}
		limit = n
	}
	offset := 0
	if v := q.Get("cursor"); v != "" {
		b, err := base64.RawURLEncoding.DecodeString(v)
		if err != nil {
			return page[T]{}, engine.ErrInvalid
		}
		offset, err = strconv.Atoi(string(b))
		if err != nil || offset < 0 {
			return page[T]{}, engine.ErrInvalid
		}
	}
	if offset > len(items) {
		offset = len(items)
	}
	end := min(offset+limit, len(items))
	selected := items[offset:end]
	if selected == nil {
		selected = []T{}
	}
	p := page[T]{Items: selected}
	if end < len(items) {
		v := base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(end)))
		p.NextCursor = &v
	}
	return p, nil
}
func listResponse[T any](s *Server, ctx context.Context, items []T, err error) response {
	if err != nil {
		return s.failure(err)
	}
	p, err := paginate(ctx, items)
	if err != nil {
		return s.failure(err)
	}
	return ok(p)
}
func (s *Server) context(ctx context.Context, ref domain.ClusterRef) (*engine.Context, engine.Provider, error) {
	c, err := s.Store.GetCluster(ctx, ref)
	if err != nil {
		return nil, nil, err
	}
	p, err := s.Registry.Get(c.Spec.Provider)
	if err != nil {
		return nil, nil, err
	}
	rc := s.Context
	rc.Cluster = c
	rc.Namespace = domain.NormalizedNamespace(c.Namespace)
	rc.Backend, err = s.namespaces.Backend(rc.Namespace)
	if err != nil {
		return nil, nil, err
	}
	rc.Dialer = s.namespaces.Dialer(rc.Namespace)
	rc.AgentURL = s.namespaces.AgentURL(rc.Namespace)
	return &rc, p, nil
}
func (s *Server) trigger(ref domain.ClusterRef) {
	if s.Trigger != nil {
		s.Trigger.Trigger(ref)
	}
}
func invalid() response { return fail(400, "invalid_request", "Invalid request") }
func ptr[T any](v T) *T { return &v }
func value[T any](p *T) T {
	if p != nil {
		return *p
	}
	var zero T
	return zero
}
func namespaceFilter(ctx context.Context) string {
	if r := request(ctx); r.URL != nil {
		return r.URL.Query().Get("namespace")
	}
	return ""
}
func clusterRef(ctx context.Context, name string) domain.ClusterRef {
	return domain.ClusterRef{Namespace: domain.NormalizedNamespace(namespaceFilter(ctx)), Name: name}
}
func clusterObject(namespace, name string) string {
	return domain.NormalizedNamespace(namespace) + "/" + name
}
func (s *Server) usableNamespace(namespace string) *response {
	if _, err := s.namespaces.Backend(namespace); err != nil {
		r := fail(422, "invalid_namespace", "Namespace "+namespace+" does not exist or is unavailable")
		return &r
	}
	return nil
}
func (s *Server) handle(ctx context.Context, name string, in input) response {
	switch name {
	case "GetHealth":
		_, err := s.Store.ListClusters(ctx, "")
		if err != nil {
			return s.failure(err)
		}
		return ok(api.Health{Status: "ok", Database: true})
	case "Login", "Logout", "RefreshSession", "ChangePassword", "GetCurrentUser", "OidcLogin", "OidcCallback", "ListAccounts", "CreateAccount", "GetAccount", "ReplaceAccount", "DeleteAccount", "ResetAccountPassword", "ListAPITokens", "CreateAPIToken", "GetAPIToken", "ReplaceAPIToken", "DeleteAPIToken", "GetPermissions", "CheckPermission", "GetRBACPolicy", "ReplaceRBACPolicy", "GetSettings", "ReplaceSettings", "GetVersion":
		return s.identity(ctx, name, in)
	case "ListBackupStorages", "CreateBackupStorage", "GetBackupStorage", "ReplaceBackupStorage", "DeleteBackupStorage", "VerifyBackupStorage":
		return s.storage(ctx, name, in)
	case "ListOrphanedVolumes":
		return s.orphanedVolumes(ctx)
	case "ListEvents", "StreamEvents", "ListOperations", "GetOperation", "GetPrometheusMetrics", "GetClusterMetrics":
		return s.observability(ctx, name, in)
	default:
		return s.cluster(ctx, name, in)
	}
}
