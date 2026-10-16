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
	"fmt"
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
	"github.com/KarakoramDev/Karakoram/internal/infra"
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
	Backends     *infra.Registry
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
	if o.Backends == nil {
		var err error
		o.Backends, err = backendRegistry(Config{})
		if err != nil {
			return nil, err
		}
	}
	if o.Namespaces == nil {
		o.Namespaces = engine.StaticNamespaces{DefaultBackend: o.Context.Backend, DefaultAgentURL: o.Context.AgentURL}
	}
	return &Server{Options: o, namespaces: o.Namespaces}, nil
}

type requestKey struct{}
type identityKey struct{}
type triggerKey struct{}
type input struct {
	Body                                                                                                                      any
	Namespace, Cluster, Account, Token, Storage, Backup, Database, Role, Username, Member, Restore, User, Operation, Provider string
	Vhost, Name                                                                                                               string
}
type response struct {
	status      int
	body        any
	serve       func(http.ResponseWriter) error
	noStore     bool
	operationID string
	auditEvent  *domain.Event
}

func (r response) write(w http.ResponseWriter) error {
	if r.operationID != "" {
		w.Header().Set("Karakoram-Operation-Id", r.operationID)
	}
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

// failDetails is fail with a structured, secret-free details object.
func failDetails(status int, code, message string, details map[string]any) response {
	e := api.Error{Code: code, Message: message}
	if len(details) > 0 {
		e.Details = &details
	}
	return response{status: status, body: e}
}

// invalidField rejects one request field or parameter with 400 invalid_request.
func invalidField(field, reason, message string) response {
	return failDetails(400, "invalid_request", message, map[string]any{"field": field, "reason": reason})
}

func (s *Server) failure(err error) response {
	var namespaceConflict *store.NamespaceConflictError
	if errors.As(err, &namespaceConflict) {
		return fail(409, "conflict", namespaceConflict.Error())
	}
	var invalidRef *domain.ClusterRefValidationError
	if errors.As(err, &invalidRef) {
		return fail(400, "invalid_request", invalidRef.Error())
	}
	var fieldErr *domain.FieldError
	if errors.As(err, &fieldErr) {
		return invalidField(fieldErr.Field, fieldErr.Reason, fieldErr.Message)
	}
	var resErr *domain.ResourceError
	if errors.As(err, &resErr) {
		details := map[string]any{"resource": resErr.Resource, "name": resErr.Name, "reason": resErr.Reason}
		if resErr.State != "" {
			details["state"] = resErr.State
		}
		if resErr.Reason == domain.ReasonNotFound {
			return failDetails(404, "not_found", resErr.Message, details)
		}
		return failDetails(409, "conflict", resErr.Message, details)
	}
	switch {
	case errors.Is(err, engine.ErrUnavailable):
		return fail(503, "unavailable", "Database agent or relay is temporarily unavailable; retry the request after connectivity recovers")
	case errors.Is(err, store.ErrNotFound), errors.Is(err, engine.ErrNotFound):
		return fail(404, "not_found", "Resource not found")
	case errors.Is(err, store.ErrConflict), errors.Is(err, engine.ErrConflict), errors.Is(err, engine.ErrNotSupported):
		return fail(409, "conflict", "Request conflicts with the resource state")
	case errors.Is(err, engine.ErrInvalid), errors.Is(err, auth.ErrInvalid), errors.Is(err, store.ErrInvalid):
		var v *engine.ValidationError
		if errors.As(err, &v) {
			if v.Field != "" {
				reason := v.Reason
				if reason == "" {
					reason = domain.ReasonInvalid
				}
				return invalidField(v.Field, reason, v.Message)
			}
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

// namespaceAllowed checks grants without looking up namespace registration.
// Namespace metadata and object-scoped grants both count; matching denies still win.
func (s *Server) namespaceAllowed(ctx context.Context, namespace string) bool {
	p := principal(ctx)
	if p == nil {
		return false
	}
	for _, rule := range s.RBAC.Permissions(p.Subjects) {
		v := rule.Values
		if len(v) < 5 || v[4] != "allow" {
			continue
		}
		object := namespace
		if v[1] != "namespaces" {
			_, name, scoped := strings.Cut(v[3], "/")
			if !scoped {
				name = "**"
			}
			object += "/" + name
		}
		if s.RBAC.Can(p.Subjects, v[1], v[2], object) {
			return true
		}
	}
	return false
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
	case "listClusters", "listOrphanedVolumes", "listOperations", "listEvents", "streamEvents", "getPrometheusMetrics", "getOperation", "cancelOperation", "listBackupStorages", "listNamespaces":
		return true
	}
	return false
}
func (s *Server) middleware(next api.StrictHandlerFunc, name string) api.StrictHandlerFunc {
	return func(ctx context.Context, w http.ResponseWriter, r *http.Request, v any) (any, error) {
		// Binding has finished: downstream middleware uses decoded PathValues,
		// unlike the generated binder's encoded-segment input contract.
		if _, escaped := r.Context().Value(wireURLKey{}).(*url.URL); escaped {
			for segment := range strings.SplitSeq(r.Pattern, "/") {
				if len(segment) > 2 && segment[0] == '{' && segment[len(segment)-1] == '}' {
					key := segment[1 : len(segment)-1]
					decoded, err := url.PathUnescape(r.PathValue(key))
					if err != nil {
						return invalidField(key, domain.ReasonInvalid, key+" must use valid URL path escaping"), nil
					}
					r.SetPathValue(key, decoded)
				}
			}
		}
		id := operationID(name)
		rule, exists := api.OperationRBAC[id]
		if !exists {
			return fail(403, "forbidden", "Unknown API operation"), nil
		}
		ctx = context.WithValue(ctx, requestKey{}, r)
		if cluster := r.PathValue("cluster"); cluster != "" {
			if err := domain.ValidateClusterName(cluster); err != nil {
				return fail(404, "not_found", "Resource not found"), nil
			}
		}
		if namespace := r.PathValue("namespace"); namespace != "" {
			if err := domain.ValidateNamespaceName(namespace); err != nil {
				return fail(404, "not_found", "Resource not found"), nil
			}
		}
		if namespaces, present := r.URL.Query()["namespace"]; present {
			if len(namespaces) != 1 || domain.ValidateNamespaceName(namespaces[0]) != nil {
				return invalidField("namespace", domain.ReasonInvalid, "namespace must contain 2–32 lowercase letters, digits or hyphens, start with a letter and end with a letter or digit"), nil
			}
		}
		if id == "login" || id == "refreshSession" || id == "logout" || id == "oidcLogin" {
			if !s.sameOrigin(r) {
				return fail(403, "forbidden", "Cross-origin authentication request rejected"), nil
			}
			if id == "login" {
				kind, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
				if err != nil || kind != "application/json" {
					return failDetails(415, "unsupported_media_type", "Content-Type must be application/json for login", map[string]any{"field": "Content-Type", "reason": domain.ReasonUnsupported}), nil
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
					return failDetails(403, "forbidden", fmt.Sprintf("Permission denied: %s %s on %q; ask an administrator to grant access", rule.Resource, rule.Action, obj), map[string]any{"resource": rule.Resource, "action": rule.Action, "object": obj}), nil
				}
			}
		}
		// Public and deferred-authentication routes must not reveal registration.
		// Cluster creation checks name syntax before resolving its namespace.
		if namespace := r.URL.Query().Get("namespace"); !rule.Public && namespace != "" && id != "createCluster" && s.namespaceAllowed(ctx, namespace) {
			if failure := s.requireNamespace(ctx, namespace); failure != nil {
				return *failure, nil
			}
		}
		return s.idempotent(next, id, ctx, w, r.WithContext(ctx), v)
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

type wireURLKey struct{}

func (m operationMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	originalRequest := r
	// ServeMux decodes wildcard segments, while generated oapi handlers decode
	// them again. It also treats a decoded %2F as a trailing slash rather than a
	// wildcard. Route the encoded representation and restore the original URL
	// before dispatch, leaving encoded PathValues for the generated binder.
	if escaped := r.URL.EscapedPath(); strings.HasPrefix(r.URL.Path, "/api/v1/") && escaped != r.URL.Path {
		original := r.URL
		r = r.Clone(context.WithValue(r.Context(), wireURLKey{}, original))
		r.URL.Path, r.URL.RawPath = escaped, ""
	}
	m.ServeMux.ServeHTTP(w, r)
	originalRequest.Pattern = r.Pattern
}

func (m operationMux) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	switch pattern {
	case "GET /api/v1/healthz":
		pattern = "GET /healthz"
	case "GET /api/v1/metrics":
		pattern = "GET /metrics"
	}
	m.ServeMux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		if original, ok := r.Context().Value(wireURLKey{}).(*url.URL); ok {
			r.URL = original
		}
		handler(w, r)
	})
}

// Handler returns the public router, including root health/metrics and SPA history fallback.
func (s *Server) Handler() http.Handler {
	strict := api.NewStrictHandlerWithOptions(s, []api.StrictMiddlewareFunc{s.middleware}, api.StrictHTTPServerOptions{
		RequestErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			var timestamp *time.ParseError
			if errors.As(err, &timestamp) && (r.URL.Path == "/api/v1/api-tokens" || strings.HasPrefix(r.URL.Path, "/api/v1/api-tokens/")) {
				_ = invalidField("expiresAt", domain.ReasonInvalid, "expiresAt must be a valid RFC 3339 timestamp").write(w)
				return
			}
			_ = decoderFailure(err).write(w)
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
	routes := operationMux{mux}
	api.HandlerWithOptions(strict, api.StdHTTPServerOptions{BaseURL: "/api/v1", BaseRouter: routes, ErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
		_ = parameterFailure(err).write(w)
	}})
	mux.HandleFunc("/api/v1/", func(w http.ResponseWriter, r *http.Request) {
		_ = fail(404, "not_found", "API route not found").write(w)
	})
	mux.Handle("/", ui.Handler())
	validated := s.validationMiddleware(routes)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		id := requestID(r)
		w.Header().Set("X-Request-ID", id)
		r.Header.Set("X-Request-ID", id)
		defer func() {
			// Route patterns contain no names, credentials or query values.
			s.Logger.Info("HTTP request", "request_id", id, "method", r.Method, "route", r.Pattern, "duration", time.Since(started))
		}()
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; font-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
		if strings.HasPrefix(s.PublicURL, "https://") {
			w.Header().Set("Strict-Transport-Security", "max-age=31536000")
		}
		if r.ContentLength > 2<<20 {
			_ = fail(413, "payload_too_large", "body exceeds the 2 MiB size limit").write(w)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
		var readable bool
		r, readable = s.captureIdempotencyBody(w, r)
		if !readable {
			return
		}
		validated.ServeHTTP(w, r)
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
			return page[T]{}, domain.InvalidField("limit", domain.ReasonOutOfRange, "limit must be an integer from 1 to 500")
		}
		limit = n
	}
	offset := 0
	if v := q.Get("cursor"); v != "" {
		b, err := base64.RawURLEncoding.DecodeString(v)
		if err != nil {
			return page[T]{}, domain.InvalidField("cursor", domain.ReasonInvalid, "cursor must be a cursor returned by the preceding page")
		}
		offset, err = strconv.Atoi(string(b))
		if err != nil || offset < 0 {
			return page[T]{}, domain.InvalidField("cursor", domain.ReasonInvalid, "cursor must encode a nonnegative page offset")
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
	return &rc, p, nil
}

func (s *Server) providerContext(rc *engine.Context) *response {
	backend, err := s.namespaces.Backend(rc.Namespace)
	if err != nil {
		r := fail(503, "invalid_namespace", "Namespace "+rc.Namespace+" is unavailable; repair its token or relay")
		return &r
	}
	rc.Backend = backend
	rc.Dialer = s.namespaces.Dialer(rc.Namespace)
	rc.AgentURL = s.namespaces.AgentURL(rc.Namespace)
	return nil
}
func (s *Server) trigger(ctx context.Context, ref domain.ClusterRef) {
	if pending, ok := ctx.Value(triggerKey{}).(*domain.ClusterRef); ok {
		*pending = ref
		return
	}
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
	if failure := s.requireNamespace(context.Background(), namespace); failure != nil {
		return failure
	}
	if _, err := s.namespaces.Backend(namespace); err != nil {
		r := fail(422, "invalid_namespace", "Namespace "+namespace+" does not exist or is unavailable")
		return &r
	}
	return nil
}
func (s *Server) handle(ctx context.Context, name string, in input) response {
	var pending domain.ClusterRef
	ctx = context.WithValue(ctx, triggerKey{}, &pending)
	if p := principal(ctx); p != nil {
		ctx = store.WithActor(ctx, p.Actor())
	}
	result := s.dispatch(ctx, name, in)
	if err := s.auditAction(ctx, name, in, result); err != nil {
		return s.failure(err)
	}
	if pending.Name != "" && s.Trigger != nil {
		s.Trigger.Trigger(pending)
	}
	return result
}

func (s *Server) dispatch(ctx context.Context, name string, in input) response {
	switch name {
	case "ListElasticUsers", "GetElasticUser", "CreateElasticUser", "UpdateElasticUser", "DeleteElasticUser", "ListElasticRoles", "GetElasticRole", "CreateElasticRole", "UpdateElasticRole", "DeleteElasticRole", "ListElasticAPIKeys", "CreateElasticAPIKey", "InvalidateElasticAPIKey":
		return s.elasticAccess(ctx, name, in)
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
	case "CreateNamespace", "UpdateNamespace", "RotateNamespaceToken", "ValidateNamespace", "DeleteNamespace":
		return s.namespaceMutation(ctx, name, in)
	case "ListOrphanedVolumes":
		return s.orphanedVolumes(ctx)
	case "ListEvents", "StreamEvents", "ListOperations", "GetOperation", "CancelOperation", "GetPrometheusMetrics", "GetClusterMetrics":
		return s.observability(ctx, name, in)
	default:
		return s.cluster(ctx, name, in)
	}
}
