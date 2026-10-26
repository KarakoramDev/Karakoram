// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/namespaces"
	"github.com/KarakoramDev/Karakoram/internal/redact"
)

// requireNamespace checks registration, not relay health: unavailable namespaces
// still have readable inventory. Single-project installations have an implicit default.
func (s *Server) requireNamespace(ctx context.Context, name string) *response {
	if _, err := s.Store.GetNamespace(ctx, name); err != nil {
		if name == domain.DefaultNamespace && errors.Is(err, namespaces.ErrNotFound) {
			if _, registered := s.namespaces.(*namespaces.Registry); !registered {
				return nil
			}
		}
		r := s.failure(err)
		return &r
	}
	return nil
}

func namespaceAPI(n *domain.Namespace, count int, capabilities infra.Capabilities) api.Namespace {
	safe := *n
	n = &safe
	n.Token.LastError = redact.Diagnostic(n.Token.LastError)
	n.Relay.LastError = redact.Diagnostic(n.Relay.LastError)
	optionalString := func(v string) *string {
		if v == "" {
			return nil
		}
		return &v
	}
	optionalTime := func(v time.Time) *time.Time {
		if v.IsZero() {
			return nil
		}
		return &v
	}
	caps := []api.Capability{}
	for _, name := range capabilities.List() {
		caps = append(caps, api.Capability(name))
	}
	backend := n.Backend
	if backend == "" {
		backend = "zerops"
	}
	var config *map[string]string
	if n.Config != nil {
		config = &n.Config
	}
	return api.Namespace{Backend: backend, Capabilities: caps, Config: config, Name: n.Name, Description: optionalString(n.Description), ProjectId: n.ProjectID, ProjectName: n.ProjectName, Default: n.Default, Phase: api.NamespacePhase(n.Phase), ClusterCount: &count, CreatedAt: n.CreatedAt, UpdatedAt: n.UpdatedAt, Token: api.TokenStatus{Source: api.TokenStatusSource(n.Token.Source), Valid: n.Token.Valid, LastValidatedAt: optionalTime(n.Token.LastValidatedAt), LastError: optionalString(n.Token.LastError), RotatedAt: optionalTime(n.Token.RotatedAt)}, Relay: api.RelayStatus{Hostname: optionalString(n.Relay.Hostname), ServiceId: optionalString(n.Relay.ServiceID), Phase: api.RelayStatusPhase(n.Relay.Phase), Connected: n.Relay.Connected, ConnectedSince: optionalTime(n.Relay.ConnectedSince), LastSeenAt: optionalTime(n.Relay.LastSeenAt), Version: optionalString(n.Relay.Version), DesiredVersion: optionalString(n.Relay.DesiredVersion), CertNotAfter: optionalTime(n.Relay.CertNotAfter), Reconnects: n.Relay.Reconnects, LastError: optionalString(n.Relay.LastError)}}
}
func (s *Server) namespaceRegistry() (*namespaces.Registry, error) {
	r, ok := s.namespaces.(*namespaces.Registry)
	if !ok {
		return nil, errors.New("namespace registry is unavailable")
	}
	return r, nil
}
func (s *Server) namespaceFailure(err error) response {
	var unavailable *namespaces.PlatformUnavailableError
	if errors.As(err, &unavailable) {
		return fail(http.StatusInternalServerError, "internal_error", unavailable.Error())
	}
	var fieldErr *domain.FieldError
	var resourceErr *domain.ResourceError
	if errors.As(err, &fieldErr) || errors.As(err, &resourceErr) {
		return s.failure(err)
	}
	switch {
	case errors.Is(err, infra.ErrUnsupported), errors.Is(err, infra.ErrRejected):
		return fail(400, "invalid_request", err.Error())
	case errors.Is(err, namespaces.ErrInvalid):
		return fail(422, "invalid_namespace", err.Error())
	case errors.Is(err, namespaces.ErrConflict):
		return fail(409, "conflict", err.Error())
	default:
		return s.failure(err)
	}
}
func (s *Server) namespaceResponse(ctx context.Context, r *namespaces.Registry, n *domain.Namespace, status int) response {
	count, err := r.ClusterCount(ctx, n.Name)
	if err != nil {
		return s.namespaceFailure(err)
	}
	return response{status: status, body: namespaceAPI(n, count, s.namespaceCapabilities(n.Name, n.Backend)), noStore: true}
}
func (s *Server) ListNamespaces(ctx context.Context, request api.ListNamespacesRequestObject) (api.ListNamespacesResponseObject, error) {
	r, err := s.namespaceRegistry()
	if err != nil {
		return s.namespaceFailure(err), nil
	}
	all, err := r.List(ctx)
	if err != nil {
		return s.namespaceFailure(err), nil
	}
	items := make([]api.Namespace, 0, len(all))
	for i := range all {
		n := &all[i]
		if !s.allowed(ctx, "namespaces", "read", n.Name) {
			continue
		}
		count, err := r.ClusterCount(ctx, n.Name)
		if err != nil {
			return s.namespaceFailure(err), nil
		}
		items = append(items, namespaceAPI(n, count, s.namespaceCapabilities(n.Name, n.Backend)))
	}
	return listResponse(s, ctx, items, nil), nil
}
func (r response) VisitListNamespacesResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) CreateNamespace(ctx context.Context, request api.CreateNamespaceRequestObject) (api.CreateNamespaceResponseObject, error) {
	return s.handle(ctx, "CreateNamespace", input{Body: request.Body}), nil
}
func (r response) VisitCreateNamespaceResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) GetNamespace(ctx context.Context, request api.GetNamespaceRequestObject) (api.GetNamespaceResponseObject, error) {
	r, err := s.namespaceRegistry()
	if err != nil {
		return s.namespaceFailure(err), nil
	}
	n, err := r.Get(ctx, request.Namespace)
	if err != nil {
		return s.namespaceFailure(err), nil
	}
	return s.namespaceResponse(ctx, r, n, 200), nil
}
func (r response) VisitGetNamespaceResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) UpdateNamespace(ctx context.Context, request api.UpdateNamespaceRequestObject) (api.UpdateNamespaceResponseObject, error) {
	return s.handle(ctx, "UpdateNamespace", input{Namespace: request.Namespace, Body: request.Body}), nil
}
func (r response) VisitUpdateNamespaceResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) RotateNamespaceToken(ctx context.Context, request api.RotateNamespaceTokenRequestObject) (api.RotateNamespaceTokenResponseObject, error) {
	return s.handle(ctx, "RotateNamespaceToken", input{Namespace: request.Namespace, Body: request.Body}), nil
}
func (r response) VisitRotateNamespaceTokenResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) ValidateNamespace(ctx context.Context, request api.ValidateNamespaceRequestObject) (api.ValidateNamespaceResponseObject, error) {
	return s.handle(ctx, "ValidateNamespace", input{Namespace: request.Namespace}), nil
}
func (r response) VisitValidateNamespaceResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) DeleteNamespace(ctx context.Context, request api.DeleteNamespaceRequestObject) (api.DeleteNamespaceResponseObject, error) {
	return s.handle(ctx, "DeleteNamespace", input{Namespace: request.Namespace}), nil
}
func (r response) VisitDeleteNamespaceResponse(w http.ResponseWriter) error { return r.write(w) }

func (s *Server) namespaceMutation(ctx context.Context, name string, in input) response {
	r, err := s.namespaceRegistry()
	if err != nil {
		return s.namespaceFailure(err)
	}
	var n *domain.Namespace
	status := 200
	switch name {
	case "CreateNamespace":
		b := in.Body.(*api.CreateNamespaceJSONRequestBody)
		if b == nil || b.Token == nil {
			return invalidField("token", domain.ReasonRequired, "token is required")
		}
		var config map[string]string
		if b.Config != nil {
			config = *b.Config
		}
		n, err = r.Create(ctx, b.Name, b.ProjectId, *b.Token, value(b.Description), value(b.Backend), config)
		status = 201
	case "UpdateNamespace":
		b := in.Body.(*api.UpdateNamespaceJSONRequestBody)
		if b == nil {
			return invalid()
		}
		n, err = r.Update(ctx, in.Namespace, b.Description)
	case "RotateNamespaceToken":
		b := in.Body.(*api.RotateNamespaceTokenJSONRequestBody)
		if b == nil || b.Token == nil {
			return invalidField("token", domain.ReasonRequired, "token is required")
		}
		n, err = r.RotateToken(ctx, in.Namespace, *b.Token)
	case "ValidateNamespace":
		n, err = r.Validate(ctx, in.Namespace)
	case "DeleteNamespace":
		if err = r.Delete(ctx, in.Namespace); err != nil {
			return s.namespaceFailure(err)
		}
		return response{status: 202, noStore: true}
	}
	if err != nil {
		return s.namespaceFailure(err)
	}
	return s.namespaceResponse(ctx, r, n, status)
}

func (s *Server) namespaceCapabilities(namespace, kind string) infra.Capabilities {
	if s.namespaces != nil {
		if backend, err := s.namespaces.Backend(domain.NormalizedNamespace(namespace)); err == nil && backend != nil {
			return backend.Capabilities()
		}
	}
	if kind == "" {
		kind = "zerops"
	}
	f, err := s.Backends.Get(kind)
	if err != nil {
		return infra.Capabilities{}
	}
	return f.Info().Capabilities
}

func (s *Server) ListBackends(ctx context.Context, request api.ListBackendsRequestObject) (api.ListBackendsResponseObject, error) {
	items := []api.BackendInfo{}
	for _, info := range s.Backends.List() {
		caps := []api.Capability{}
		for _, capability := range info.Capabilities.List() {
			caps = append(caps, api.Capability(capability))
		}
		keys := append([]string{}, info.ConfigKeys...)
		items = append(items, api.BackendInfo{Kind: info.Kind, DisplayName: info.DisplayName, Experimental: info.Experimental, Relay: info.Relay, Capabilities: caps, ConfigKeys: keys})
	}
	return response{status: 200, body: api.BackendList{Items: items}}, nil
}

func (r response) VisitListBackendsResponse(w http.ResponseWriter) error { return r.write(w) }
