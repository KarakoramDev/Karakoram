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
	"github.com/KarakoramDev/Karakoram/internal/namespaces"
)

func namespaceAPI(n *domain.Namespace, count int) api.Namespace {
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
	return api.Namespace{Name: n.Name, Description: optionalString(n.Description), ProjectId: n.ProjectID, ProjectName: n.ProjectName, Default: n.Default, Phase: api.NamespacePhase(n.Phase), ClusterCount: &count, CreatedAt: n.CreatedAt, UpdatedAt: n.UpdatedAt, Token: api.TokenStatus{Source: api.TokenStatusSource(n.Token.Source), Valid: n.Token.Valid, LastValidatedAt: optionalTime(n.Token.LastValidatedAt), LastError: optionalString(n.Token.LastError), RotatedAt: optionalTime(n.Token.RotatedAt)}, Relay: api.RelayStatus{Hostname: optionalString(n.Relay.Hostname), ServiceId: optionalString(n.Relay.ServiceID), Phase: api.RelayStatusPhase(n.Relay.Phase), Connected: n.Relay.Connected, ConnectedSince: optionalTime(n.Relay.ConnectedSince), LastSeenAt: optionalTime(n.Relay.LastSeenAt), Version: optionalString(n.Relay.Version), DesiredVersion: optionalString(n.Relay.DesiredVersion), CertNotAfter: optionalTime(n.Relay.CertNotAfter), Reconnects: n.Relay.Reconnects, LastError: optionalString(n.Relay.LastError)}}
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
	switch {
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
	return response{status: status, body: namespaceAPI(n, count), noStore: true}
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
		items = append(items, namespaceAPI(n, count))
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
			return invalid()
		}
		n, err = r.Create(ctx, b.Name, b.ProjectId, *b.Token, value(b.Description))
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
			return invalid()
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
