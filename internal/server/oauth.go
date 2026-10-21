// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package server

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/api"
	"net/http"
)

func (s *Server) ListOAuthGrants(ctx context.Context, request api.ListOAuthGrantsRequestObject) (api.ListOAuthGrantsResponseObject, error) {
	return s.handle(ctx, "ListOAuthGrants", input{}), nil
}
func (r response) VisitListOAuthGrantsResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) DeleteOAuthGrant(ctx context.Context, request api.DeleteOAuthGrantRequestObject) (api.DeleteOAuthGrantResponseObject, error) {
	return s.handle(ctx, "DeleteOAuthGrant", input{Token: request.Grant}), nil
}
func (r response) VisitDeleteOAuthGrantResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) oauthGrants(ctx context.Context, name string, in input) response {
	if name == "ListOAuthGrants" {
		result := api.OAuthGrantList{Enabled: s.OAuth != nil, Items: []api.OAuthGrant{}}
		if s.OAuth == nil {
			return ok(result)
		}
		grants, e := s.OAuth.List(ctx)
		if e != nil {
			return s.failure(e)
		}
		for _, g := range grants {
			if g.Owner != principal(ctx).Account && !s.allowed(ctx, "api-tokens", "read", "*") {
				continue
			}
			namespaces := g.Restrictions.Namespaces
			if namespaces == nil {
				namespaces = []string{}
			}
			result.Items = append(result.Items, api.OAuthGrant{Id: g.ID, Owner: g.Owner, ClientId: g.ClientID, ClientName: g.ClientName, ClientHost: g.ClientHost, Scopes: g.Scopes, Namespaces: namespaces, CreatedAt: g.CreatedAt, ExpiresAt: g.ExpiresAt, LastUsedAt: g.LastUsedAt})
		}
		return ok(result)
	}
	if s.OAuth == nil {
		return fail(404, "not_found", "OAuth is disabled")
	}
	g, e := s.OAuth.Get(ctx, in.Token)
	if e != nil {
		return s.failure(e)
	}
	if g.Owner != principal(ctx).Account && !s.allowed(ctx, "api-tokens", "delete", "*") {
		return fail(403, "forbidden", "Grant belongs to another account")
	}
	if e = s.OAuth.Revoke(ctx, in.Token); e != nil {
		return s.failure(e)
	}
	return response{status: 204}
}
