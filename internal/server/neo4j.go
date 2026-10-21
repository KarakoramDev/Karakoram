// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bytes"
	"context"
	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j"
	"io"
	"net/http"
)

// The generated strict decoder expects JSON even for an optional request body.
// Normalize an absent password body only after OpenAPI validation, preserving
// the original captured bytes for idempotency and all nonempty request bodies.
func neo4jOptionalPasswordBody(next func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			_ = decoderFailure(err).write(w)
			return
		}
		if len(body) == 0 {
			body = []byte("{}")
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		next(w, r)
	}
}

func (s *Server) neo4j(ctx context.Context, name string, in input, rc *engine.Context, provider engine.Provider) response {
	access, supported := provider.(neo4j.Access)
	if !supported || rc.Cluster.Spec.Provider != "neo4j" {
		return s.failure(engine.ErrNotSupported)
	}
	changed := func(status int, body any, noStore bool) response {
		s.trigger(ctx, rc.Cluster.Ref())
		return response{status: status, body: body, noStore: noStore}
	}
	switch name {
	case "ListNeo4jUsers":
		users, err := access.ListUsers(ctx, rc)
		if err != nil {
			return s.failure(err)
		}
		return ok(page[neo4j.User]{Items: nonNil(users)})
	case "GetNeo4jUser":
		user, err := access.GetUser(ctx, rc, in.User)
		if err != nil {
			return s.failure(err)
		}
		return ok(user)
	case "CreateNeo4jUser":
		body := in.Body.(*api.Neo4jUserCreate)
		password, err := access.CreateUser(ctx, rc, body.Name, value(body.Password))
		if err != nil {
			return s.failure(err)
		}
		out := api.Neo4jUserCreated{User: api.Neo4jUser{Name: body.Name, PasswordChangeRequired: false}}
		if password != "" {
			out.GeneratedPassword = &password
		}
		return changed(201, out, true)
	case "SetNeo4jUserPassword":
		var password string
		if body, ok := in.Body.(*api.Neo4jUserPassword); ok && body != nil {
			password = value(body.Password)
		}
		generated, err := access.SetUserPassword(ctx, rc, in.User, password)
		if err != nil {
			return s.failure(err)
		}
		out := api.Neo4jUserPasswordChanged{}
		if generated != "" {
			out.GeneratedPassword = &generated
		}
		return changed(200, out, true)
	case "DeleteNeo4jUser":
		if err := access.DeleteUser(ctx, rc, in.User); err != nil {
			return s.failure(err)
		}
		return changed(204, nil, false)
	}
	return s.failure(engine.ErrNotSupported)
}

func (s *Server) ListNeo4jUsers(ctx context.Context, r api.ListNeo4jUsersRequestObject) (api.ListNeo4jUsersResponseObject, error) {
	return s.handle(ctx, "ListNeo4jUsers", input{Cluster: r.Cluster}), nil
}
func (r response) VisitListNeo4jUsersResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) GetNeo4jUser(ctx context.Context, r api.GetNeo4jUserRequestObject) (api.GetNeo4jUserResponseObject, error) {
	return s.handle(ctx, "GetNeo4jUser", input{Cluster: r.Cluster, User: r.User}), nil
}
func (r response) VisitGetNeo4jUserResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) CreateNeo4jUser(ctx context.Context, r api.CreateNeo4jUserRequestObject) (api.CreateNeo4jUserResponseObject, error) {
	return s.handle(ctx, "CreateNeo4jUser", input{Cluster: r.Cluster, Body: r.Body}), nil
}
func (r response) VisitCreateNeo4jUserResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) SetNeo4jUserPassword(ctx context.Context, r api.SetNeo4jUserPasswordRequestObject) (api.SetNeo4jUserPasswordResponseObject, error) {
	return s.handle(ctx, "SetNeo4jUserPassword", input{Cluster: r.Cluster, User: r.User, Body: r.Body}), nil
}
func (r response) VisitSetNeo4jUserPasswordResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) DeleteNeo4jUser(ctx context.Context, r api.DeleteNeo4jUserRequestObject) (api.DeleteNeo4jUserResponseObject, error) {
	return s.handle(ctx, "DeleteNeo4jUser", input{Cluster: r.Cluster, User: r.User}), nil
}
func (r response) VisitDeleteNeo4jUserResponse(w http.ResponseWriter) error { return r.write(w) }
