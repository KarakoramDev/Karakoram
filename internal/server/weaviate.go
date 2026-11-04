// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate"
)

func weaviateUser(v weaviate.User) api.WeaviateUser {
	return api.WeaviateUser{Name: v.Name, UserType: v.UserType, Active: v.Active, Roles: nonNil(v.Roles), CreatedAt: v.CreatedAt, LastUsedAt: v.LastUsedAt}
}

// JSON conversion uses both models' additional-property support, preserving
// native permission scopes and future fields at every nesting level.
func weaviateRole(v api.WeaviateRole) (weaviate.Role, error) {
	var out weaviate.Role
	b, err := json.Marshal(v)
	if err != nil {
		return out, err
	}
	err = json.Unmarshal(b, &out)
	return out, err
}

func (s *Server) weaviateRolesAllowed(ctx context.Context, name string, rc *engine.Context, roles []string) bool {
	for _, role := range roles {
		if role == "viewer" || role == "read-only" {
			continue
		}
		return s.weaviateAdminAllowed(ctx, name, rc)
	}
	return true
}
func (s *Server) weaviateAdminAllowed(ctx context.Context, name string, rc *engine.Context) bool {
	rule, exists := api.OperationAdditionalRBAC[operationID(name)]
	return exists && !rule.Public && rule.ObjectParam == "cluster" && s.allowed(ctx, rule.Resource, rule.Action, clusterObject(rc.Cluster.Namespace, rc.Cluster.Name))
}

// authorizeWeaviateReplay repeats conditional credential authorization before
// consulting the idempotency cache. Rotation uses current native roles rather
// than trusting permissions from the original request.
func (s *Server) authorizeWeaviateReplay(ctx context.Context, id string, v any) *response {
	var cluster, user string
	var roles []string
	switch q := v.(type) {
	case api.CreateWeaviateUserRequestObject:
		cluster = q.Cluster
		if q.Body != nil {
			roles = q.Body.Roles
		}
	case api.RotateWeaviateUserKeyRequestObject:
		cluster, user = q.Cluster, q.User
	default:
		return nil
	}
	rc, provider, err := s.context(ctx, clusterRef(ctx, cluster))
	if err != nil {
		failure := s.failure(err)
		return &failure
	}
	if id == "rotateWeaviateUserKey" {
		access, ok := provider.(weaviate.Access)
		if !ok {
			failure := s.failure(engine.ErrNotSupported)
			return &failure
		}
		current, err := access.GetUser(ctx, rc, user)
		if err != nil {
			failure := s.failure(err)
			return &failure
		}
		roles = current.Roles
	}
	if !s.weaviateRolesAllowed(ctx, id, rc, roles) {
		failure := fail(403, "forbidden", "Weaviate privileged credentials require clusters/update permission")
		return &failure
	}
	return nil
}

func (s *Server) weaviate(ctx context.Context, name string, in input, rc *engine.Context, p engine.Provider) response {
	a, yes := p.(weaviate.Access)
	if !yes {
		return s.failure(engine.ErrNotSupported)
	}
	switch name {
	case "ListWeaviateUsers":
		users, err := a.ListUsers(ctx, rc)
		if err != nil {
			return s.failure(err)
		}
		out := make([]api.WeaviateUser, len(users))
		for i, v := range users {
			out[i] = weaviateUser(v)
		}
		return ok(page[api.WeaviateUser]{Items: out})
	case "GetWeaviateUser":
		user, err := a.GetUser(ctx, rc, in.Name)
		if err != nil {
			return s.failure(err)
		}
		return ok(weaviateUser(*user))
	case "CreateWeaviateUser":
		b := in.Body.(*api.WeaviateUserCreate)
		if !s.weaviateRolesAllowed(ctx, name, rc, b.Roles) {
			return fail(403, "forbidden", "Weaviate privileged roles require clusters/update permission")
		}
		v, err := a.CreateUser(ctx, rc, weaviate.UserCreate{Name: b.Name, Roles: b.Roles})
		if err != nil {
			return s.failure(err)
		}
		return response{status: 201, body: api.WeaviateUserCreated{User: weaviateUser(v.User), ApiKey: &v.APIKey}, noStore: true}
	case "UpdateWeaviateUser":
		b := in.Body.(*api.WeaviateUserUpdate)
		if !s.weaviateRolesAllowed(ctx, name, rc, b.Roles) {
			return fail(403, "forbidden", "Weaviate privileged roles require clusters/update permission")
		}
		if err := a.UpdateUser(ctx, rc, in.Name, weaviate.UserUpdate{Roles: b.Roles, Active: b.Active}); err != nil {
			return s.failure(err)
		}
		return response{status: 204}
	case "DeleteWeaviateUser":
		if err := a.DeleteUser(ctx, rc, in.Name); err != nil {
			return s.failure(err)
		}
		return response{status: 204}
	case "RotateWeaviateUserKey":
		user, err := a.GetUser(ctx, rc, in.Name)
		if err != nil {
			return s.failure(err)
		}
		if !s.weaviateRolesAllowed(ctx, name, rc, user.Roles) {
			return fail(403, "forbidden", "Weaviate privileged user keys require clusters/update permission")
		}
		key, err := a.RotateUserKey(ctx, rc, in.Name)
		if err != nil {
			return s.failure(err)
		}
		return response{status: 200, body: api.WeaviateAPIKey{ApiKey: &key}, noStore: true}
	case "ListWeaviateRoles":
		roles, err := a.ListRoles(ctx, rc)
		if err != nil {
			return s.failure(err)
		}
		for i := range roles {
			roles[i].Permissions = nonNil(roles[i].Permissions)
		}
		return ok(page[weaviate.Role]{Items: nonNil(roles)})
	case "GetWeaviateRole":
		role, err := a.GetRole(ctx, rc, in.Name)
		if err != nil {
			return s.failure(err)
		}
		role.Permissions = nonNil(role.Permissions)
		return ok(role)
	case "CreateWeaviateRole", "UpdateWeaviateRole":
		if !s.weaviateAdminAllowed(ctx, name, rc) {
			return fail(403, "forbidden", "Weaviate role changes require clusters/update permission")
		}
		b := in.Body.(*api.WeaviateRole)
		if name == "UpdateWeaviateRole" && b.Name != in.Name {
			return invalidField("name", domain.ReasonInvalid, "name must match the path parameter")
		}
		role, err := weaviateRole(*b)
		if err != nil {
			return s.failure(err)
		}
		status := 200
		if name == "CreateWeaviateRole" {
			err = a.CreateRole(ctx, rc, role)
			status = 201
		} else {
			err = a.ReplaceRole(ctx, rc, role)
		}
		if err != nil {
			return s.failure(err)
		}
		role.Permissions = nonNil(role.Permissions)
		return response{status: status, body: role}
	case "DeleteWeaviateRole":
		if !s.weaviateAdminAllowed(ctx, name, rc) {
			return fail(403, "forbidden", "Weaviate role changes require clusters/update permission")
		}
		if err := a.DeleteRole(ctx, rc, in.Name); err != nil {
			return s.failure(err)
		}
		return response{status: 204}
	}
	return fail(500, "internal_error", "Unknown Weaviate operation")
}
func (s *Server) ListWeaviateUsers(ctx context.Context, request api.ListWeaviateUsersRequestObject) (api.ListWeaviateUsersResponseObject, error) {
	return s.handle(ctx, "ListWeaviateUsers", input{Cluster: request.Cluster}), nil
}
func (r response) VisitListWeaviateUsersResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) CreateWeaviateUser(ctx context.Context, request api.CreateWeaviateUserRequestObject) (api.CreateWeaviateUserResponseObject, error) {
	return s.handle(ctx, "CreateWeaviateUser", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitCreateWeaviateUserResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) GetWeaviateUser(ctx context.Context, request api.GetWeaviateUserRequestObject) (api.GetWeaviateUserResponseObject, error) {
	return s.handle(ctx, "GetWeaviateUser", input{Cluster: request.Cluster, Name: request.User}), nil
}
func (r response) VisitGetWeaviateUserResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) UpdateWeaviateUser(ctx context.Context, request api.UpdateWeaviateUserRequestObject) (api.UpdateWeaviateUserResponseObject, error) {
	return s.handle(ctx, "UpdateWeaviateUser", input{Cluster: request.Cluster, Name: request.User, Body: request.Body}), nil
}
func (r response) VisitUpdateWeaviateUserResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) DeleteWeaviateUser(ctx context.Context, request api.DeleteWeaviateUserRequestObject) (api.DeleteWeaviateUserResponseObject, error) {
	return s.handle(ctx, "DeleteWeaviateUser", input{Cluster: request.Cluster, Name: request.User}), nil
}
func (r response) VisitDeleteWeaviateUserResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) RotateWeaviateUserKey(ctx context.Context, request api.RotateWeaviateUserKeyRequestObject) (api.RotateWeaviateUserKeyResponseObject, error) {
	return s.handle(ctx, "RotateWeaviateUserKey", input{Cluster: request.Cluster, Name: request.User}), nil
}
func (r response) VisitRotateWeaviateUserKeyResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) ListWeaviateRoles(ctx context.Context, request api.ListWeaviateRolesRequestObject) (api.ListWeaviateRolesResponseObject, error) {
	return s.handle(ctx, "ListWeaviateRoles", input{Cluster: request.Cluster}), nil
}
func (r response) VisitListWeaviateRolesResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) CreateWeaviateRole(ctx context.Context, request api.CreateWeaviateRoleRequestObject) (api.CreateWeaviateRoleResponseObject, error) {
	return s.handle(ctx, "CreateWeaviateRole", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitCreateWeaviateRoleResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) GetWeaviateRole(ctx context.Context, request api.GetWeaviateRoleRequestObject) (api.GetWeaviateRoleResponseObject, error) {
	return s.handle(ctx, "GetWeaviateRole", input{Cluster: request.Cluster, Name: request.Role}), nil
}
func (r response) VisitGetWeaviateRoleResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) UpdateWeaviateRole(ctx context.Context, request api.UpdateWeaviateRoleRequestObject) (api.UpdateWeaviateRoleResponseObject, error) {
	return s.handle(ctx, "UpdateWeaviateRole", input{Cluster: request.Cluster, Name: request.Role, Body: request.Body}), nil
}
func (r response) VisitUpdateWeaviateRoleResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) DeleteWeaviateRole(ctx context.Context, request api.DeleteWeaviateRoleRequestObject) (api.DeleteWeaviateRoleResponseObject, error) {
	return s.handle(ctx, "DeleteWeaviateRole", input{Cluster: request.Cluster, Name: request.Role}), nil
}
func (r response) VisitDeleteWeaviateRoleResponse(w http.ResponseWriter) error { return r.write(w) }
