// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic"
)

func elasticUserAPI(u elastic.User) api.ElasticUser {
	return api.ElasticUser{Username: u.Username, Roles: nonNil(u.Roles), FullName: optionalString(u.FullName), Email: optionalString(u.Email), Enabled: u.Enabled}
}
func elasticDescriptorDomain(cluster []string, indices []api.ElasticIndexPrivileges, applications []api.ElasticApplicationPrivileges) elastic.Role {
	r := elastic.Role{Cluster: cluster, Indices: make([]elastic.IndexPrivileges, 0, len(indices)), Applications: make([]elastic.ApplicationPrivileges, 0, len(applications))}
	for _, v := range indices {
		index := elastic.IndexPrivileges{Names: v.Names, Privileges: v.Privileges, AllowRestrictedIndices: v.AllowRestrictedIndices, Query: v.Query}
		if v.FieldSecurity != nil {
			index.FieldSecurity = &elastic.FieldSecurity{Grant: v.FieldSecurity.Grant, Except: v.FieldSecurity.Except}
		}
		r.Indices = append(r.Indices, index)
	}
	for _, v := range applications {
		r.Applications = append(r.Applications, elastic.ApplicationPrivileges{Application: v.Application, Privileges: v.Privileges, Resources: v.Resources})
	}
	return r
}
func elasticRoleAPI(r elastic.AccessRole) api.ElasticRole {
	out := api.ElasticRole{Name: r.Name, Reserved: ptr(r.Reserved), Cluster: nonNil(r.Cluster), Indices: make([]api.ElasticIndexPrivileges, 0, len(r.Indices)), Applications: make([]api.ElasticApplicationPrivileges, 0, len(r.Applications))}
	for _, v := range r.Indices {
		index := api.ElasticIndexPrivileges{Names: nonNil(v.Names), Privileges: nonNil(v.Privileges), AllowRestrictedIndices: v.AllowRestrictedIndices, Query: v.Query}
		if v.FieldSecurity != nil {
			index.FieldSecurity = &api.ElasticFieldSecurity{Grant: v.FieldSecurity.Grant, Except: v.FieldSecurity.Except}
		}
		out.Indices = append(out.Indices, index)
	}
	for _, v := range r.Applications {
		out.Applications = append(out.Applications, api.ElasticApplicationPrivileges{Application: v.Application, Privileges: nonNil(v.Privileges), Resources: nonNil(v.Resources)})
	}
	return out
}
func elasticExpiration(v int64) *int64 {
	if v == 0 {
		return nil
	}
	return &v
}

func (s *Server) elasticAccess(ctx context.Context, name string, in input) response {
	rc, p, err := s.context(ctx, clusterRef(ctx, in.Cluster))
	if err != nil {
		return s.failure(err)
	}
	access, yes := p.(elastic.Access)
	if !yes {
		return s.failure(engine.ErrNotSupported)
	}
	if failure := s.providerContext(rc); failure != nil {
		return *failure
	}
	switch name {
	case "ListElasticUsers":
		items, err := access.ListUsers(ctx, rc)
		if err != nil {
			return s.failure(err)
		}
		out := make([]api.ElasticUser, 0, len(items))
		for _, u := range items {
			out = append(out, elasticUserAPI(u))
		}
		return listResponse(s, ctx, out, nil)
	case "GetElasticUser":
		u, err := access.GetUser(ctx, rc, in.Username)
		if err != nil {
			return s.failure(err)
		}
		return ok(elasticUserAPI(*u))
	case "CreateElasticUser":
		b := in.Body.(*api.ElasticUserCreate)
		if utf8.RuneCountInString(value(b.Password)) < 8 {
			return invalidField("password", "out_of_range", "password must contain at least 8 characters")
		}
		u := elastic.UserWrite{User: elastic.User{Username: b.Username, Roles: b.Roles, FullName: value(b.FullName), Email: value(b.Email), Enabled: b.Enabled}, Password: value(b.Password)}
		if err := access.CreateUser(ctx, rc, u); err != nil {
			return s.failure(err)
		}
		return response{status: 201, body: elasticUserAPI(u.User)}
	case "UpdateElasticUser":
		b := in.Body.(*api.ElasticUserWrite)
		if b.Password != nil && utf8.RuneCountInString(*b.Password) < 8 {
			return invalidField("password", "out_of_range", "password must contain at least 8 characters")
		}
		if b.Username != in.Username {
			return invalidField("username", "conflict", "username must match the user named in the request path")
		}
		u := elastic.UserWrite{User: elastic.User{Username: in.Username, Roles: b.Roles, FullName: value(b.FullName), Email: value(b.Email), Enabled: b.Enabled}, Password: value(b.Password)}
		if err := access.UpdateUser(ctx, rc, u); err != nil {
			return s.failure(err)
		}
		return ok(elasticUserAPI(u.User))
	case "DeleteElasticUser":
		if err := access.DeleteUser(ctx, rc, in.Username); err != nil {
			return s.failure(err)
		}
		return response{status: 204}
	case "ListElasticRoles":
		items, err := access.ListRoles(ctx, rc)
		if err != nil {
			return s.failure(err)
		}
		out := make([]api.ElasticRole, 0, len(items))
		for _, r := range items {
			out = append(out, elasticRoleAPI(r))
		}
		return listResponse(s, ctx, out, nil)
	case "GetElasticRole":
		r, err := access.GetRole(ctx, rc, in.Role)
		if err != nil {
			return s.failure(err)
		}
		return ok(elasticRoleAPI(*r))
	case "CreateElasticRole", "UpdateElasticRole":
		b := in.Body.(*api.ElasticRoleInput)
		if name == "UpdateElasticRole" && b.Name != in.Role {
			return invalidField("name", "conflict", "name must match the role named in the request path")
		}
		r := elastic.AccessRole{Name: b.Name, Role: elasticDescriptorDomain(b.Cluster, b.Indices, b.Applications)}
		if err := access.PutRole(ctx, rc, r, name == "CreateElasticRole"); err != nil {
			return s.failure(err)
		}
		if name == "CreateElasticRole" {
			return response{status: 201, body: elasticRoleAPI(r)}
		}
		return ok(elasticRoleAPI(r))
	case "DeleteElasticRole":
		if err := access.DeleteRole(ctx, rc, in.Role); err != nil {
			return s.failure(err)
		}
		return response{status: 204}
	case "ListElasticAPIKeys":
		items, err := access.ListAPIKeys(ctx, rc)
		if err != nil {
			return s.failure(err)
		}
		out := make([]api.ElasticAPIKey, 0, len(items))
		for _, k := range items {
			out = append(out, api.ElasticAPIKey{Id: k.ID, Name: k.Name, Creation: k.Creation, Expiration: elasticExpiration(k.Expiration), Invalidated: k.Invalidated, Username: k.Username})
		}
		return listResponse(s, ctx, out, nil)
	case "CreateElasticAPIKey":
		b := in.Body.(*api.ElasticAPIKeyCreate)
		if len(b.RoleDescriptors) == 0 {
			return invalid()
		}
		req := elastic.APIKeyCreate{Name: b.Name, Expiration: value(b.Expiration), RoleDescriptors: make(map[string]elastic.RoleDescriptor, len(b.RoleDescriptors))}
		for name, r := range b.RoleDescriptors {
			req.RoleDescriptors[name] = elasticDescriptorDomain(r.Cluster, r.Indices, r.Applications)
		}
		k, err := access.CreateAPIKey(ctx, rc, req)
		if err != nil {
			return s.failure(err)
		}
		return response{status: 201, noStore: true, body: api.ElasticAPIKeyCreated{Id: k.ID, Name: k.Name, ApiKey: k.APIKey, Encoded: k.Encoded, Expiration: elasticExpiration(k.Expiration)}}
	case "InvalidateElasticAPIKey":
		if err := access.InvalidateAPIKey(ctx, rc, in.Token); err != nil {
			return s.failure(err)
		}
		return response{status: 204}
	}
	return fail(500, "internal_error", "Unknown Elasticsearch access operation")
}

// ListElasticUsers implements the generated strict API operation.
func (s *Server) ListElasticUsers(ctx context.Context, request api.ListElasticUsersRequestObject) (api.ListElasticUsersResponseObject, error) {
	return s.handle(ctx, "ListElasticUsers", input{Cluster: request.Cluster}), nil
}
func (r response) VisitListElasticUsersResponse(w http.ResponseWriter) error { return r.write(w) }

// CreateElasticUser implements the generated strict API operation.
func (s *Server) CreateElasticUser(ctx context.Context, request api.CreateElasticUserRequestObject) (api.CreateElasticUserResponseObject, error) {
	return s.handle(ctx, "CreateElasticUser", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitCreateElasticUserResponse(w http.ResponseWriter) error { return r.write(w) }

// GetElasticUser implements the generated strict API operation.
func (s *Server) GetElasticUser(ctx context.Context, request api.GetElasticUserRequestObject) (api.GetElasticUserResponseObject, error) {
	return s.handle(ctx, "GetElasticUser", input{Cluster: request.Cluster, Username: request.Username}), nil
}
func (r response) VisitGetElasticUserResponse(w http.ResponseWriter) error { return r.write(w) }

// UpdateElasticUser implements the generated strict API operation.
func (s *Server) UpdateElasticUser(ctx context.Context, request api.UpdateElasticUserRequestObject) (api.UpdateElasticUserResponseObject, error) {
	return s.handle(ctx, "UpdateElasticUser", input{Cluster: request.Cluster, Username: request.Username, Body: request.Body}), nil
}
func (r response) VisitUpdateElasticUserResponse(w http.ResponseWriter) error { return r.write(w) }

// DeleteElasticUser implements the generated strict API operation.
func (s *Server) DeleteElasticUser(ctx context.Context, request api.DeleteElasticUserRequestObject) (api.DeleteElasticUserResponseObject, error) {
	return s.handle(ctx, "DeleteElasticUser", input{Cluster: request.Cluster, Username: request.Username}), nil
}
func (r response) VisitDeleteElasticUserResponse(w http.ResponseWriter) error { return r.write(w) }

// ListElasticRoles implements the generated strict API operation.
func (s *Server) ListElasticRoles(ctx context.Context, request api.ListElasticRolesRequestObject) (api.ListElasticRolesResponseObject, error) {
	return s.handle(ctx, "ListElasticRoles", input{Cluster: request.Cluster}), nil
}
func (r response) VisitListElasticRolesResponse(w http.ResponseWriter) error { return r.write(w) }

// CreateElasticRole implements the generated strict API operation.
func (s *Server) CreateElasticRole(ctx context.Context, request api.CreateElasticRoleRequestObject) (api.CreateElasticRoleResponseObject, error) {
	return s.handle(ctx, "CreateElasticRole", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitCreateElasticRoleResponse(w http.ResponseWriter) error { return r.write(w) }

// GetElasticRole implements the generated strict API operation.
func (s *Server) GetElasticRole(ctx context.Context, request api.GetElasticRoleRequestObject) (api.GetElasticRoleResponseObject, error) {
	return s.handle(ctx, "GetElasticRole", input{Cluster: request.Cluster, Role: request.Role}), nil
}
func (r response) VisitGetElasticRoleResponse(w http.ResponseWriter) error { return r.write(w) }

// UpdateElasticRole implements the generated strict API operation.
func (s *Server) UpdateElasticRole(ctx context.Context, request api.UpdateElasticRoleRequestObject) (api.UpdateElasticRoleResponseObject, error) {
	return s.handle(ctx, "UpdateElasticRole", input{Cluster: request.Cluster, Role: request.Role, Body: request.Body}), nil
}
func (r response) VisitUpdateElasticRoleResponse(w http.ResponseWriter) error { return r.write(w) }

// DeleteElasticRole implements the generated strict API operation.
func (s *Server) DeleteElasticRole(ctx context.Context, request api.DeleteElasticRoleRequestObject) (api.DeleteElasticRoleResponseObject, error) {
	return s.handle(ctx, "DeleteElasticRole", input{Cluster: request.Cluster, Role: request.Role}), nil
}
func (r response) VisitDeleteElasticRoleResponse(w http.ResponseWriter) error { return r.write(w) }

// ListElasticAPIKeys implements the generated strict API operation.
func (s *Server) ListElasticAPIKeys(ctx context.Context, request api.ListElasticAPIKeysRequestObject) (api.ListElasticAPIKeysResponseObject, error) {
	return s.handle(ctx, "ListElasticAPIKeys", input{Cluster: request.Cluster}), nil
}
func (r response) VisitListElasticAPIKeysResponse(w http.ResponseWriter) error { return r.write(w) }

// CreateElasticAPIKey implements the generated strict API operation.
func (s *Server) CreateElasticAPIKey(ctx context.Context, request api.CreateElasticAPIKeyRequestObject) (api.CreateElasticAPIKeyResponseObject, error) {
	return s.handle(ctx, "CreateElasticAPIKey", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitCreateElasticAPIKeyResponse(w http.ResponseWriter) error { return r.write(w) }

// InvalidateElasticAPIKey implements the generated strict API operation.
func (s *Server) InvalidateElasticAPIKey(ctx context.Context, request api.InvalidateElasticAPIKeyRequestObject) (api.InvalidateElasticAPIKeyResponseObject, error) {
	// Finish revocation and its audit record even if the browser disconnects.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	return s.handle(ctx, "InvalidateElasticAPIKey", input{Cluster: request.Cluster, Token: request.Id}), nil
}
func (r response) VisitInvalidateElasticAPIKeyResponse(w http.ResponseWriter) error {
	return r.write(w)
}
