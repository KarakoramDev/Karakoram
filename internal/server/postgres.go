// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
)

func postgresRole(v api.PostgresRoleWrite) postgres.RoleWrite {
	out := postgres.RoleWrite{Role: postgres.Role{Name: v.Name, Login: true, ConnectionLimit: -1, MemberOf: []string{}}, Password: v.Password}
	applyPostgresRole(&out, api.PostgresRolePatch{Login: v.Login, Superuser: v.Superuser, Createdb: v.Createdb, Createrole: v.Createrole, Replication: v.Replication, Bypassrls: v.Bypassrls, ConnectionLimit: v.ConnectionLimit, MemberOf: v.MemberOf, ValidUntil: v.ValidUntil})
	return out
}

func applyPostgresRole(out *postgres.RoleWrite, v api.PostgresRolePatch) {
	if v.Login != nil {
		out.Login = *v.Login
	}
	if v.Superuser != nil {
		out.Superuser = *v.Superuser
	}
	if v.Createdb != nil {
		out.Createdb = *v.Createdb
	}
	if v.Createrole != nil {
		out.Createrole = *v.Createrole
	}
	if v.Replication != nil {
		out.Replication = *v.Replication
	}
	if v.Bypassrls != nil {
		out.BypassRLS = *v.Bypassrls
	}
	if v.ConnectionLimit != nil {
		out.ConnectionLimit = *v.ConnectionLimit
	}
	if v.MemberOf != nil {
		out.MemberOf = *v.MemberOf
		// Public patches cannot grant admin options; removing a membership
		// also removes any internal delegation attached to that membership.
		admin := out.MemberOfAdmin[:0]
		for _, parent := range out.MemberOfAdmin {
			for _, member := range out.MemberOf {
				if parent == member {
					admin = append(admin, parent)
					break
				}
			}
		}
		out.MemberOfAdmin = admin
	}
	if v.ValidUntil != nil {
		until := v.ValidUntil.Format(time.RFC3339)
		out.ValidUntil = &until
	}
	if v.Password != nil {
		out.Password = v.Password
	}
}

func postgresGrant(v api.PostgresGrant) postgres.Grant {
	privileges := make([]string, len(v.Privileges))
	for i, privilege := range v.Privileges {
		privileges[i] = string(privilege)
	}
	return postgres.Grant{Role: v.Role, Database: v.Database, Schema: value(v.Schema), Privileges: privileges}
}

func privilegedPostgresRole(v postgres.Role) bool {
	return v.Superuser || v.Replication || v.BypassRLS || len(v.MemberOfAdmin) != 0
}

// Keep internal delegation metadata off the unchanged public role schema.
func publicPostgresRole(role postgres.Role) postgres.Role {
	role.MemberOfAdmin = nil
	return role
}

// Check the effective role graph as well as requested flags: managing the
// password of an existing privileged role or granting its membership is also an
// administrative operation, not a way around the explicit privileged flags.
func (s *Server) postgresRoleAllowed(ctx context.Context, name string, rc *engine.Context, access postgres.Access, desired postgres.Role) (bool, error) {
	rule, exists := api.OperationAdditionalRBAC[operationID(name)]
	admin := exists && !rule.Public && rule.ObjectParam == "cluster" && s.allowed(ctx, rule.Resource, rule.Action, clusterObject(rc.Cluster.Namespace, rc.Cluster.Name))
	if admin {
		return true, nil
	}
	roles, err := access.ListRoles(ctx, rc)
	if err != nil {
		return false, err
	}
	byName := make(map[string]postgres.Role, len(roles))
	for _, role := range roles {
		byName[role.Name] = role
	}
	if privilegedPostgresRole(desired) || privilegedPostgresRole(byName[desired.Name]) {
		return false, nil
	}
	seen := map[string]bool{}
	var privileged func(postgres.Role) bool
	privileged = func(role postgres.Role) bool {
		if privilegedPostgresRole(role) {
			return true
		}
		for _, parent := range role.MemberOf {
			if seen[parent] {
				continue
			}
			seen[parent] = true
			parentRole, visible := byName[parent]
			if !visible {
				switch parent {
				case "pg_read_all_data", "pg_write_all_data", "pg_monitor", "pg_read_all_stats", "pg_signal_backend":
					continue
				default:
					// Hidden system memberships must never become an escalation
					// path through an otherwise unprivileged application role.
					return true
				}
			}
			if privileged(parentRole) {
				return true
			}
		}
		return false
	}
	return !privileged(desired), nil
}

func (s *Server) postgres(ctx context.Context, name string, in input, rc *engine.Context, provider engine.Provider) response {
	access, supported := provider.(postgres.Access)
	if !supported {
		return s.unsupportedClusterAction(name, provider)
	}
	changed := func(status int, body any, noStore bool) response {
		s.trigger(ctx, rc.Cluster.Ref())
		return response{status: status, body: body, noStore: noStore}
	}
	switch name {
	case "ListPostgresBindings":
		bindings, err := s.Store.ListPostgresBindings(ctx, rc.Cluster.Ref())
		out := make([]api.PostgresBinding, 0, len(bindings))
		for _, binding := range bindings {
			out = append(out, postgresBindingAPI(binding))
		}
		return listResponse(s, ctx, out, err)
	case "RotatePostgresBinding":
		binding, err := s.Store.GetPostgresBinding(ctx, in.Name)
		if err != nil {
			return s.failure(err)
		}
		if binding.SourceRef() != rc.Cluster.Ref() || binding.SourceUID != strconv.FormatInt(rc.Cluster.CreatedAt.UnixNano(), 10) {
			return fail(404, "not_found", "binding not found on this source")
		}
		if _, err := s.postgresBindings.Rotate(ctx, binding.ID); err != nil {
			return s.failure(err)
		}
		binding, err = s.Store.GetPostgresBinding(ctx, binding.ID)
		if err != nil {
			return s.failure(err)
		}
		return ok(postgresBindingAPI(*binding))
	case "ListPostgresRoles":
		roles, err := access.ListRoles(ctx, rc)
		if err != nil {
			return s.failure(err)
		}
		for i := range roles {
			roles[i] = publicPostgresRole(roles[i])
		}
		return ok(page[postgres.Role]{Items: nonNil(roles)})
	case "GetPostgresRole":
		role, err := access.GetRole(ctx, rc, in.Role)
		if err != nil {
			return s.failure(err)
		}
		return ok(publicPostgresRole(*role))
	case "CreatePostgresRole", "UpdatePostgresRole", "PatchPostgresRole":
		var role postgres.RoleWrite
		if name == "PatchPostgresRole" {
			current, err := access.GetRole(ctx, rc, in.Role)
			if err != nil {
				return s.failure(err)
			}
			role.Role = *current
			applyPostgresRole(&role, *in.Body.(*api.PostgresRolePatch))
		} else {
			role = postgresRole(*in.Body.(*api.PostgresRoleWrite))
			if name != "CreatePostgresRole" && role.Name != in.Role {
				return invalidField("name", domain.ReasonInvalid, "name must match the path parameter")
			}
		}
		allowed, err := s.postgresRoleAllowed(ctx, name, rc, access, role.Role)
		if err != nil {
			return s.failure(err)
		}
		if !allowed {
			return fail(403, "forbidden", "Privileged PostgreSQL roles and memberships require clusters/update permission")
		}
		if name == "CreatePostgresRole" {
			password, err := access.CreateRole(ctx, rc, role)
			if err != nil {
				return s.failure(err)
			}
			out := struct {
				postgres.Role
				GeneratedPassword *string `json:"generatedPassword,omitempty"`
			}{Role: publicPostgresRole(role.Role)}
			if password != "" {
				out.GeneratedPassword = &password
			}
			return changed(201, out, true)
		}
		if err := access.UpdateRole(ctx, rc, role); err != nil {
			return s.failure(err)
		}
		return changed(200, publicPostgresRole(role.Role), true)
	case "DeletePostgresRole":
		if err := access.DeleteRole(ctx, rc, in.Role); err != nil {
			return s.failure(err)
		}
		return changed(204, nil, false)
	case "RotatePostgresRolePassword":
		role, err := access.GetRole(ctx, rc, in.Role)
		if err != nil {
			return s.failure(err)
		}
		allowed, err := s.postgresRoleAllowed(ctx, name, rc, access, *role)
		if err != nil {
			return s.failure(err)
		}
		if !allowed {
			return fail(403, "forbidden", "Privileged PostgreSQL role passwords require clusters/update permission")
		}
		password, err := access.RotateRolePassword(ctx, rc, in.Role)
		if err != nil {
			return s.failure(err)
		}
		return changed(200, map[string]string{"generatedPassword": password}, true)
	case "ListPostgresDatabases":
		databases, err := access.ListDatabases(ctx, rc)
		if err != nil {
			return s.failure(err)
		}
		return ok(page[postgres.Database]{Items: nonNil(databases)})
	case "GetPostgresDatabase":
		database, err := access.GetDatabase(ctx, rc, in.Database)
		if err != nil {
			return s.failure(err)
		}
		return ok(database)
	case "CreatePostgresDatabase":
		body := in.Body.(*api.PostgresDatabase)
		database := postgres.Database{Name: body.Name, Owner: body.Owner, Encoding: "UTF8", Locale: "C.UTF-8", AllowConnections: true}
		if body.Extensions != nil {
			database.Extensions = *body.Extensions
		}
		if body.Encoding != nil {
			database.Encoding = *body.Encoding
		}
		if body.Locale != nil {
			database.Locale = *body.Locale
		}
		if body.AllowConnections != nil {
			database.AllowConnections = *body.AllowConnections
		}
		if err := access.CreateDatabase(ctx, rc, database); err != nil {
			return s.failure(err)
		}
		return changed(201, database, false)
	case "DeletePostgresDatabase":
		if err := access.DeleteDatabase(ctx, rc, in.Database, request(ctx).URL.Query().Get("force") == "true"); err != nil {
			return s.failure(err)
		}
		return changed(204, nil, false)
	case "ListPostgresGrants":
		grants, err := access.ListGrants(ctx, rc, in.Database)
		if err != nil {
			return s.failure(err)
		}
		return ok(page[postgres.Grant]{Items: nonNil(grants)})
	case "GrantPostgresPrivileges", "RevokePostgresPrivileges":
		grant := postgresGrant(*in.Body.(*api.PostgresGrant))
		var err error
		if name == "GrantPostgresPrivileges" {
			err = access.Grant(ctx, rc, grant)
		} else {
			err = access.Revoke(ctx, rc, grant)
		}
		if err != nil {
			return s.failure(err)
		}
		return changed(204, nil, false)
	}
	return s.unsupportedClusterAction(name, provider)
}

func (s *Server) ListPostgresRoles(ctx context.Context, r api.ListPostgresRolesRequestObject) (api.ListPostgresRolesResponseObject, error) {
	return s.handle(ctx, "ListPostgresRoles", input{Cluster: r.Cluster}), nil
}
func (r response) VisitListPostgresRolesResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) GetPostgresRole(ctx context.Context, r api.GetPostgresRoleRequestObject) (api.GetPostgresRoleResponseObject, error) {
	return s.handle(ctx, "GetPostgresRole", input{Cluster: r.Cluster, Role: r.Role}), nil
}
func (r response) VisitGetPostgresRoleResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) CreatePostgresRole(ctx context.Context, r api.CreatePostgresRoleRequestObject) (api.CreatePostgresRoleResponseObject, error) {
	return s.handle(ctx, "CreatePostgresRole", input{Cluster: r.Cluster, Body: r.Body}), nil
}
func (r response) VisitCreatePostgresRoleResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) UpdatePostgresRole(ctx context.Context, r api.UpdatePostgresRoleRequestObject) (api.UpdatePostgresRoleResponseObject, error) {
	return s.handle(ctx, "UpdatePostgresRole", input{Cluster: r.Cluster, Role: r.Role, Body: r.Body}), nil
}
func (r response) VisitUpdatePostgresRoleResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) PatchPostgresRole(ctx context.Context, r api.PatchPostgresRoleRequestObject) (api.PatchPostgresRoleResponseObject, error) {
	return s.handle(ctx, "PatchPostgresRole", input{Cluster: r.Cluster, Role: r.Role, Body: r.Body}), nil
}
func (r response) VisitPatchPostgresRoleResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) DeletePostgresRole(ctx context.Context, r api.DeletePostgresRoleRequestObject) (api.DeletePostgresRoleResponseObject, error) {
	return s.handle(ctx, "DeletePostgresRole", input{Cluster: r.Cluster, Role: r.Role}), nil
}
func (r response) VisitDeletePostgresRoleResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) RotatePostgresRolePassword(ctx context.Context, r api.RotatePostgresRolePasswordRequestObject) (api.RotatePostgresRolePasswordResponseObject, error) {
	return s.handle(ctx, "RotatePostgresRolePassword", input{Cluster: r.Cluster, Role: r.Role}), nil
}
func (r response) VisitRotatePostgresRolePasswordResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) ListPostgresDatabases(ctx context.Context, r api.ListPostgresDatabasesRequestObject) (api.ListPostgresDatabasesResponseObject, error) {
	return s.handle(ctx, "ListPostgresDatabases", input{Cluster: r.Cluster}), nil
}
func (r response) VisitListPostgresDatabasesResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) GetPostgresDatabase(ctx context.Context, r api.GetPostgresDatabaseRequestObject) (api.GetPostgresDatabaseResponseObject, error) {
	return s.handle(ctx, "GetPostgresDatabase", input{Cluster: r.Cluster, Database: r.Database}), nil
}
func (r response) VisitGetPostgresDatabaseResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) CreatePostgresDatabase(ctx context.Context, r api.CreatePostgresDatabaseRequestObject) (api.CreatePostgresDatabaseResponseObject, error) {
	return s.handle(ctx, "CreatePostgresDatabase", input{Cluster: r.Cluster, Body: r.Body}), nil
}
func (r response) VisitCreatePostgresDatabaseResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) DeletePostgresDatabase(ctx context.Context, r api.DeletePostgresDatabaseRequestObject) (api.DeletePostgresDatabaseResponseObject, error) {
	return s.handle(ctx, "DeletePostgresDatabase", input{Cluster: r.Cluster, Database: r.Database}), nil
}
func (r response) VisitDeletePostgresDatabaseResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) ListPostgresGrants(ctx context.Context, r api.ListPostgresGrantsRequestObject) (api.ListPostgresGrantsResponseObject, error) {
	return s.handle(ctx, "ListPostgresGrants", input{Cluster: r.Cluster, Database: r.Params.Database}), nil
}
func (r response) VisitListPostgresGrantsResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) GrantPostgresPrivileges(ctx context.Context, r api.GrantPostgresPrivilegesRequestObject) (api.GrantPostgresPrivilegesResponseObject, error) {
	return s.handle(ctx, "GrantPostgresPrivileges", input{Cluster: r.Cluster, Body: r.Body}), nil
}
func (r response) VisitGrantPostgresPrivilegesResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) RevokePostgresPrivileges(ctx context.Context, r api.RevokePostgresPrivilegesRequestObject) (api.RevokePostgresPrivilegesResponseObject, error) {
	return s.handle(ctx, "RevokePostgresPrivileges", input{Cluster: r.Cluster, Body: r.Body}), nil
}
func (r response) VisitRevokePostgresPrivilegesResponse(w http.ResponseWriter) error {
	return r.write(w)
}

func postgresBindingAPI(b domain.PostgresBinding) api.PostgresBinding {
	out := api.PostgresBinding{
		Id: b.ID, Databases: nonNil(b.Databases), Extensions: nonNil(b.Extensions),
		Role: b.RoleName, CredentialGeneration: b.CredentialGeneration, State: b.State,
		CreatedAt: b.CreatedAt, UpdatedAt: b.UpdatedAt,
	}
	out.Consumer.Kind, out.Consumer.Namespace, out.Consumer.Name = b.ConsumerKind, b.ConsumerNamespace, b.ConsumerName
	if b.MinMajor != 0 {
		out.MinMajor = ptr(b.MinMajor)
	}
	if b.MaxMajor != 0 {
		out.MaxMajor = ptr(b.MaxMajor)
	}
	return out
}

func (s *Server) ListPostgresBindings(ctx context.Context, r api.ListPostgresBindingsRequestObject) (api.ListPostgresBindingsResponseObject, error) {
	return s.handle(ctx, "ListPostgresBindings", input{Cluster: r.Cluster}), nil
}
func (r response) VisitListPostgresBindingsResponse(w http.ResponseWriter) error { return r.write(w) }

func (s *Server) RotatePostgresBinding(ctx context.Context, r api.RotatePostgresBindingRequestObject) (api.RotatePostgresBindingResponseObject, error) {
	return s.handle(ctx, "RotatePostgresBinding", input{Cluster: r.Cluster, Name: r.Binding}), nil
}
func (r response) VisitRotatePostgresBindingResponse(w http.ResponseWriter) error { return r.write(w) }
