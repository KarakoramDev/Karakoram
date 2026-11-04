// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"net/http"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func (s *Server) mysql(ctx context.Context, name string, in input, rc *engine.Context, provider engine.Provider) response {
	access, supported := provider.(mysql.Access)
	if !supported {
		return s.failure(engine.ErrNotSupported)
	}
	changed := func(status int, body any, noStore bool) response {
		s.trigger(ctx, rc.Cluster.Ref())
		return response{status: status, body: body, noStore: noStore}
	}
	switch name {
	case "RecoverMySQLQuorum":
		body := in.Body.(*api.MySQLQuorumRecoveryRequest)
		if string(body.Confirmation) != myspec.GaleraForcePrimaryConfirmation {
			return invalidField("confirmation", domain.ReasonRequired, "explicit absent-member fencing confirmation is required")
		}
		if rc.Cluster.Deleting {
			return s.failure(engine.ErrConflict)
		}
		return s.enqueue(ctx, rc.Cluster.Ref(), "recover-quorum", map[string]string{"confirmation": string(body.Confirmation)})
	case "ListMySQLUsers":
		users, err := access.ListUsers(ctx, rc)
		if err != nil {
			return s.failure(err)
		}
		return ok(page[mysql.User]{Items: nonNil(users)})
	case "GetMySQLUser":
		user, err := access.GetUser(ctx, rc, in.User)
		if err != nil {
			return s.failure(err)
		}
		return ok(user)
	case "CreateMySQLUser":
		body := in.Body.(*api.MySQLUserWrite)
		user := mysql.UserWrite{Name: body.Name, Password: body.Password}
		if body.MaxUserConnections != nil {
			user.MaxUserConnections = *body.MaxUserConnections
		}
		if body.Locked != nil {
			user.Locked = *body.Locked
		}
		password, err := access.CreateUser(ctx, rc, user)
		if err != nil {
			return s.failure(err)
		}
		out := struct {
			mysql.User
			GeneratedPassword *string `json:"generatedPassword,omitempty"`
		}{User: mysql.User{Name: user.Name, Host: "%", MaxUserConnections: user.MaxUserConnections, Locked: user.Locked, RoutableThroughProxy: true}}
		if password != "" {
			out.GeneratedPassword = &password
		}
		return changed(201, out, true)
	case "PatchMySQLUser":
		body := in.Body.(*api.MySQLUserPatch)
		if err := access.PatchUser(ctx, rc, in.User, mysql.UserPatch{MaxUserConnections: body.MaxUserConnections, Locked: body.Locked}); err != nil {
			return s.failure(err)
		}
		user, err := access.GetUser(ctx, rc, in.User)
		if err != nil {
			return s.failure(err)
		}
		return changed(200, user, false)
	case "DeleteMySQLUser":
		if err := access.DeleteUser(ctx, rc, in.User); err != nil {
			return s.failure(err)
		}
		return changed(204, nil, false)
	case "RotateMySQLUserPassword":
		body := in.Body.(*api.RotateMySQLUserPasswordJSONRequestBody)
		retain := body.RetainCurrent != nil && *body.RetainCurrent
		password, err := access.RotateUserPassword(ctx, rc, in.User, retain)
		if err != nil {
			return s.failure(err)
		}
		return changed(200, map[string]string{"generatedPassword": password}, true)
	case "DiscardMySQLUserOldPassword":
		if err := access.DiscardOldPassword(ctx, rc, in.User); err != nil {
			return s.failure(err)
		}
		return changed(204, nil, false)
	case "ListMySQLDatabases":
		databases, err := access.ListDatabases(ctx, rc)
		if err != nil {
			return s.failure(err)
		}
		return ok(page[mysql.Database]{Items: nonNil(databases)})
	case "GetMySQLDatabase":
		database, err := access.GetDatabase(ctx, rc, in.Database)
		if err != nil {
			return s.failure(err)
		}
		return ok(database)
	case "CreateMySQLDatabase":
		body := in.Body.(*api.MySQLDatabaseWrite)
		database := mysql.Database{Name: body.Name, CharacterSet: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}
		if body.CharacterSet != nil {
			database.CharacterSet = *body.CharacterSet
		}
		if body.Collation != nil {
			database.Collation = *body.Collation
		}
		if err := access.CreateDatabase(ctx, rc, database); err != nil {
			return s.failure(err)
		}
		return changed(201, database, false)
	case "DeleteMySQLDatabase":
		if err := access.DeleteDatabase(ctx, rc, in.Database, request(ctx).URL.Query().Get("force") == "true"); err != nil {
			return s.failure(err)
		}
		return changed(204, nil, false)
	case "ListMySQLGrants":
		if in.User == "" && in.Database == "" {
			return invalidField("user", domain.ReasonInvalid, "at least one of user or database is required")
		}
		grants, err := access.ListGrants(ctx, rc, in.User, in.Database)
		if err != nil {
			return s.failure(err)
		}
		return ok(page[mysql.Grant]{Items: nonNil(grants)})
	case "GrantMySQLPrivileges", "RevokeMySQLPrivileges":
		body := in.Body.(*api.MySQLGrant)
		grant := mysql.Grant{User: body.User, Database: body.Database, Privileges: make([]string, len(body.Privileges))}
		for i, privilege := range body.Privileges {
			grant.Privileges[i] = string(privilege)
		}
		var err error
		if name == "GrantMySQLPrivileges" {
			err = access.Grant(ctx, rc, grant)
		} else {
			err = access.Revoke(ctx, rc, grant)
		}
		if err != nil {
			return s.failure(err)
		}
		return changed(204, nil, false)
	}
	return s.failure(engine.ErrNotSupported)
}

func (s *Server) ListMySQLUsers(ctx context.Context, r api.ListMySQLUsersRequestObject) (api.ListMySQLUsersResponseObject, error) {
	return s.handle(ctx, "ListMySQLUsers", input{Cluster: r.Cluster}), nil
}
func (r response) VisitListMySQLUsersResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) GetMySQLUser(ctx context.Context, r api.GetMySQLUserRequestObject) (api.GetMySQLUserResponseObject, error) {
	return s.handle(ctx, "GetMySQLUser", input{Cluster: r.Cluster, User: r.User}), nil
}
func (r response) VisitGetMySQLUserResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) CreateMySQLUser(ctx context.Context, r api.CreateMySQLUserRequestObject) (api.CreateMySQLUserResponseObject, error) {
	return s.handle(ctx, "CreateMySQLUser", input{Cluster: r.Cluster, Body: r.Body}), nil
}
func (r response) VisitCreateMySQLUserResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) PatchMySQLUser(ctx context.Context, r api.PatchMySQLUserRequestObject) (api.PatchMySQLUserResponseObject, error) {
	return s.handle(ctx, "PatchMySQLUser", input{Cluster: r.Cluster, User: r.User, Body: r.Body}), nil
}
func (r response) VisitPatchMySQLUserResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) DeleteMySQLUser(ctx context.Context, r api.DeleteMySQLUserRequestObject) (api.DeleteMySQLUserResponseObject, error) {
	return s.handle(ctx, "DeleteMySQLUser", input{Cluster: r.Cluster, User: r.User}), nil
}
func (r response) VisitDeleteMySQLUserResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) RotateMySQLUserPassword(ctx context.Context, r api.RotateMySQLUserPasswordRequestObject) (api.RotateMySQLUserPasswordResponseObject, error) {
	return s.handle(ctx, "RotateMySQLUserPassword", input{Cluster: r.Cluster, User: r.User, Body: r.Body}), nil
}
func (r response) VisitRotateMySQLUserPasswordResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) DiscardMySQLUserOldPassword(ctx context.Context, r api.DiscardMySQLUserOldPasswordRequestObject) (api.DiscardMySQLUserOldPasswordResponseObject, error) {
	return s.handle(ctx, "DiscardMySQLUserOldPassword", input{Cluster: r.Cluster, User: r.User}), nil
}
func (r response) VisitDiscardMySQLUserOldPasswordResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) ListMySQLDatabases(ctx context.Context, r api.ListMySQLDatabasesRequestObject) (api.ListMySQLDatabasesResponseObject, error) {
	return s.handle(ctx, "ListMySQLDatabases", input{Cluster: r.Cluster}), nil
}
func (r response) VisitListMySQLDatabasesResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) GetMySQLDatabase(ctx context.Context, r api.GetMySQLDatabaseRequestObject) (api.GetMySQLDatabaseResponseObject, error) {
	return s.handle(ctx, "GetMySQLDatabase", input{Cluster: r.Cluster, Database: r.Database}), nil
}
func (r response) VisitGetMySQLDatabaseResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) CreateMySQLDatabase(ctx context.Context, r api.CreateMySQLDatabaseRequestObject) (api.CreateMySQLDatabaseResponseObject, error) {
	return s.handle(ctx, "CreateMySQLDatabase", input{Cluster: r.Cluster, Body: r.Body}), nil
}
func (r response) VisitCreateMySQLDatabaseResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) DeleteMySQLDatabase(ctx context.Context, r api.DeleteMySQLDatabaseRequestObject) (api.DeleteMySQLDatabaseResponseObject, error) {
	return s.handle(ctx, "DeleteMySQLDatabase", input{Cluster: r.Cluster, Database: r.Database}), nil
}
func (r response) VisitDeleteMySQLDatabaseResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) ListMySQLGrants(ctx context.Context, r api.ListMySQLGrantsRequestObject) (api.ListMySQLGrantsResponseObject, error) {
	return s.handle(ctx, "ListMySQLGrants", input{Cluster: r.Cluster, User: value(r.Params.User), Database: value(r.Params.Database)}), nil
}
func (r response) VisitListMySQLGrantsResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) GrantMySQLPrivileges(ctx context.Context, r api.GrantMySQLPrivilegesRequestObject) (api.GrantMySQLPrivilegesResponseObject, error) {
	return s.handle(ctx, "GrantMySQLPrivileges", input{Cluster: r.Cluster, Body: r.Body}), nil
}
func (r response) VisitGrantMySQLPrivilegesResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) RevokeMySQLPrivileges(ctx context.Context, r api.RevokeMySQLPrivilegesRequestObject) (api.RevokeMySQLPrivilegesResponseObject, error) {
	return s.handle(ctx, "RevokeMySQLPrivileges", input{Cluster: r.Cluster, Body: r.Body}), nil
}
func (r response) VisitRevokeMySQLPrivilegesResponse(w http.ResponseWriter) error { return r.write(w) }

func (s *Server) RecoverMySQLQuorum(ctx context.Context, r api.RecoverMySQLQuorumRequestObject) (api.RecoverMySQLQuorumResponseObject, error) {
	return s.handle(ctx, "RecoverMySQLQuorum", input{Cluster: r.Cluster, Body: r.Body}), nil
}
func (r response) VisitRecoverMySQLQuorumResponse(w http.ResponseWriter) error { return r.write(w) }
