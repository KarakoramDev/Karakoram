// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func (s *Server) database(ctx context.Context, name string, in input, rc *engine.Context, dp engine.DataPlane) response {
	switch name {
	case "ListSystemUsers":
		items, err := dp.SystemUsers(ctx, rc)
		out := make([]api.SystemUser, 0, len(items))
		for _, u := range items {
			out = append(out, api.SystemUser{Name: api.SystemUserName(u.Name), Username: u.Name, Roles: rolesAPI(u.Roles)})
		}
		if err != nil {
			return s.failure(err)
		}
		return ok(page[api.SystemUser]{Items: out})
	case "ListDatabaseUsers":
		items, err := dp.ListDatabaseUsers(ctx, rc)
		out := make([]api.DatabaseUser, 0, len(items))
		for _, u := range items {
			out = append(out, userAPI(u))
		}
		return listResponse(s, ctx, out, err)
	case "GetDatabaseUser":
		u, err := dp.GetDatabaseUser(ctx, rc, in.Database, in.Username)
		if err != nil {
			return s.failure(err)
		}
		return ok(userAPI(*u))
	case "CreateDatabaseUser":
		b := in.Body.(*api.CreateDatabaseUserRequest)
		u := domain.DatabaseUser{Name: b.Username, Database: b.Database, Roles: rolesDomain(b.Roles)}
		password, err := dp.CreateDatabaseUser(ctx, rc, u, value(b.Password))
		if err != nil {
			return s.failure(err)
		}
		a := api.CreatedDatabaseUser{Username: u.Name, Database: u.Database, Roles: rolesAPI(u.Roles)}
		if value(b.Password) == "" {
			a.GeneratedPassword = &password
		}
		return response{status: 201, body: a, noStore: true}
	case "ReplaceDatabaseUser":
		b := in.Body.(*api.UpdateDatabaseUserRequest)
		u := domain.DatabaseUser{Name: in.Username, Database: in.Database, Roles: rolesDomain(b.Roles)}
		if err := dp.UpdateDatabaseUser(ctx, rc, u, value(b.Password)); err != nil {
			return s.failure(err)
		}
		return response{status: 200, body: userAPI(u), noStore: true}
	case "DeleteDatabaseUser":
		if err := dp.DeleteDatabaseUser(ctx, rc, in.Database, in.Username); err != nil {
			return s.failure(err)
		}
		return response{status: 204}
	case "ListDatabaseRoles":
		items, err := dp.ListDatabaseRoles(ctx, rc)
		out := make([]api.DatabaseRole, 0, len(items))
		for _, r := range items {
			out = append(out, roleAPI(r))
		}
		return listResponse(s, ctx, out, err)
	case "GetDatabaseRole":
		r, err := dp.GetDatabaseRole(ctx, rc, in.Database, in.Role)
		if err != nil {
			return s.failure(err)
		}
		return ok(roleAPI(*r))
	case "CreateDatabaseRole":
		b := in.Body.(*api.DatabaseRole)
		r := domain.DatabaseRole{Name: b.Name, Database: b.Database, Roles: rolesDomain(b.Roles), Privileges: privilegesDomain(b.Privileges)}
		if err := dp.CreateDatabaseRole(ctx, rc, r); err != nil {
			return s.failure(err)
		}
		return response{status: 201, body: roleAPI(r)}
	case "ReplaceDatabaseRole":
		b := in.Body.(*api.UpdateDatabaseRoleRequest)
		r := domain.DatabaseRole{Name: in.Role, Database: in.Database, Roles: rolesDomain(b.Roles), Privileges: privilegesDomain(b.Privileges)}
		if err := dp.UpdateDatabaseRole(ctx, rc, r); err != nil {
			return s.failure(err)
		}
		return ok(roleAPI(r))
	case "DeleteDatabaseRole":
		if err := dp.DeleteDatabaseRole(ctx, rc, in.Database, in.Role); err != nil {
			return s.failure(err)
		}
		return response{status: 204}
	}
	return fail(500, "internal_error", "Unknown cluster operation")
}
