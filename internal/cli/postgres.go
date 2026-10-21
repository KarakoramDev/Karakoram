// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	apiclient "github.com/KarakoramDev/Karakoram/pkg/client"
)

func postgresCommandSpecs() []commandSpec {
	return []commandSpec{
		{"postgres bindings list <cluster>", "listPostgresBindings", 1, false, nil},
		{"postgres bindings rotate <cluster> <id>", "rotatePostgresBinding", 2, false, nil},
		{"postgres rebalance <cluster>", "rebalancePostgres", 1, true, nil},
		{"postgres roles list <cluster>", "listPostgresRoles", 1, false, nil},
		{"postgres roles get <cluster> <name>", "getPostgresRole", 2, false, nil},
		{"postgres roles create <cluster>", "createPostgresRole", 1, true, nil},
		{"postgres roles update <cluster> <name>", "updatePostgresRole", 2, true, nil},
		{"postgres roles patch <cluster> <name>", "patchPostgresRole", 2, true, nil},
		{"postgres roles delete <cluster> <name>", "deletePostgresRole", 2, false, nil},
		{"postgres roles rotate-password <cluster> <name>", "rotatePostgresRolePassword", 2, false, nil},
		{"postgres databases list <cluster>", "listPostgresDatabases", 1, false, nil},
		{"postgres databases get <cluster> <name>", "getPostgresDatabase", 2, false, nil},
		{"postgres databases create <cluster>", "createPostgresDatabase", 1, true, nil},
		{"postgres databases delete <cluster> <name>", "deletePostgresDatabase", 2, false, []queryFlag{{"force", "force", "bool"}}},
		{"postgres grants list <cluster>", "listPostgresGrants", 1, false, []queryFlag{{"database", "database", "string"}}},
		{"postgres grants grant <cluster>", "grantPostgresPrivileges", 1, true, nil},
		{"postgres grants revoke <cluster>", "revokePostgresPrivileges", 1, true, nil},
	}
}

func dispatchPostgres(ctx context.Context, c *apiclient.Client, op string, args []string, q, body []byte) (*http.Response, error) {
	switch op {
	case "listPostgresBindings":
		var p apiclient.ListPostgresBindingsParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListPostgresBindings(ctx, args[0], &p)
	case "rotatePostgresBinding":
		var p apiclient.RotatePostgresBindingParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.RotatePostgresBinding(ctx, args[0], args[1], &p)
	case "rebalancePostgres":
		var p apiclient.RebalancePostgresParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.RebalancePostgresWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "patchPostgresRole":
		var p apiclient.PatchPostgresRoleParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.PatchPostgresRoleWithBody(ctx, args[0], args[1], &p, "application/json", bytes.NewReader(body))
	case "listPostgresRoles":
		var p apiclient.ListPostgresRolesParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListPostgresRoles(ctx, args[0], &p)
	case "getPostgresRole":
		var p apiclient.GetPostgresRoleParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetPostgresRole(ctx, args[0], args[1], &p)
	case "createPostgresRole":
		var p apiclient.CreatePostgresRoleParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreatePostgresRoleWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "updatePostgresRole":
		var p apiclient.UpdatePostgresRoleParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.UpdatePostgresRoleWithBody(ctx, args[0], args[1], &p, "application/json", bytes.NewReader(body))
	case "deletePostgresRole":
		var p apiclient.DeletePostgresRoleParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeletePostgresRole(ctx, args[0], args[1], &p)
	case "rotatePostgresRolePassword":
		var p apiclient.RotatePostgresRolePasswordParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.RotatePostgresRolePassword(ctx, args[0], args[1], &p)
	case "listPostgresDatabases":
		var p apiclient.ListPostgresDatabasesParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListPostgresDatabases(ctx, args[0], &p)
	case "getPostgresDatabase":
		var p apiclient.GetPostgresDatabaseParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetPostgresDatabase(ctx, args[0], args[1], &p)
	case "createPostgresDatabase":
		var p apiclient.CreatePostgresDatabaseParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreatePostgresDatabaseWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "deletePostgresDatabase":
		var p apiclient.DeletePostgresDatabaseParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeletePostgresDatabase(ctx, args[0], args[1], &p)
	case "listPostgresGrants":
		var p apiclient.ListPostgresGrantsParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListPostgresGrants(ctx, args[0], &p)
	case "grantPostgresPrivileges":
		var p apiclient.GrantPostgresPrivilegesParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GrantPostgresPrivilegesWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "revokePostgresPrivileges":
		var p apiclient.RevokePostgresPrivilegesParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.RevokePostgresPrivilegesWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	}
	return nil, fmt.Errorf("unknown PostgreSQL API operation %q", op)
}
