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

func mysqlCommandSpecs() []commandSpec {
	return []commandSpec{
		{"mysql recover-quorum <cluster>", "recoverMySQLQuorum", 1, true, nil},
		{"mysql users list <cluster>", "listMySQLUsers", 1, false, nil},
		{"mysql users get <cluster> <name>", "getMySQLUser", 2, false, nil},
		{"mysql users create <cluster>", "createMySQLUser", 1, true, nil},
		{"mysql users update <cluster> <name>", "patchMySQLUser", 2, true, nil},
		{"mysql users delete <cluster> <name>", "deleteMySQLUser", 2, false, nil},
		{"mysql users rotate-password <cluster> <name>", "rotateMySQLUserPassword", 2, false, nil},
		{"mysql users discard-old-password <cluster> <name>", "discardMySQLUserOldPassword", 2, false, nil},
		{"mysql databases list <cluster>", "listMySQLDatabases", 1, false, nil},
		{"mysql databases get <cluster> <name>", "getMySQLDatabase", 2, false, nil},
		{"mysql databases create <cluster>", "createMySQLDatabase", 1, true, nil},
		{"mysql databases delete <cluster> <name>", "deleteMySQLDatabase", 2, false, []queryFlag{{"force", "force", "bool"}}},
		{"mysql grants list <cluster>", "listMySQLGrants", 1, false, []queryFlag{{"user", "user", "string"}, {"database", "database", "string"}}},
		{"mysql grants grant <cluster>", "grantMySQLPrivileges", 1, true, nil},
		{"mysql grants revoke <cluster>", "revokeMySQLPrivileges", 1, true, nil},
	}
}

func dispatchMySQL(ctx context.Context, c *apiclient.Client, op string, args []string, q, body []byte) (*http.Response, error) {
	switch op {
	case "recoverMySQLQuorum":
		var p apiclient.RecoverMySQLQuorumParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.RecoverMySQLQuorumWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "listMySQLUsers":
		var p apiclient.ListMySQLUsersParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListMySQLUsers(ctx, args[0], &p)
	case "getMySQLUser":
		var p apiclient.GetMySQLUserParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetMySQLUser(ctx, args[0], args[1], &p)
	case "createMySQLUser":
		var p apiclient.CreateMySQLUserParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateMySQLUserWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "patchMySQLUser":
		var p apiclient.PatchMySQLUserParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.PatchMySQLUserWithBody(ctx, args[0], args[1], &p, "application/json", bytes.NewReader(body))
	case "deleteMySQLUser":
		var p apiclient.DeleteMySQLUserParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeleteMySQLUser(ctx, args[0], args[1], &p)
	case "rotateMySQLUserPassword":
		var p apiclient.RotateMySQLUserPasswordParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.RotateMySQLUserPasswordWithBody(ctx, args[0], args[1], &p, "application/json", bytes.NewReader(body))
	case "discardMySQLUserOldPassword":
		var p apiclient.DiscardMySQLUserOldPasswordParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DiscardMySQLUserOldPassword(ctx, args[0], args[1], &p)
	case "listMySQLDatabases":
		var p apiclient.ListMySQLDatabasesParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListMySQLDatabases(ctx, args[0], &p)
	case "getMySQLDatabase":
		var p apiclient.GetMySQLDatabaseParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetMySQLDatabase(ctx, args[0], args[1], &p)
	case "createMySQLDatabase":
		var p apiclient.CreateMySQLDatabaseParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateMySQLDatabaseWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "deleteMySQLDatabase":
		var p apiclient.DeleteMySQLDatabaseParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeleteMySQLDatabase(ctx, args[0], args[1], &p)
	case "listMySQLGrants":
		var p apiclient.ListMySQLGrantsParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListMySQLGrants(ctx, args[0], &p)
	case "grantMySQLPrivileges":
		var p apiclient.GrantMySQLPrivilegesParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GrantMySQLPrivilegesWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "revokeMySQLPrivileges":
		var p apiclient.RevokeMySQLPrivilegesParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.RevokeMySQLPrivilegesWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	}
	return nil, fmt.Errorf("unknown MySQL API operation %q", op)
}
