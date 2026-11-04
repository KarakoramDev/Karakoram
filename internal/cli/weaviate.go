// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	apiclient "github.com/KarakoramDev/Karakoram/pkg/client"
	"github.com/spf13/cobra"
	"net/http"
)

func weaviateCommandSpecs() []commandSpec {
	return []commandSpec{
		{"weaviate users list <cluster>", "listWeaviateUsers", 1, false, nil},
		{"weaviate users create <cluster> <name>", "createWeaviateUser", 2, true, nil},
		{"weaviate users get <cluster> <name>", "getWeaviateUser", 2, false, nil},
		{"weaviate users update <cluster> <name>", "updateWeaviateUser", 2, true, nil},
		{"weaviate users delete <cluster> <name>", "deleteWeaviateUser", 2, false, nil},
		{"weaviate users rotate-key <cluster> <name>", "rotateWeaviateUserKey", 2, false, nil},
		{"weaviate roles list <cluster>", "listWeaviateRoles", 1, false, nil},
		{"weaviate roles create <cluster>", "createWeaviateRole", 1, true, nil},
		{"weaviate roles get <cluster> <name>", "getWeaviateRole", 2, false, nil},
		{"weaviate roles update <cluster> <name>", "updateWeaviateRole", 2, true, nil},
		{"weaviate roles delete <cluster> <name>", "deleteWeaviateRole", 2, false, nil},
	}
}
func weaviateUserFlags(c *cobra.Command) {
	c.Flags().StringArray("role", nil, "Assign a role (repeatable; omitted means no roles)")
	if c.Name() == "update" {
		c.Flags().Bool("active", false, "Activate the database user")
		c.Flags().Bool("inactive", false, "Deactivate the database user")
		c.MarkFlagsMutuallyExclusive("active", "inactive")
	}
}
func weaviateUserBody(cmd *cobra.Command, args []string) (any, error) {
	roles, _ := cmd.Flags().GetStringArray("role")
	if roles == nil {
		roles = []string{}
	}
	b := map[string]any{"roles": roles}
	if cmd.Name() == "create" {
		b["name"] = args[1]
	}
	if cmd.Flags().Changed("active") {
		active, _ := cmd.Flags().GetBool("active")
		b["active"] = active
	}
	if cmd.Flags().Changed("inactive") {
		inactive, _ := cmd.Flags().GetBool("inactive")
		b["active"] = !inactive
	}
	return b, nil
}
func dispatchWeaviate(ctx context.Context, c *apiclient.Client, op string, args []string, q, body []byte) (*http.Response, error) {
	switch op {
	case "listWeaviateUsers":
		var p apiclient.ListWeaviateUsersParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListWeaviateUsers(ctx, args[0], &p)
	case "createWeaviateUser":
		var p apiclient.CreateWeaviateUserParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateWeaviateUserWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "getWeaviateUser":
		var p apiclient.GetWeaviateUserParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetWeaviateUser(ctx, args[0], args[1], &p)
	case "updateWeaviateUser":
		var p apiclient.UpdateWeaviateUserParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.UpdateWeaviateUserWithBody(ctx, args[0], args[1], &p, "application/json", bytes.NewReader(body))
	case "deleteWeaviateUser":
		var p apiclient.DeleteWeaviateUserParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeleteWeaviateUser(ctx, args[0], args[1], &p)
	case "rotateWeaviateUserKey":
		var p apiclient.RotateWeaviateUserKeyParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.RotateWeaviateUserKey(ctx, args[0], args[1], &p)
	case "listWeaviateRoles":
		var p apiclient.ListWeaviateRolesParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListWeaviateRoles(ctx, args[0], &p)
	case "createWeaviateRole":
		var p apiclient.CreateWeaviateRoleParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateWeaviateRoleWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "getWeaviateRole":
		var p apiclient.GetWeaviateRoleParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetWeaviateRole(ctx, args[0], args[1], &p)
	case "updateWeaviateRole":
		var p apiclient.UpdateWeaviateRoleParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.UpdateWeaviateRoleWithBody(ctx, args[0], args[1], &p, "application/json", bytes.NewReader(body))
	case "deleteWeaviateRole":
		var p apiclient.DeleteWeaviateRoleParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeleteWeaviateRole(ctx, args[0], args[1], &p)
	}
	return nil, fmt.Errorf("unknown Weaviate API operation %q", op)
}
