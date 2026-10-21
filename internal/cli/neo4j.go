// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	apiclient "github.com/KarakoramDev/Karakoram/pkg/client"
	"github.com/spf13/cobra"
)

func neo4jCommandSpecs() []commandSpec {
	return []commandSpec{
		{"neo4j users list <cluster>", "listNeo4jUsers", 1, false, nil},
		{"neo4j users get <cluster> <name>", "getNeo4jUser", 2, false, nil},
		{"neo4j users create <cluster> <name>", "createNeo4jUser", 2, false, nil},
		{"neo4j users delete <cluster> <name>", "deleteNeo4jUser", 2, false, nil},
		{"neo4j users password <cluster> <name>", "setNeo4jUserPassword", 2, false, nil},
	}
}

func (a *app) neo4jBody(cmd *cobra.Command, op string, args []string) (any, error) {
	if op != "createNeo4jUser" && op != "setNeo4jUserPassword" {
		return nil, nil
	}
	body := map[string]any{}
	if op == "createNeo4jUser" {
		body["name"] = args[1]
	}
	stdin, _ := cmd.Flags().GetBool("password-stdin")
	if stdin {
		input, err := readInput(a.opts.In)
		if err != nil {
			return nil, err
		}
		password := strings.TrimSuffix(strings.TrimSuffix(string(input), "\n"), "\r")
		if password == "" {
			return nil, usageError("password must not be empty")
		}
		body["password"] = password
	}
	return body, nil
}

func dispatchNeo4j(ctx context.Context, c *apiclient.Client, op string, args []string, q, body []byte) (*http.Response, error) {
	switch op {
	case "listNeo4jUsers":
		var p apiclient.ListNeo4jUsersParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListNeo4jUsers(ctx, args[0], &p)
	case "getNeo4jUser":
		var p apiclient.GetNeo4jUserParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetNeo4jUser(ctx, args[0], args[1], &p)
	case "createNeo4jUser":
		var p apiclient.CreateNeo4jUserParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateNeo4jUserWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "deleteNeo4jUser":
		var p apiclient.DeleteNeo4jUserParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeleteNeo4jUser(ctx, args[0], args[1], &p)
	case "setNeo4jUserPassword":
		var p apiclient.SetNeo4jUserPasswordParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.SetNeo4jUserPasswordWithBody(ctx, args[0], args[1], &p, "application/json", bytes.NewReader(body))
	}
	return nil, fmt.Errorf("unknown Neo4j API operation %q", op)
}
