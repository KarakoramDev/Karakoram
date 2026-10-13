// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/KarakoramDev/Karakoram/pkg/client"
	"github.com/spf13/cobra"
)

type elasticCommand struct {
	group, verb, operation string
	named                  bool
}

var elasticCommands = []elasticCommand{
	{"users", "list", "listElasticUsers", false},
	{"users", "create", "createElasticUser", true},
	{"users", "get", "getElasticUser", true},
	{"users", "update", "updateElasticUser", true},
	{"users", "delete", "deleteElasticUser", true},
	{"roles", "list", "listElasticRoles", false},
	{"roles", "create", "createElasticRole", true},
	{"roles", "get", "getElasticRole", true},
	{"roles", "update", "updateElasticRole", true},
	{"roles", "delete", "deleteElasticRole", true},
	{"api-keys", "list", "listElasticAPIKeys", false},
	{"api-keys", "create", "createElasticAPIKey", false},
	{"api-keys", "invalidate", "invalidateElasticAPIKey", true},
}

func (a *app) addElastic(root *cobra.Command) {
	group := &cobra.Command{Use: "elasticsearch", Short: "Manage Elasticsearch native users, roles and API keys"}
	root.AddCommand(group)
	groups := map[string]*cobra.Command{}
	for _, spec := range elasticCommands {
		s := spec
		parent := groups[s.group]
		if parent == nil {
			parent = &cobra.Command{Use: s.group, Short: "Manage Elasticsearch " + s.group}
			group.AddCommand(parent)
			groups[s.group] = parent
		}
		use := s.verb + " <cluster>"
		argc := 1
		if s.named {
			argc = 2
			use += " <name>"
			if s.group == "api-keys" {
				use = s.verb + " <cluster> <id>"
			}
		}
		c := &cobra.Command{Use: use, Short: s.operation, Args: cobra.ExactArgs(argc)}
		annotate(c, s.operation)
		if s.verb == "list" {
			c.Flags().Int("limit", 100, "Maximum items")
			c.Flags().String("cursor", "", "Continuation cursor")
		}
		if s.verb == "create" || s.verb == "update" {
			switch s.group {
			case "users":
				c.Flags().StringSlice("roles", nil, "Comma-separated Elasticsearch role names")
				c.Flags().String("full-name", "", "Display name")
				c.Flags().String("email", "", "Email address")
				c.Flags().Bool("disabled", false, "Disable the user")
				c.Flags().Bool("password-stdin", false, "Read the password from standard input (required on create)")
				if s.verb == "update" {
					annotate(c, s.operation, "getElasticUser")
				}
			case "roles":
				c.Flags().StringP("file", "f", "", "Role definition JSON file ('-' for stdin)")
			case "api-keys":
				c.Flags().String("name", "", "API key name")
				c.Flags().String("expiration", "", "Elasticsearch duration, for example 1d")
				c.Flags().StringP("file", "f", "", "Role descriptors JSON map ('-' for stdin)")
			}
		}
		c.RunE = func(cmd *cobra.Command, args []string) error { return a.runElastic(cmd, s, args) }
		parent.AddCommand(c)
	}
}
func (a *app) runElastic(cmd *cobra.Command, s elasticCommand, args []string) error {
	q := map[string]any{}
	if cmd.Flags().Changed("limit") {
		q["limit"], _ = cmd.Flags().GetInt("limit")
	}
	if cmd.Flags().Changed("cursor") {
		q["cursor"], _ = cmd.Flags().GetString("cursor")
	}
	var body map[string]any
	if s.verb == "create" || s.verb == "update" {
		switch s.group {
		case "users":
			body = map[string]any{"username": args[1], "roles": []string{}, "enabled": true}
			if s.verb == "update" {
				previous, err := a.request(cmd.Context(), "getElasticUser", args, nil, nil)
				if err != nil {
					return err
				}
				var ok bool
				body, ok = previous.(map[string]any)
				if !ok {
					return fmt.Errorf("invalid user response")
				}
			}
			if cmd.Flags().Changed("roles") {
				body["roles"], _ = cmd.Flags().GetStringSlice("roles")
			}
			for _, flag := range []struct{ name, key string }{{"full-name", "fullName"}, {"email", "email"}} {
				if cmd.Flags().Changed(flag.name) {
					body[flag.key], _ = cmd.Flags().GetString(flag.name)
				}
			}
			if cmd.Flags().Changed("disabled") {
				disabled, _ := cmd.Flags().GetBool("disabled")
				body["enabled"] = !disabled
			}
			passwordStdin, _ := cmd.Flags().GetBool("password-stdin")
			if s.verb == "create" && !passwordStdin {
				return fmt.Errorf("user creation requires --password-stdin")
			}
			if passwordStdin {
				password, err := io.ReadAll(a.opts.In)
				if err != nil {
					return err
				}
				body["password"] = strings.TrimSuffix(strings.TrimSuffix(string(password), "\n"), "\r")
			}
		case "roles":
			var err error
			body, err = a.fileBody(cmd)
			if err != nil {
				return err
			}
			if name, exists := body["name"]; exists && name != args[1] {
				return fmt.Errorf("role file name must match the command argument")
			}
			body["name"] = args[1]
			for _, field := range []string{"cluster", "indices", "applications"} {
				if _, ok := body[field]; !ok {
					body[field] = []any{}
				}
			}
		case "api-keys":
			name, _ := cmd.Flags().GetString("name")
			if strings.TrimSpace(name) == "" {
				return fmt.Errorf("--name is required")
			}
			body = map[string]any{"name": name}
			if cmd.Flags().Changed("expiration") {
				body["expiration"], _ = cmd.Flags().GetString("expiration")
			}
			file, _ := cmd.Flags().GetString("file")
			if file != "" {
				descriptors, err := a.fileBody(cmd)
				if err != nil {
					return err
				}
				body["roleDescriptors"] = descriptors
			}
		}
	}
	result, err := a.request(cmd.Context(), s.operation, args, q, body)
	if err != nil {
		return err
	}
	return a.print(result)
}

func dispatchElastic(ctx context.Context, c *client.Client, op string, args []string, query, body []byte) (*http.Response, error) {
	switch op {
	case "listElasticUsers":
		var p client.ListElasticUsersParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.ListElasticUsers(ctx, args[0], &p)
	case "createElasticUser":
		var p client.CreateElasticUserParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.CreateElasticUserWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "getElasticUser":
		var p client.GetElasticUserParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.GetElasticUser(ctx, args[0], args[1], &p)
	case "updateElasticUser":
		var p client.UpdateElasticUserParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.UpdateElasticUserWithBody(ctx, args[0], args[1], &p, "application/json", bytes.NewReader(body))
	case "deleteElasticUser":
		var p client.DeleteElasticUserParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.DeleteElasticUser(ctx, args[0], args[1], &p)
	case "listElasticRoles":
		var p client.ListElasticRolesParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.ListElasticRoles(ctx, args[0], &p)
	case "createElasticRole":
		var p client.CreateElasticRoleParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.CreateElasticRoleWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "getElasticRole":
		var p client.GetElasticRoleParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.GetElasticRole(ctx, args[0], args[1], &p)
	case "updateElasticRole":
		var p client.UpdateElasticRoleParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.UpdateElasticRoleWithBody(ctx, args[0], args[1], &p, "application/json", bytes.NewReader(body))
	case "deleteElasticRole":
		var p client.DeleteElasticRoleParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.DeleteElasticRole(ctx, args[0], args[1], &p)
	case "listElasticAPIKeys":
		var p client.ListElasticAPIKeysParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.ListElasticAPIKeys(ctx, args[0], &p)
	case "createElasticAPIKey":
		var p client.CreateElasticAPIKeyParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.CreateElasticAPIKeyWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "invalidateElasticAPIKey":
		var p client.InvalidateElasticAPIKeyParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.InvalidateElasticAPIKey(ctx, args[0], args[1], &p)
	}
	return nil, fmt.Errorf("unknown Elasticsearch operation %q", op)
}
