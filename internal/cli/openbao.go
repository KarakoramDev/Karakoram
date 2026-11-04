// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	apiclient "github.com/KarakoramDev/Karakoram/pkg/client"
	"github.com/spf13/cobra"
)

func openBaoCommandSpecs() []commandSpec {
	return []commandSpec{
		{"openbao unseal-keys reveal <cluster>", "revealOpenBaoUnsealKeys", 1, false, nil},
		{"openbao database-connections list <cluster>", "listOpenBaoDatabaseConnections", 1, false, nil},
		{"openbao database-connections get <cluster> <name>", "getOpenBaoDatabaseConnection", 2, false, nil},
		{"openbao database-connections create <cluster>", "createOpenBaoDatabaseConnection", 1, true, nil},
		{"openbao database-connections delete <cluster> <name>", "deleteOpenBaoDatabaseConnection", 2, false, nil},
	}
}

func openBaoFlags(c *cobra.Command, op string) {
	switch op {
	case "revealOpenBaoUnsealKeys":
		c.Flags().String("confirm", "", "Confirm the cluster name to reveal unseal material")
		_ = c.MarkFlagRequired("confirm")
		validate := c.Args
		c.Args = func(cmd *cobra.Command, args []string) error {
			if err := validate(cmd, args); err != nil {
				return err
			}
			confirm, _ := cmd.Flags().GetString("confirm")
			if confirm == "" {
				return usageError("--confirm is required")
			}
			if confirm != args[0] {
				return usageError("--confirm must equal the cluster name")
			}
			return nil
		}
	case "createOpenBaoDatabaseConnection":
		c.Flags().String("name", "", "Database connection name")
		c.Flags().String("target", "", "Target PostgreSQL, MongoDB replica-set or RabbitMQ cluster in the same namespace")
		c.Flags().StringArray("role", nil, "Role: name:readonly|readwrite[:database] (repeatable)")
		c.Flags().Duration("default-ttl", 0, "Default credential TTL for roles without an explicit TTL (e.g. 1h)")
		c.Flags().Duration("max-ttl", 0, "Maximum credential TTL for roles without an explicit TTL (e.g. 24h)")
	}
}

var openBaoRoleName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,30}$`)

func parseOpenBaoRole(value string) (map[string]any, error) {
	parts := strings.Split(value, ":")
	if len(parts) < 2 || len(parts) > 3 || !openBaoRoleName.MatchString(parts[0]) || (parts[1] != "readonly" && parts[1] != "readwrite") || len(parts) == 3 && parts[2] == "" {
		return nil, usageError("invalid --role %q: expected name:readonly|readwrite[:database]", value)
	}
	role := map[string]any{"name": parts[0], "access": parts[1]}
	if len(parts) == 3 {
		role["database"] = parts[2]
	}
	return role, nil
}

func (a *app) openBaoBody(cmd *cobra.Command, op string) (any, error) {
	if op == "revealOpenBaoUnsealKeys" {
		confirm, _ := cmd.Flags().GetString("confirm")
		fmt.Fprintln(a.opts.Err, "WARNING: Output contains unseal shares and the initial root token; store them offline.")
		return map[string]any{"confirm": confirm}, nil
	}
	if op != "createOpenBaoDatabaseConnection" {
		return nil, nil
	}
	var body map[string]any
	file, _ := cmd.Flags().GetString("file")
	if file != "" {
		var err error
		body, err = a.fileBody(cmd)
		if err != nil {
			return nil, err
		}
	} else {
		name, _ := cmd.Flags().GetString("name")
		target, _ := cmd.Flags().GetString("target")
		values, _ := cmd.Flags().GetStringArray("role")
		if name == "" || target == "" || len(values) == 0 {
			return nil, usageError("--name, --target and at least one --role are required without -f")
		}
		roles := make([]any, 0, len(values))
		for _, value := range values {
			role, err := parseOpenBaoRole(value)
			if err != nil {
				return nil, err
			}
			roles = append(roles, role)
		}
		body = map[string]any{"name": name, "targetCluster": target, "roles": roles}
	}
	for _, ttl := range []struct{ flag, key string }{{"default-ttl", "defaultTtlSeconds"}, {"max-ttl", "maxTtlSeconds"}} {
		if !cmd.Flags().Changed(ttl.flag) {
			continue
		}
		duration, _ := cmd.Flags().GetDuration(ttl.flag)
		if duration <= 0 || duration%time.Second != 0 {
			return nil, usageError("--%s must be a positive whole number of seconds", ttl.flag)
		}
		roles, _ := body["roles"].([]any)
		for _, value := range roles {
			role, ok := value.(map[string]any)
			if ok {
				if _, exists := role[ttl.key]; !exists {
					role[ttl.key] = int64(duration / time.Second)
				}
			}
		}
	}
	return body, nil
}

func (a *app) openBaoUnsealKeysTable(result any) error {
	if _, err := fmt.Fprintf(a.opts.Out, "Threshold: %s\nSeal type: %s\n", cell(nested(result, "threshold")), cell(nested(result, "sealType"))); err != nil {
		return err
	}
	shares, _ := nested(result, "shares").([]any)
	for i, share := range shares {
		if _, err := fmt.Fprintf(a.opts.Out, "Share %d: %s\n", i+1, cell(share)); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(a.opts.Out, "Initial root token: %s\n", cell(nested(result, "initialRootToken")))
	return err
}

func dispatchOpenBao(ctx context.Context, c *apiclient.Client, op string, args []string, q, body []byte) (*http.Response, error) {
	switch op {
	case "revealOpenBaoUnsealKeys":
		var p apiclient.RevealOpenBaoUnsealKeysParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.RevealOpenBaoUnsealKeysWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "listOpenBaoDatabaseConnections":
		var p apiclient.ListOpenBaoDatabaseConnectionsParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListOpenBaoDatabaseConnections(ctx, args[0], &p)
	case "getOpenBaoDatabaseConnection":
		var p apiclient.GetOpenBaoDatabaseConnectionParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetOpenBaoDatabaseConnection(ctx, args[0], args[1], &p)
	case "createOpenBaoDatabaseConnection":
		var p apiclient.CreateOpenBaoDatabaseConnectionParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateOpenBaoDatabaseConnectionWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "deleteOpenBaoDatabaseConnection":
		var p apiclient.DeleteOpenBaoDatabaseConnectionParams
		if err := json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeleteOpenBaoDatabaseConnection(ctx, args[0], args[1], &p)
	}
	return nil, fmt.Errorf("unknown OpenBao API operation %q", op)
}
