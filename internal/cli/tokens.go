// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var tokenCreateFlags = []string{"name", "expires", "read-only", "scope-namespace", "scope-cluster", "permission", "allow-credential-reveal"}

func tokenFlags(c *cobra.Command) {
	c.Long = "Create an API token and reveal its secret once. Use --name and optional restrictions, or supply the complete request with -f (not both). Restrictions are immutable and can only narrow the owner's authority."
	c.Flags().String("name", "", "Token name (required without -f)")
	c.Flags().String("expires", "", "Expiry as an RFC3339 timestamp or duration from now (for example 720h)")
	c.Flags().Bool("read-only", false, "Restrict the token to read actions")
	c.Flags().StringArray("scope-namespace", nil, "Allowed token namespace (repeatable; distinct from --namespace query scope)")
	c.Flags().StringArray("scope-cluster", nil, "Allowed token cluster as namespace/name (repeatable)")
	c.Flags().StringArray("permission", nil, "Allowed resource:action permission; action may be * (repeatable)")
	c.Flags().Bool("allow-credential-reveal", false, "Allow restricted tokens to reveal credentials when owner permissions allow")
}

func (a *app) tokenBody(cmd *cobra.Command) (any, error) {
	if cmd.Flags().Changed("file") {
		for _, flag := range tokenCreateFlags {
			if cmd.Flags().Changed(flag) {
				return nil, usageError("-f cannot be combined with --%s", flag)
			}
		}
		return a.fileBody(cmd)
	}
	name, _ := cmd.Flags().GetString("name")
	if strings.TrimSpace(name) == "" {
		return nil, usageError("--name is required without -f")
	}
	body := map[string]any{"name": name}
	if cmd.Flags().Changed("expires") {
		value, _ := cmd.Flags().GetString("expires")
		expires, err := time.Parse(time.RFC3339, value)
		if err != nil {
			duration, durationErr := time.ParseDuration(value)
			if durationErr != nil {
				return nil, usageError("--expires must be an RFC3339 timestamp or Go duration (for example 720h)")
			}
			expires = time.Now().Add(duration)
		}
		body["expiresAt"] = expires
	}
	restrictions := map[string]any{}
	for _, flag := range []string{"read-only", "allow-credential-reveal"} {
		if cmd.Flags().Changed(flag) {
			value, _ := cmd.Flags().GetBool(flag)
			key := "readOnly"
			if flag == "allow-credential-reveal" {
				key = "allowCredentialReveal"
			}
			restrictions[key] = value
		}
	}
	for _, field := range []struct{ flag, key string }{{"scope-namespace", "namespaces"}, {"scope-cluster", "clusters"}, {"permission", "permissions"}} {
		if cmd.Flags().Changed(field.flag) {
			values, _ := cmd.Flags().GetStringArray(field.flag)
			restrictions[field.key] = values
		}
	}
	if len(restrictions) > 0 {
		body["restrictions"] = restrictions
	}
	return body, nil
}

func tokenRestrictionsSummary(value any) string {
	restrictions, ok := value.(map[string]any)
	if !ok {
		return "-"
	}
	var parts []string
	if restrictions["readOnly"] == true {
		parts = append(parts, "read-only")
	}
	for _, field := range []struct{ key, label string }{{"namespaces", "ns"}, {"clusters", "clusters"}, {"permissions", "perms"}} {
		if values, ok := restrictions[field.key].([]any); ok && len(values) > 0 {
			parts = append(parts, field.label+"="+cell(values))
		}
	}
	if restrictions["allowCredentialReveal"] == true {
		parts = append(parts, "credential-reveal")
	} else {
		parts = append(parts, "no-credential-reveal")
	}
	return strings.Join(parts, ", ")
}

// Only table output is summarized; JSON and YAML retain the full API response.
func (a *app) tokensTable(result any) error {
	if items, ok := nested(result, "items").([]any); ok {
		for _, item := range items {
			if token, ok := item.(map[string]any); ok {
				token["restrictions"] = tokenRestrictionsSummary(token["restrictions"])
			}
		}
	} else if token, ok := nested(result, "token").(map[string]any); ok {
		token["restrictions"] = tokenRestrictionsSummary(token["restrictions"])
	} else if token, ok := result.(map[string]any); ok {
		token["restrictions"] = tokenRestrictionsSummary(token["restrictions"])
	}
	return a.print(result)
}
