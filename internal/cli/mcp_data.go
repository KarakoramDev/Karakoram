// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/spf13/cobra"
)

func dataMCPConfig(cmd *cobra.Command, a *app, cluster string, write bool, client string) error {
	namespace, name, ok := strings.Cut(cluster, "/")
	if !ok || !dataMCPPathComponent(namespace) || !dataMCPPathComponent(name) {
		return usageError("--cluster must be namespace/name")
	}
	switch client {
	case "claude-code", "cursor", "vscode", "json":
	default:
		return usageError("unsupported MCP client %q (choose claude-code, cursor, vscode or json)", client)
	}
	lifetime, _ := cmd.Flags().GetString("expires-in")
	duration, err := dataMCPDuration(lifetime)
	if err != nil {
		return usageError("--expires-in must be a positive duration (e.g. 90d or 24h)")
	}
	tokenName, _ := cmd.Flags().GetString("token-name")
	if !cmd.Flags().Changed("token-name") {
		hostname, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("determine hostname for token name: %w", err)
		}
		tokenName = dataMCPTokenName(name, hostname)
	}
	tokenName = strings.TrimSpace(tokenName)
	if tokenName == "" || utf8.RuneCountInString(tokenName) > 128 {
		return usageError("--token-name must contain 1–128 non-blank characters")
	}
	permissions := []string{"cluster-data:read"}
	if write {
		permissions = append(permissions, "cluster-data:write")
	}
	body, err := json.Marshal(map[string]any{
		"name":      tokenName,
		"expiresAt": time.Now().UTC().Add(duration),
		"restrictions": map[string]any{
			"namespaces": []string{namespace}, "clusters": []string{cluster},
			"permissions": permissions, "readOnly": !write,
		},
	})
	if err != nil {
		return err
	}
	// Use the raw-body generated method: older schemas do not yet expose
	// restrictions, and omitting them would mint an unrestricted credential.
	resp, err := a.client.CreateAPITokenWithBody(cmd.Context(), nil, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create restricted data MCP API token: %w", err)
	}
	result, err := decodeResponse(resp)
	if err != nil {
		return fmt.Errorf("create restricted data MCP API token (server must support token restrictions): %w", err)
	}
	object, _ := result.(map[string]any)
	secret, _ := object["secret"].(string)
	if secret == "" {
		return fmt.Errorf("create restricted data MCP API token: response did not contain a secret")
	}
	endpoint := strings.TrimSuffix(a.server, "/api/v1") + "/mcp/clusters/" + url.PathEscape(namespace) + "/" + url.PathEscape(name)
	serverName := "karakoram-" + namespace + "-" + name
	headers := map[string]string{"Authorization": "Bearer " + secret}
	fmt.Fprintln(a.opts.Err, "WARNING: this output contains a credential. Store it securely; the token secret is shown only once.")
	if client == "claude-code" {
		quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
		_, err = fmt.Fprintf(a.opts.Out, "claude mcp add --transport http %s %s --header %s\n", quote(serverName), quote(endpoint), quote("Authorization: Bearer "+secret))
		return err
	}
	var output any = map[string]any{"url": endpoint, "headers": headers}
	switch client {
	case "cursor":
		output = map[string]any{"mcpServers": map[string]any{serverName: output}}
	case "vscode":
		output = map[string]any{"servers": map[string]any{serverName: map[string]any{"type": "http", "url": endpoint, "headers": headers}}}
	}
	encoder := json.NewEncoder(a.opts.Out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(output)
}

func dataMCPPathComponent(s string) bool {
	return s != "" && s != "." && s != ".." && !strings.ContainsAny(s, "/\\") && !strings.ContainsFunc(s, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) })
}

func dataMCPDuration(s string) (time.Duration, error) {
	if strings.HasSuffix(s, "d") {
		days, err := strconv.ParseInt(strings.TrimSuffix(s, "d"), 10, 64)
		const day = 24 * time.Hour
		if err != nil || days <= 0 || days > int64((1<<63-1)/day) {
			return 0, fmt.Errorf("invalid day duration")
		}
		return time.Duration(days) * day, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("invalid duration")
	}
	return d, nil
}

func dataMCPTokenName(cluster, hostname string) string {
	name := strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return '-'
		}
		return r
	}, "mcp-data-"+cluster+"-"+hostname)
	runes := []rune(name)
	if len(runes) > 128 {
		name = string(runes[:128])
	}
	return name
}
