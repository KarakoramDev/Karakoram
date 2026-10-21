// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

func mcpTokenLifetime(value string) (time.Duration, error) {
	if strings.HasSuffix(value, "d") {
		days, err := strconv.ParseInt(strings.TrimSuffix(value, "d"), 10, 64)
		if err != nil || days <= 0 || days > int64((1<<63-1)/(24*time.Hour)) {
			return 0, usageError("--expires-in must be a positive lifetime (for example 90d or 24h)")
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, usageError("--expires-in must be a positive lifetime (for example 90d or 24h)")
	}
	return duration, nil
}

func (a *app) managementMCPConfig(cmd *cobra.Command, client string, write bool) error {
	switch client {
	case "claude-code", "cursor", "vscode", "codex", "json":
	default:
		return usageError("unsupported MCP client %q", client)
	}
	lifetime, _ := cmd.Flags().GetString("expires-in")
	duration, err := mcpTokenLifetime(lifetime)
	if err != nil {
		return err
	}
	name, _ := cmd.Flags().GetString("token-name")
	now := time.Now().UTC()
	if name == "" {
		name = "mcp-" + client + "-" + now.Format("2006-01-02")
	}
	namespaces, _ := cmd.Flags().GetStringArray("namespace")
	restrictions := map[string]any{"readOnly": !write}
	if len(namespaces) > 0 {
		restrictions["namespaces"] = namespaces
	}
	result, err := a.request(cmd.Context(), "createAPIToken", nil, nil, map[string]any{"name": name, "expiresAt": now.Add(duration), "restrictions": restrictions})
	if err != nil {
		return err
	}
	secret, _ := nested(result, "secret").(string)
	if secret == "" {
		return fmt.Errorf("token creation response did not contain a secret")
	}
	stdio, _ := cmd.Flags().GetBool("stdio")
	snippet, err := mcpClientConfig(client, a.mcpEndpoint(""), secret, a.selected, stdio)
	if err != nil {
		return err
	}
	fmt.Fprintln(a.opts.Err, "WARNING: A dedicated API token was created. Its secret is shown once; protect this output and revoke the token when no longer needed.")
	// Remote snippets contain the secret exactly once. Stdio uses the existing
	// saved context, so reveal the new token separately, never inside that snippet.
	if stdio {
		fmt.Fprintln(a.opts.Err, "API token: "+secret)
	}
	output, _ := cmd.Flags().GetString("output")
	if output != "" {
		return writeMCPConfig(output, snippet)
	}
	_, err = fmt.Fprint(a.opts.Out, snippet)
	return err
}

func mcpClientConfig(client, endpoint, secret, contextName string, stdio bool) (string, error) {
	args := []string{"--context", contextName, "mcp"}
	entry := map[string]any{"url": endpoint, "headers": map[string]string{"Authorization": "Bearer " + secret}}
	if stdio {
		entry = map[string]any{"command": "karactl", "args": args}
	}
	switch client {
	case "claude-code":
		if stdio {
			return "claude mcp add --transport stdio karakoram -- karactl --context " + mcpShellQuote(contextName) + " mcp\n", nil
		}
		return "claude mcp add --transport http karakoram " + mcpShellQuote(endpoint) + " --header " + mcpShellQuote("Authorization: Bearer "+secret) + "\n", nil
	case "codex":
		if stdio {
			encodedArgs := make([]string, len(args))
			for i, arg := range args {
				encodedArgs[i] = strconv.Quote(arg)
			}
			return "[mcp_servers.karakoram]\ncommand = \"karactl\"\nargs = [" + strings.Join(encodedArgs, ", ") + "]\n", nil
		}
		return "[mcp_servers.karakoram]\nurl = " + strconv.Quote(endpoint) + "\nhttp_headers = { Authorization = " + strconv.Quote("Bearer "+secret) + " }\n", nil
	case "vscode":
		if stdio {
			entry["type"] = "stdio"
		} else {
			entry["type"] = "http"
		}
		b, err := json.MarshalIndent(map[string]any{"servers": map[string]any{"karakoram": entry}}, "", "  ")
		return string(b) + "\n", err
	case "cursor", "json":
		if client == "json" && !stdio {
			entry["type"] = "http"
		}
		b, err := json.MarshalIndent(map[string]any{"mcpServers": map[string]any{"karakoram": entry}}, "", "  ")
		return string(b) + "\n", err
	default:
		return "", usageError("unsupported MCP client %q", client)
	}
}

func mcpShellQuote(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'" }

// Write through a private same-directory temporary file so replacing an existing
// world-readable file never exposes the secret before permissions are tightened.
func writeMCPConfig(path, content string) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".karactl-mcp-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.WriteString(content); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}
