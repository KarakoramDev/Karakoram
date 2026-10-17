// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/KarakoramDev/Karakoram/pkg/client"
	"github.com/spf13/cobra"
)

var supabaseFunctionName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

func (a *app) addSupabase(root *cobra.Command) {
	group := groupCommand("supabase", "Manage Supabase credentials, SMTP and edge functions")
	root.AddCommand(group)
	functions := groupCommand("functions", "List, inspect, deploy and delete edge functions")
	smtp := groupCommand("smtp-password", "Set the write-only SMTP password")
	group.AddCommand(functions, smtp)
	specs := []struct {
		parent         *cobra.Command
		use, short, op string
		args           int
	}{
		{group, "credentials <cluster>", "Show Supabase endpoints and masked credentials (use --show-secrets to reveal)", "getSupabaseCredentials", 1},
		{smtp, "set <cluster>", "Read the SMTP password from a hidden prompt or the first stdin line", "setSupabaseSMTPPassword", 1},
		{functions, "list <cluster>", "List deployed edge function names", "listSupabaseFunctions", 1},
		{functions, "get <cluster> <name>", "Read edge function source files", "getSupabaseFunction", 2},
		{functions, "deploy <cluster> <name> <dir|file>", "Deploy source files (at most 1 MiB, without symlinks)", "putSupabaseFunction", 3},
		{functions, "delete <cluster> <name>", "Delete an edge function after confirmation", "deleteSupabaseFunction", 2},
		{group, "rotate-jwt-secret <cluster>", "Rotate the JWT signing secret and API keys after confirmation", "rotateSupabaseJWTSecret", 1},
	}
	for _, spec := range specs {
		s := spec
		cmd := &cobra.Command{Use: s.use, Short: s.short, Args: cobra.ExactArgs(s.args)}
		annotate(cmd, s.op)
		if s.op == "getSupabaseCredentials" {
			cmd.Flags().Bool("show-secrets", false, "Reveal credentials in output; protect terminal and redirected output")
		}
		if destructive(s.op) {
			cmd.Flags().BoolP("yes", "y", false, "Confirm without reading stdin")
		}
		if s.op == "putSupabaseFunction" {
			cmd.ValidArgsFunction = cobra.FixedCompletions(nil, cobra.ShellCompDirectiveDefault)
		} else {
			cmd.ValidArgsFunction = cobra.FixedCompletions(nil, cobra.ShellCompDirectiveNoFileComp)
		}
		cmd.RunE = func(cmd *cobra.Command, args []string) error { return a.runSupabase(cmd, s.op, args) }
		s.parent.AddCommand(cmd)
	}
}

func (a *app) runSupabase(cmd *cobra.Command, op string, args []string) error {
	if len(args) > 1 && !supabaseFunctionName.MatchString(args[1]) {
		return usageError("function name must match [a-z0-9][a-z0-9_-]{0,62}")
	}
	if err := a.confirm(cmd, op, args); err != nil {
		return err
	}
	var body map[string]string
	switch op {
	case "setSupabaseSMTPPassword":
		value, err := password(a.opts.In, a.opts.Err)
		if err != nil {
			return err
		}
		body = map[string]string{"password": value}
	case "putSupabaseFunction":
		files, err := readSupabaseFunction(args[2])
		if err != nil {
			return err
		}
		body = files
	}
	result, err := a.request(cmd.Context(), op, args, nil, body)
	if err != nil {
		return err
	}
	if op == "getSupabaseCredentials" {
		show, _ := cmd.Flags().GetBool("show-secrets")
		if !show {
			if fields, ok := result.(map[string]any); ok {
				for _, field := range []string{"anonKey", "serviceRoleKey", "publishableKey", "secretKey", "dashboardUsername", "dashboardPassword", "jwtSecret"} {
					fields[field] = "********"
				}
			}
			fmt.Fprintln(a.opts.Err, "Use --show-secrets to reveal sensitive Supabase credentials.")
		}
	}
	return a.print(result)
}

func readSupabaseFunction(source string) (map[string]string, error) {
	info, err := os.Lstat(source)
	if err != nil {
		return nil, err
	}
	files := map[string]string{}
	total := int64(0)
	add := func(path, name string, info fs.FileInfo) error {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("function sources must be regular files, not symlinks or special files")
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		data, err := io.ReadAll(io.LimitReader(file, (1<<20)-total+1))
		if err != nil {
			return err
		}
		total += int64(len(data))
		if total > 1<<20 {
			return fmt.Errorf("function source must not exceed 1 MiB")
		}
		files[filepath.ToSlash(name)] = string(data)
		return nil
	}
	if info.IsDir() {
		err = filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				return nil
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			name, err := filepath.Rel(source, path)
			if err != nil {
				return err
			}
			return add(path, name, info)
		})
	} else {
		err = add(source, "index.ts", info)
	}
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("function must contain at least one source file")
	}
	for name := range files {
		if strings.ContainsAny(name, "\\\x00:") {
			return nil, fmt.Errorf("function source paths must be portable relative paths")
		}
	}
	return files, nil
}

func dispatchSupabase(ctx context.Context, c *client.Client, op string, args []string, query, body []byte) (*http.Response, error) {
	switch op {
	case "getSupabaseCredentials":
		var p client.GetSupabaseCredentialsParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.GetSupabaseCredentials(ctx, args[0], &p)
	case "setSupabaseSMTPPassword":
		var p client.SetSupabaseSMTPPasswordParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.SetSupabaseSMTPPasswordWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "listSupabaseFunctions":
		var p client.ListSupabaseFunctionsParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.ListSupabaseFunctions(ctx, args[0], &p)
	case "getSupabaseFunction":
		var p client.GetSupabaseFunctionParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.GetSupabaseFunction(ctx, args[0], args[1], &p)
	case "putSupabaseFunction":
		var p client.PutSupabaseFunctionParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.PutSupabaseFunctionWithBody(ctx, args[0], args[1], &p, "application/json", bytes.NewReader(body))
	case "deleteSupabaseFunction":
		var p client.DeleteSupabaseFunctionParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.DeleteSupabaseFunction(ctx, args[0], args[1], &p)
	case "rotateSupabaseJWTSecret":
		var p client.RotateSupabaseJWTSecretParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.RotateSupabaseJWTSecret(ctx, args[0], &p)
	}
	return nil, fmt.Errorf("unknown Supabase operation %q", op)
}
