// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

func (a *app) namespaceFlags(c *cobra.Command, op string) {
	switch op {
	case "createNamespace":
		c.Flags().String("name", "", "Namespace name")
		c.Flags().String("project-id", "", "Zerops project ID")
		c.Flags().String("description", "", "Namespace description")
		c.Flags().String("token-file", "", "Zerops token file ('-' reads stdin)")
	case "updateNamespace":
		c.Flags().String("description", "", "Namespace description")
	case "rotateNamespaceToken":
		c.Flags().String("token-file", "", "Zerops token file ('-' reads stdin)")
	}
}

func (a *app) namespaceBody(cmd *cobra.Command, op string) (any, error) {
	switch op {
	case "createNamespace", "rotateNamespaceToken":
		path, _ := cmd.Flags().GetString("token-file")
		if path == "" {
			return nil, fmt.Errorf("--token-file is required ('-' reads stdin)")
		}
		var data []byte
		var err error
		if path == "-" {
			data, err = io.ReadAll(a.opts.In)
		} else {
			data, err = os.ReadFile(path)
		}
		if err != nil {
			return nil, fmt.Errorf("read token file: %w", err)
		}
		token := strings.TrimSpace(string(data))
		if token == "" {
			return nil, fmt.Errorf("token file is empty")
		}
		body := map[string]any{"token": token}
		if op == "createNamespace" {
			name, _ := cmd.Flags().GetString("name")
			project, _ := cmd.Flags().GetString("project-id")
			if name == "" || project == "" {
				return nil, fmt.Errorf("--name and --project-id are required")
			}
			description, _ := cmd.Flags().GetString("description")
			body["name"], body["projectId"], body["description"] = name, project, description
		}
		return body, nil
	case "updateNamespace":
		if !cmd.Flags().Changed("description") {
			return nil, fmt.Errorf("--description is required")
		}
		description, _ := cmd.Flags().GetString("description")
		return map[string]any{"description": description}, nil
	default:
		return nil, nil
	}
}

// listNamespaces enumerates every authorized page for both commands and internal callers.
func (a *app) listNamespaces(ctx context.Context) (any, error) {
	query := map[string]any{}
	items := []any{}
	for {
		resp, err := dispatch(ctx, a.client, a.root, "listNamespaces", nil, query, nil)
		if err != nil {
			return nil, err
		}
		page, err := decodeResponse(resp)
		if err != nil {
			return nil, err
		}
		rows, ok := nested(page, "items").([]any)
		if !ok {
			return nil, fmt.Errorf("unexpected namespace list response")
		}
		items = append(items, rows...)
		cursor, _ := nested(page, "nextCursor").(string)
		if cursor == "" {
			return map[string]any{"items": items}, nil
		}
		query["cursor"] = cursor
	}
}

func (a *app) namespacesTable(v any) error {
	rows, ok := nested(v, "items").([]any)
	if !ok {
		return fmt.Errorf("unexpected namespace list response")
	}
	if len(rows) == 0 {
		_, err := fmt.Fprintln(a.opts.Out, "No results.")
		return err
	}
	w := tabwriter.NewWriter(a.opts.Out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tPROJECT\tDEFAULT\tPHASE\tTOKEN VALID\tRELAY PHASE\tCONNECTED\tCLUSTERS")
	for _, row := range rows {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", cell(nested(row, "name")), cell(nested(row, "projectId")), cell(nested(row, "default")), cell(nested(row, "phase")), cell(nested(row, "token", "valid")), cell(nested(row, "relay", "phase")), cell(nested(row, "relay", "connected")), cell(nested(row, "clusterCount")))
	}
	return w.Flush()
}

func (a *app) addConfig(root *cobra.Command) {
	group := &cobra.Command{Use: "config", Short: "Manage saved connection contexts"}
	set := &cobra.Command{Use: "set-context <name>", Short: "Save server, credential and namespace flags in a context", Args: cobra.ExactArgs(1)}
	set.PersistentPreRunE = func(cmd *cobra.Command, args []string) error { return nil }
	set.RunE = func(cmd *cobra.Command, args []string) error {
		path := a.opts.ConfigPath
		if path == "" {
			var err error
			path, err = ConfigPath()
			if err != nil {
				return err
			}
		}
		config, err := LoadConfig(path)
		if err != nil {
			return err
		}
		c := config.Contexts[args[0]]
		if cmd.Flags().Changed("server") {
			c.Server = a.server
		}
		if cmd.Flags().Changed("token") {
			c.Token = a.token
			c.TokenID = ""
		}
		if cmd.Flags().Changed("namespace") {
			c.Namespace = a.namespace
		}
		config.Contexts[args[0]] = c
		if config.CurrentContext == "" {
			config.CurrentContext = args[0]
		}
		if err := SaveConfig(path, config); err != nil {
			return err
		}
		_, err = fmt.Fprintf(a.opts.Out, "Context %s saved.\n", args[0])
		return err
	}
	group.AddCommand(set)
	root.AddCommand(group)
}
