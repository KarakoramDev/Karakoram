// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"fmt"
	"github.com/spf13/cobra"
)

func (a *app) connectionSummary(cmd *cobra.Command, args []string) error {
	cluster, err := a.request(cmd.Context(), "getCluster", args, nil, nil)
	if err != nil {
		return err
	}
	mode := nested(cluster, "spec", "tls", "mode")
	ca := "not checked; export with clusters ca (credentials-read permission required)"
	if mode == "disabled" {
		ca = "not configured (TLS disabled)"
	}
	summary := map[string]any{"hosts": nested(cluster, "status", "connection", "hosts"), "publicHosts": nested(cluster, "status", "connection", "publicHosts"), "tlsMode": mode, "caAvailability": ca}
	if err := a.print(summary); err != nil {
		return err
	}
	_, err = fmt.Fprintf(a.opts.Err, "Use karactl clusters connection %s --reveal to display sensitive credentials; karactl clusters ca %s exports the CA.\n", args[0], args[0])
	return err
}

func (a *app) connectionTable(result any) error {
	if err := a.print(nested(result, "connection")); err != nil {
		return err
	}
	if credentials, ok := nested(result, "credentials").([]any); ok {
		if err := a.table(credentials); err != nil {
			return err
		}
	}
	if pem, _ := nested(result, "caCertificate").(string); pem != "" {
		if _, err := fmt.Fprintln(a.opts.Out, "\nCA certificate:"); err != nil {
			return err
		}
		return printPEM(a.opts.Out, pem)
	}
	return nil
}

func (a *app) statusTable(result any) error {
	m, ok := result.(map[string]any)
	if !ok {
		return a.print(result)
	}
	summary := make(map[string]any, len(m))
	for key, value := range m {
		if key != "conditions" && key != "members" {
			summary[key] = value
		}
	}
	if err := a.print(summary); err != nil {
		return err
	}
	for _, key := range []string{"conditions", "members"} {
		if rows, ok := m[key].([]any); ok && len(rows) > 0 {
			if _, err := fmt.Fprintf(a.opts.Out, "\n%s:\n", key); err != nil {
				return err
			}
			if err := a.table(rows); err != nil {
				return err
			}
		}
	}
	return nil
}
