// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const operationAnnotation = "operationIds"

type queryFlag struct{ name, key, kind string }
type commandSpec struct {
	path, operation string
	args            int
	body            bool
	query           []queryFlag
}

func annotate(c *cobra.Command, ops ...string) {
	c.Annotations = map[string]string{operationAnnotation: strings.Join(ops, ",")}
}
func (a *app) addCommands(root *cobra.Command) {
	groups := map[string]*cobra.Command{"": root}
	for _, spec := range commandSpecs {
		s := spec
		words := strings.Fields(s.path)
		end := 0
		for end < len(words) && !strings.HasPrefix(words[end], "<") && !strings.HasPrefix(words[end], "[") {
			end++
		}
		parent := root
		prefix := ""
		for _, word := range words[:end-1] {
			prefix = strings.TrimSpace(prefix + " " + word)
			g := groups[prefix]
			if g == nil {
				g = groupCommand(word, commandGroupDescriptions[prefix])
				parent.AddCommand(g)
				groups[prefix] = g
			}
			parent = g
		}
		use := strings.Join(words[end-1:], " ")
		c := &cobra.Command{Use: use, Short: commandDescription(s.path), Args: cobra.ExactArgs(s.args)}
		if strings.Contains(s.path, "[") {
			c.Args = cobra.RangeArgs(s.args, s.args+1)
		}
		validateArgs := c.Args
		c.Args = func(cmd *cobra.Command, args []string) error {
			if err := validateArgs(cmd, args); err != nil {
				return usageError("%s", err)
			}
			return nil
		}
		if s.operation == "deleteAPIToken" {
			c.Aliases = []string{"revoke"}
		}
		if destructive(s.operation) {
			c.Flags().BoolP("yes", "y", false, "Confirm without reading stdin (required with stdin payloads; otherwise pipe y/yes)")
		}
		annotate(c, s.operation)
		if c.Name() == "members" {
			annotate(c, s.operation, "getClusterMember")
		}
		if s.path == "clusters edit <cluster>" {
			annotate(c, s.operation, "getCluster")
		}
		if strings.HasPrefix(s.path, "restores create") {
			annotate(c, s.operation, "getCluster", "createCluster")
		}
		if dryRunOperation(s.operation) != "" {
			c.Flags().Bool("dry-run", false, "Validate and preview changes on the server without applying them")
			c.Annotations[operationAnnotation] += "," + dryRunOperation(s.operation)
		}
		if s.body {
			c.Flags().StringP("file", "f", "", "API request YAML/JSON file ('-' for stdin)")
		}
		for _, q := range s.query {
			switch q.kind {
			case "int":
				c.Flags().Int(q.name, 100, "API "+q.key+" parameter")
			case "bool":
				c.Flags().Bool(q.name, false, "API "+q.key+" parameter")
			default:
				c.Flags().String(q.name, "", "API "+q.key+" parameter")
			}
		}
		mutable := s.operation == "rebalancePostgres" || strings.HasPrefix(s.path, "clusters ") && (s.body || strings.HasPrefix(s.operation, "delete") || strings.HasPrefix(s.operation, "resume") || strings.HasPrefix(s.operation, "restart") || strings.HasPrefix(s.operation, "retry") || strings.HasPrefix(s.operation, "replace") || strings.HasPrefix(s.operation, "rotate"))
		if mutable {
			c.Flags().Bool("wait", false, "Wait for operation completion and observed cluster generation")
			c.Flags().Duration("timeout", 30*time.Minute, "Maximum wait duration")
			c.Annotations[operationAnnotation] += ",getOperation,getCluster"
		}
		a.specialFlags(c, s)
		if s.operation == "listEvents" {
			c.Flags().Lookup("order").Usage = "Event order: asc or desc (server default when omitted)"
		}
		c.RunE = func(cmd *cobra.Command, args []string) error { return a.run(cmd, s, args) }
		parent.AddCommand(c)
	}
	a.addAuth(root)
	a.addConfig(root)
	a.addElastic(root)
	a.addSupabase(root)
	addMCPCommands(a, root)
}
func (a *app) run(cmd *cobra.Command, s commandSpec, args []string) error {
	if s.operation == "getVersion" {
		return a.printVersion(cmd.Context())
	}
	q := map[string]any{}
	for _, f := range s.query {
		if !cmd.Flags().Changed(f.name) {
			continue
		}
		switch f.kind {
		case "int":
			v, _ := cmd.Flags().GetInt(f.name)
			q[f.key] = v
		case "bool":
			v, _ := cmd.Flags().GetBool(f.name)
			q[f.key] = v
		default:
			v, _ := cmd.Flags().GetString(f.name)
			q[f.key] = v
		}
	}
	if order, ok := q["order"].(string); ok && order != "asc" && order != "desc" {
		return usageError("--order must be asc or desc")
	}
	if s.operation == "streamEvents" {
		return a.watch(cmd.Context(), q)
	}
	if strings.HasPrefix(s.path, "operations wait") {
		timeout, _ := cmd.Flags().GetDuration("timeout")
		return a.waitOperation(cmd.Context(), args[0], timeout, true)
	}
	if s.path == "clusters wait <cluster>" {
		timeout, _ := cmd.Flags().GetDuration("timeout")
		return a.waitCluster(cmd.Context(), "observe", args[0], 0, nil, timeout)
	}
	op := s.operation
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	if dryRun {
		if wait, _ := cmd.Flags().GetBool("wait"); wait {
			return usageError("--dry-run cannot be combined with --wait")
		}
		op = dryRunOperation(s.operation)
	}
	if s.operation == "listClusterMembers" && len(args) == 2 {
		op = "getClusterMember"
	}
	if err := a.checkConfirmationInput(cmd, op); err != nil {
		return err
	}
	body, err := a.body(cmd, s, args)
	if err != nil {
		return err
	}
	if err := a.confirm(cmd, op, args); err != nil {
		return err
	}
	if s.operation == "getClusterConnection" && s.path != "clusters ca <cluster>" {
		reveal, _ := cmd.Flags().GetBool("reveal")
		if !reveal {
			return a.connectionSummary(cmd, args)
		}
	}
	if s.operation == "createRestore" {
		into, _ := cmd.Flags().GetString("into-new")
		if into != "" {
			return a.clone(cmd, args[0], into, body)
		}
	}
	if op == "deleteCluster" {
		// Reading is optional: delete-only principals must still be able to delete.
		if current, lookupErr := a.request(cmd.Context(), "getCluster", args, nil, nil); lookupErr == nil && nested(current, "status", "phase") == "Deleting" {
			fmt.Fprintf(a.opts.Err, "Cluster %s is already being deleted.\n", args[0])
		}
	}
	result, err := a.request(cmd.Context(), op, args, q, body)
	if err != nil {
		return err
	}
	if dryRun {
		return a.printDryRun(result)
	}
	if op == "revealOpenBaoUnsealKeys" && a.format == "table" {
		return a.openBaoUnsealKeysTable(result)
	}
	if s.path == "clusters ca <cluster>" {
		pem, _ := nested(result, "caCertificate").(string)
		return printPEM(a.opts.Out, pem)
	}
	if op == "verifyBackupStorage" {
		if err := a.print(result); err != nil {
			return err
		}
		if verified, _ := nested(result, "verified").(bool); !verified {
			return fmt.Errorf("storage verification failed: %s", cell(nested(result, "message")))
		}
		return nil
	}
	if op == "getClusterConnection" && a.format == "table" {
		return a.connectionTable(result)
	}
	if op == "getClusterStatus" && a.format == "table" {
		return a.statusTable(result)
	}
	if wait, _ := cmd.Flags().GetBool("wait"); wait {
		timeout, _ := cmd.Flags().GetDuration("timeout")
		return a.waitMutation(cmd.Context(), s.operation, args, result, q, timeout)
	}
	if op == "listOrphanedVolumes" && a.format == "table" {
		return a.orphanedVolumesTable(result)
	}
	if op == "listNamespaces" && a.format == "table" {
		return a.namespacesTable(result)
	}
	if op == "listBackends" && a.format == "table" {
		return a.backendsTable(result)
	}
	if (op == "getProvider" || op == "listProviders") && a.format == "table" {
		return a.providersTable(result, op == "listProviders")
	}
	if (op == "listAPITokens" || op == "getAPIToken" || op == "createAPIToken") && a.format == "table" {
		return a.tokensTable(result)
	}
	return a.print(result)
}
