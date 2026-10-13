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
				g = &cobra.Command{Use: word, Short: "Manage " + prefix}
				parent.AddCommand(g)
				groups[prefix] = g
			}
			parent = g
		}
		use := strings.Join(words[end-1:], " ")
		c := &cobra.Command{Use: use, Short: commandDescription(s), Args: cobra.ExactArgs(s.args)}
		if strings.Contains(s.path, "[") {
			c.Args = cobra.RangeArgs(s.args, s.args+1)
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
		mutable := strings.HasPrefix(s.path, "clusters ") && (s.body || strings.HasPrefix(s.operation, "delete") || strings.HasPrefix(s.operation, "resume") || strings.HasPrefix(s.operation, "restart") || strings.HasPrefix(s.operation, "replace") || strings.HasPrefix(s.operation, "rotate"))
		if mutable {
			c.Flags().Bool("wait", false, "Wait for operation completion and observed cluster generation")
			c.Flags().Duration("timeout", 30*time.Minute, "Maximum wait duration")
			c.Annotations[operationAnnotation] += ",getOperation,getCluster"
		}
		a.specialFlags(c, s)
		c.RunE = func(cmd *cobra.Command, args []string) error { return a.run(cmd, s, args) }
		parent.AddCommand(c)
	}
	a.addAuth(root)
}
func commandDescription(s commandSpec) string {
	if s.body {
		return s.operation + ": submit YAML/JSON with -f (see API request schema)"
	}
	return s.operation
}
func (a *app) run(cmd *cobra.Command, s commandSpec, args []string) error {
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
	if s.operation == "streamEvents" {
		return a.watch(cmd.Context(), q)
	}
	if strings.HasPrefix(s.path, "operations wait") {
		timeout, _ := cmd.Flags().GetDuration("timeout")
		return a.waitOperation(cmd.Context(), args[0], timeout, true)
	}
	op := s.operation
	if s.operation == "listClusterMembers" && len(args) == 2 {
		op = "getClusterMember"
	}
	body, err := a.body(cmd, s, args)
	if err != nil {
		return err
	}
	if s.operation == "getClusterConnection" {
		reveal, _ := cmd.Flags().GetBool("reveal")
		if !reveal {
			return fmt.Errorf("connection credentials are sensitive; pass --reveal to display them")
		}
	}
	if s.operation == "createRestore" {
		into, _ := cmd.Flags().GetString("into-new")
		if into != "" {
			return a.clone(cmd, args[0], into, body)
		}
	}
	result, err := a.request(cmd.Context(), op, args, q, body)
	if err != nil {
		return err
	}
	if wait, _ := cmd.Flags().GetBool("wait"); wait {
		timeout, _ := cmd.Flags().GetDuration("timeout")
		return a.waitMutation(cmd.Context(), s.operation, args, result, q, timeout)
	}
	if op == "listOrphanedVolumes" && a.format == "table" {
		return a.orphanedVolumesTable(result)
	}
	return a.print(result)
}
