// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"encoding/json"
	"fmt"
	"text/tabwriter"
)

func dryRunOperation(op string) string {
	switch op {
	case "createCluster":
		return "dryRunCreateCluster"
	case "replaceCluster":
		return "dryRunReplaceCluster"
	case "patchCluster":
		return "dryRunPatchCluster"
	case "createRestore":
		return "dryRunCreateRestore"
	default:
		return ""
	}
}

func (a *app) printDryRun(result any) error {
	if a.format != "table" {
		return a.print(result)
	}
	w := tabwriter.NewWriter(a.opts.Out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "Changes:")
	fmt.Fprintln(w, "PATH\tBEFORE → AFTER")
	changes, _ := nested(result, "changes").([]any)
	for _, change := range changes {
		before, err := json.Marshal(nested(change, "before"))
		if err != nil {
			return err
		}
		after, err := json.Marshal(nested(change, "after"))
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "%s\t%s → %s\n", cell(nested(change, "path")), before, after)
	}
	if len(changes) == 0 {
		fmt.Fprintln(w, "(none)")
	}
	fmt.Fprintln(w, "\nEffects:")
	fmt.Fprintln(w, "KIND\tMESSAGE")
	effects, _ := nested(result, "effects").([]any)
	for _, effect := range effects {
		fmt.Fprintf(w, "%s\t%s\n", cell(nested(effect, "kind")), cell(nested(effect, "message")))
	}
	if len(effects) == 0 {
		fmt.Fprintln(w, "(none)")
	}
	fmt.Fprintln(w, "\nWarnings:")
	warnings, _ := nested(result, "warnings").([]any)
	for _, warning := range warnings {
		fmt.Fprintf(w, "- %s\n", cell(warning))
	}
	if len(warnings) == 0 {
		fmt.Fprintln(w, "(none)")
	}
	return w.Flush()
}
