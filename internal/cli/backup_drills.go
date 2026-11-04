// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"context"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

func backupDrillFlags(cmd *cobra.Command, op string) {
	if op == "createBackupDrill" || op == "getBackupDrill" {
		cmd.Flags().Bool("wait", false, "Wait for the drill report and cleanup to reach a terminal phase")
		cmd.Flags().Duration("timeout", 6*time.Hour, "Maximum client wait duration (does not cancel the drill)")
	}
	if op == "createBackupDrill" {
		cmd.Flags().String("backup", "", "Succeeded backup name (defaults to latest eligible backup)")
		cmd.Annotations[operationAnnotation] += ",getBackupDrill"
	}
	if maintenanceSpecOperation(op) {
		cmd.Flags().Bool("drill-enabled", false, "Enable scheduled backup drills")
		cmd.Flags().String("drill-schedule", "", "UTC five-field cron schedule for backup drills")
		cmd.Flags().String("drill-timeout", "6h", "Server-side drill timeout (30m–48h)")
		cmd.Flags().String("drill-backup-schedule", "", "Restrict drills to backups from this backup schedule")
	}
}

func drillPolicyChanged(cmd *cobra.Command) bool {
	for _, name := range []string{"drill-enabled", "drill-schedule", "drill-timeout", "drill-backup-schedule"} {
		if cmd.Flags().Changed(name) {
			return true
		}
	}
	return false
}

func backupDrillPolicyBody(cmd *cobra.Command, op string, body any) error {
	if !maintenanceSpecOperation(op) || !drillPolicyChanged(cmd) {
		return nil
	}
	if file, _ := cmd.Flags().GetString("file"); file != "" {
		return nil
	}
	envelope, ok := body.(map[string]any)
	if !ok {
		return usageError("drill flags require a cluster spec object")
	}
	spec, ok := envelope["spec"].(map[string]any)
	if !ok {
		return usageError("drill flags require a cluster spec object")
	}
	backup, _ := spec["backup"].(map[string]any)
	if backup == nil {
		backup = map[string]any{}
		spec["backup"] = backup
	}
	policy, _ := backup["drill"].(map[string]any)
	if policy == nil {
		policy = map[string]any{}
		backup["drill"] = policy
	}
	for flag, key := range map[string]string{"drill-schedule": "schedule", "drill-timeout": "timeout", "drill-backup-schedule": "backupSchedule"} {
		if cmd.Flags().Changed(flag) {
			value, _ := cmd.Flags().GetString(flag)
			policy[key] = value
		}
	}
	if cmd.Flags().Changed("drill-enabled") {
		value, _ := cmd.Flags().GetBool("drill-enabled")
		policy["enabled"] = value
	}
	return nil
}

func (a *app) printBackupDrill(result any, list bool) error {
	if a.format != "table" {
		return a.print(result)
	}
	rows := []any{result}
	if list {
		rows, _ = nested(result, "items").([]any)
	}
	w := tabwriter.NewWriter(a.opts.Out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tPHASE\tBACKUP\tTRIGGER\tRESTORE (s)\tDATA AGE (s)\tCREATED")
	for _, row := range rows {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", cell(nested(row, "id")), cell(nested(row, "phase")), cell(nested(row, "backup")), cell(nested(row, "trigger")), cell(nested(row, "restoreDurationSeconds")), cell(nested(row, "dataAgeSeconds")), cell(nested(row, "createdAt")))
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if list {
		return nil
	}
	fmt.Fprintf(a.opts.Out, "Target: %s\nStarted: %s\nReady: %s\nCompleted: %s\nTotal duration (s): %s\nLimitations: %s\nCleanup: %s\nOrphaned volumes: %s\nError: %s\n", cell(nested(result, "targetCluster")), cell(nested(result, "startedAt")), cell(nested(result, "readyAt")), cell(nested(result, "completedAt")), cell(nested(result, "totalDurationSeconds")), cell(nested(result, "limitations")), cell(nested(result, "cleanup", "phase")), cell(nested(result, "cleanup", "orphanedVolumes")), cell(nested(result, "error")))
	fmt.Fprintln(a.opts.Out, "Checks:")
	checks, _ := nested(result, "checks").([]any)
	for _, check := range checks {
		fmt.Fprintf(a.opts.Out, "  %s: passed=%s %s\n", cell(nested(check, "name")), cell(nested(check, "passed")), cell(nested(check, "message")))
	}
	return nil
}

func (a *app) waitBackupDrill(ctx context.Context, cluster string, result any, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	id, _ := nested(result, "id").(string)
	if id == "" {
		return fmt.Errorf("drill response is missing its id")
	}
	fmt.Fprintf(a.opts.Err, "Waiting for backup drill %s.\n", id)
	resume := fmt.Sprintf("karactl --server %q --namespace %q backup-drills get %q %q --wait --timeout %s", a.server, a.namespace, cluster, id, timeout)
	last := ""
	lastPrinted := time.Time{}
	for {
		phase, _ := nested(result, "phase").(string)
		if phase != last || time.Since(lastPrinted) >= 15*time.Second {
			fmt.Fprintf(a.opts.Err, "Backup drill %s: %s\n", id, phase)
			last, lastPrinted = phase, time.Now()
		}
		switch phase {
		case "Succeeded", "Failed":
			if err := a.printBackupDrill(result, false); err != nil {
				return err
			}
			if phase == "Failed" {
				return fmt.Errorf("backup drill %s failed: %s", id, cell(nested(result, "error")))
			}
			return nil
		}
		observed, err := a.observeArgs(ctx, "getBackupDrill", []string{cluster, id})
		if err != nil {
			return waitFailure(ctx, err, resume)
		}
		result = observed
		next, _ := nested(result, "phase").(string)
		if next != "Succeeded" && next != "Failed" {
			if err := pollPause(ctx); err != nil {
				return waitFailure(ctx, err, resume)
			}
		}
	}
}
