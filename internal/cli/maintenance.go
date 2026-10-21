// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"github.com/spf13/cobra"
	"strings"
)

func maintenanceOverrideOperation(op string) bool {
	switch op {
	case "restartCluster", "rotateClusterTLS", "replaceClusterMember", "raiseClusterFCV", "replaceCluster", "patchCluster", "upgradeClusterAgents", "moveClusterMember":
		return true
	}
	return false
}
func maintenanceSpecOperation(op string) bool {
	return op == "createCluster" || op == "replaceCluster" || op == "patchCluster"
}
func maintenanceFlags(cmd *cobra.Command, op string) {
	if maintenanceOverrideOperation(op) && cmd.Flags().Lookup("ignore-maintenance-window") == nil {
		cmd.Flags().Bool("ignore-maintenance-window", false, "Permit this operation or desired generation outside maintenance windows")
	}
	if maintenanceSpecOperation(op) {
		cmd.Flags().String("maintenance-time-zone", "UTC", "IANA time zone for maintenance window starts")
		cmd.Flags().StringArray("maintenance-window", nil, "Maintenance start and elapsed duration: 'CRON;DURATION' (repeatable; empty clears windows)")
		cmd.Long += "\nMaintenance in a request file: spec: {maintenance: {timeZone: UTC, windows: [{schedule: '0 2 * * 0', duration: 2h}]}}.\nAn empty windows list permits disruption at any time."
	}
}

func maintenanceBody(cmd *cobra.Command, op string, body any) error {
	if !maintenanceSpecOperation(op) || (!cmd.Flags().Changed("maintenance-window") && !cmd.Flags().Changed("maintenance-time-zone")) {
		return nil
	}
	if file, _ := cmd.Flags().GetString("file"); file != "" {
		return nil // A request file remains authoritative over convenience flags.
	}
	envelope, ok := body.(map[string]any)
	if !ok {
		return usageError("maintenance flags require a cluster spec object")
	}
	spec, ok := envelope["spec"].(map[string]any)
	if !ok {
		return usageError("maintenance flags require a cluster spec object")
	}
	zone, _ := cmd.Flags().GetString("maintenance-time-zone")
	policy, _ := spec["maintenance"].(map[string]any)
	if policy == nil {
		policy = map[string]any{}
	}
	if cmd.Flags().Changed("maintenance-time-zone") || policy["timeZone"] == nil {
		policy["timeZone"] = zone
	}
	if cmd.Flags().Changed("maintenance-window") {
		values, _ := cmd.Flags().GetStringArray("maintenance-window")
		windows := make([]any, 0, len(values))
		for _, value := range values {
			if value == "" {
				continue
			}
			schedule, duration, ok := strings.Cut(value, ";")
			if !ok {
				return usageError("maintenance window must be 'CRON;DURATION'")
			}
			windows = append(windows, map[string]any{"schedule": strings.TrimSpace(schedule), "duration": strings.TrimSpace(duration)})
		}
		policy["windows"] = windows
	}
	spec["maintenance"] = policy
	return nil
}
