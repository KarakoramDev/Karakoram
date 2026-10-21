// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
	"strings"
)

func (a *app) controlPlaneBackupPolicyBody(cmd *cobra.Command) (any, error) {
	enabled, _ := cmd.Flags().GetBool("enabled")
	storage, _ := cmd.Flags().GetString("storage")
	schedule, _ := cmd.Flags().GetString("schedule")
	retain, _ := cmd.Flags().GetInt("retain-count")
	if retain < 1 || retain > 1000 {
		return nil, usageError("--retain-count must be between 1 and 1000")
	}
	if _, err := cron.ParseStandard(schedule); err != nil {
		return nil, usageError("--schedule must be a five-field cron expression")
	}
	namespace := a.namespace
	if namespace == "" {
		namespace = "default"
	}
	name := storage
	if strings.Contains(storage, "/") {
		namespace, name, _ = strings.Cut(storage, "/")
	}
	if namespace == "" || name == "" || strings.Contains(name, "/") {
		return nil, usageError("--storage must be [namespace/]name")
	}
	return map[string]any{"enabled": enabled, "storage": map[string]string{"namespace": namespace, "name": name}, "schedule": schedule, "retainCount": retain}, nil
}
