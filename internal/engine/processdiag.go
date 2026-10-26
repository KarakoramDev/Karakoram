// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// ApplyProcessDiagnostics projects supervised child failures independently of
// agent reachability and observed database role. Call after native observations
// so they cannot hide a paused child. An empty instance denotes the logical
// member observation; pinned observations should supply their instance ID.
// The caller owns aggregation of the returned conditions for the current sweep.
func ApplyProcessDiagnostics(member *domain.MemberStatus, processes map[string]agentapi.ProcessStatus, instance string, at time.Time) []domain.Condition {
	var conditions []domain.Condition
	for _, name := range slices.Sorted(maps.Keys(processes)) {
		process := processes[name]
		if process.State != "paused" && process.Error == "" {
			continue
		}
		kind := "Process/" + member.Name + "/" + name
		subject := fmt.Sprintf("Member %s process %s", member.Name, name)
		if instance != "" {
			kind += "/" + instance
			subject += " instance " + instance
		}
		reason := "ProcessError"
		message := subject + " reports an error; investigate the supervised process and agent logs before recovery"
		if process.State == "paused" {
			reason = "ProcessPaused"
			message = subject + " is paused; investigate why it was stopped before resuming the verified process with SIGCONT"
			member.State = "paused"
		} else if member.State != "paused" {
			member.State = "error"
		}
		member.Health = false
		if member.Agent.Message != "" && !strings.Contains(member.Agent.Message, message) {
			member.Agent.Message += "; "
		}
		if !strings.Contains(member.Agent.Message, message) {
			member.Agent.Message += message
		}
		conditions = append(conditions, domain.Condition{Type: kind, Status: "False", Reason: reason, Message: message, LastTransitionTime: at})
	}
	return conditions
}
