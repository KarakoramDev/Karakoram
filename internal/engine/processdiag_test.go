// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestApplyProcessDiagnostics(t *testing.T) {
	at := time.Now()
	member := domain.MemberStatus{Name: "node", Role: "primary", State: "PRIMARY", Health: true, Agent: domain.AgentStatus{Healthy: true, Message: "native diagnostic"}}
	conditions := ApplyProcessDiagnostics(&member, map[string]agentapi.ProcessStatus{
		"engine": {State: "paused", Error: "private native diagnostic"},
		"backup": {State: "failed", Error: "private launch diagnostic"},
		"normal": {State: "running"},
	}, "instance-2", at)
	if member.State != "paused" || member.Health || !member.Agent.Healthy || member.Role != "primary" || !strings.Contains(member.Agent.Message, "native diagnostic;") {
		t.Fatalf("process health changed reachability or role: %+v", member)
	}
	if len(conditions) != 2 || conditions[0].Type != "Process/node/backup/instance-2" || conditions[0].Reason != "ProcessError" || conditions[1].Reason != "ProcessPaused" || !strings.Contains(conditions[1].Message, "SIGCONT") {
		t.Fatalf("unexpected diagnostics: %+v", conditions)
	}
	for _, condition := range conditions {
		if condition.Status != "False" || !condition.LastTransitionTime.Equal(at) || strings.Contains(condition.Message, "private") || !strings.Contains(condition.Message, "investigate") {
			t.Fatalf("unsafe or incomplete diagnostic: %+v", condition)
		}
	}
	before := member
	if conditions := ApplyProcessDiagnostics(&member, map[string]agentapi.ProcessStatus{"engine": {State: "running"}}, "", at); len(conditions) != 0 || member != before {
		t.Fatal("healthy observation rewrote prior instance failure", member, conditions)
	}
}

func TestApplyAgentDiagnosticPreservesReachability(t *testing.T) {
	for _, connected := range []bool{true, false} {
		member := domain.MemberStatus{Health: true, Agent: domain.AgentStatus{Healthy: connected}}
		ApplyAgentDiagnostic(&member, "publication refused")
		if member.Health || member.Agent.Healthy != connected || !strings.Contains(member.Agent.Message, "Configuration apply: publication refused") {
			t.Fatalf("configuration failure rewrote agent reachability: %+v", member)
		}
	}
}
