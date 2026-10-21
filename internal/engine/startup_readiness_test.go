// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

func TestClusteredRuntimeStartupReadiness(t *testing.T) {
	for _, name := range []string{"postgres", "supabase", "psmdb", "rabbitmq", "elasticsearch", "openbao"} {
		t.Run(name, func(t *testing.T) {
			b := fake.New(fake.Options{})
			spec := (AgentRuntime{Engine: name, Member: "member", Volume: "data"}).Spec(b.Layout())
			want := []string{"agent", "readiness", "--startup", "--data-dir=" + b.Layout().DataDir(spec.Volume)}
			if !reflect.DeepEqual(spec.Readiness.Args, want) {
				t.Fatalf("deployment probe waits on application readiness: got %v want %v", spec.Readiness.Args, want)
			}
		})
	}
}

func TestStatelessReadinessDecisionAndRetryStability(t *testing.T) {
	now := time.Now()
	healthy := domain.ClusterStatus{ObservedAt: now, ObservedGeneration: 1, Members: []domain.MemberStatus{
		{Name: "database", Health: true, Agent: domain.AgentStatus{Healthy: true}},
		{Name: "frontend", Health: false},
	}}
	rc := &Context{Cluster: &domain.Cluster{Status: healthy}}
	var decision *bool
	native := []string{"--engine=postgres", "--data-dir=/data"}
	args := StatelessReadiness(rc, "frontend", &decision, native...)
	if !reflect.DeepEqual(args, native) || decision == nil || !*decision {
		t.Fatal("healthy dependencies lost native rolling overlap", args, decision)
	}
	rc.Cluster.Status = domain.ClusterStatus{}
	if args := StatelessReadiness(rc, "frontend", &decision, native...); !reflect.DeepEqual(args, native) {
		t.Fatal("retry changed an already selected native probe", args)
	}
	decision = nil // The engine clears this only after native convergence.
	if args := StatelessReadiness(rc, "frontend", &decision, native...); len(args) != 0 {
		t.Fatal("dependency wait retained a native deployment probe", args)
	}
	rc.Cluster.Status = healthy
	if args := StatelessReadiness(rc, "frontend", &decision, native...); len(args) != 0 {
		t.Fatal("bootstrap retry changed its selected startup probe", args)
	}
	decision = nil
	rc.Cluster.Status.ObservedAt = now.Add(-ReadinessMaxAge - time.Second)
	if args := StatelessReadiness(rc, "frontend", &decision, native...); len(args) != 0 {
		t.Fatal("stale dependency health selected native readiness", args)
	}
	decision = nil
	rc.Cluster.Status = healthy
	rc.Cluster.Status.ObservedGeneration = 0
	if args := StatelessReadiness(rc, "frontend", &decision, native...); len(args) != 0 {
		t.Fatal("initial create selected peer-dependent readiness", args)
	}
}

func TestApplyAgentDiagnosticRetainsErrorAndWithdrawsHealth(t *testing.T) {
	member := domain.MemberStatus{Health: true, Agent: domain.AgentStatus{Healthy: true, Message: "native diagnostic"}}
	ApplyAgentDiagnostic(&member, "migration exited with status 1")
	if member.Health || !member.Agent.Healthy || !strings.Contains(member.Agent.Message, "migration exited with status 1") || !strings.Contains(member.Agent.Message, "native diagnostic") {
		t.Fatal("engine Apply failure lost health diagnostic or responding agent reachability", member)
	}
	healthy := domain.MemberStatus{Health: true, Agent: domain.AgentStatus{Healthy: true}}
	ApplyAgentDiagnostic(&healthy, "")
	if !healthy.Health || !healthy.Agent.Healthy || healthy.Agent.Message != "" {
		t.Fatal("empty diagnostic changed member health", healthy)
	}
}
