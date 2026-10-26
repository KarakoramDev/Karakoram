// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestImperativeAdmissionWaitsForFrozenImportRecovery(t *testing.T) {
	for _, kind := range []string{"initial-plan", "runtime-plan", "runtime-handle"} {
		t.Run(kind, func(t *testing.T) {
			p, rc, _ := setup(t, "single")
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			s.allocate(rc.Cluster)
			m := s.data()[0]
			switch kind {
			case "initial-plan":
				m.ImportBatch = &engine.ImportPlan{}
			case "runtime-plan":
				m.RuntimeRecovery.ImportPlan = &engine.ImportPlan{}
			case "runtime-handle":
				m.RuntimeRecovery.Op = &infra.Op{ID: "runtime-recovery"}
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			p = New(p.options)
			if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || allowed {
				t.Fatalf("imperative operation preempted durable import %s: %t %v", kind, allowed, err)
			}
			m.ImportBatch = nil
			m.RuntimeRecovery.ImportPlan = nil
			m.RuntimeRecovery.Op = nil
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || !allowed {
				t.Fatalf("completed recovery kept the queue fenced: %t %v", allowed, err)
			}
		})
	}
}

func TestReplacementJoinContinuesThroughItsFrozenImport(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.Topology = "ha"
	s.allocate(rc.Cluster)
	next := &member{Name: "demop3", Role: "data", Stage: "planned", ImportBatch: &engine.ImportPlan{}}
	s.Members = append(s.Members, next)
	s.Replacement = &replacementState{Operation: "replacement-owner", Old: "demop1", New: next.Name, Stage: "join"}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || allowed {
		t.Fatalf("a new operation may preempt frozen replacement import: %t %v", allowed, err)
	}
	result, err := p.RunOperation(t.Context(), rc, &domain.Operation{ID: "replacement-owner", Kind: domain.OperationReplaceMember})
	if err != nil || result.Done || result.Step != "join" {
		t.Fatalf("matching replacement join was failed by its own provisioning receipt: %+v %v", result, err)
	}
	if _, err := p.RunOperation(t.Context(), rc, &domain.Operation{ID: "unrelated", Kind: domain.OperationReplaceMember}); err == nil {
		t.Fatal("unrelated replacement bypassed frozen-import admission")
	}
}
