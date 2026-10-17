// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func advanceReplacementOperation(t *testing.T, p *Core, rc *engine.Context, op *domain.Operation) engine.OpResult {
	t.Helper()
	result, err := p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = result.Checkpoint
	if p.SyncDuringOperation(op) {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	return result
}

func TestReplacementFailureRecoveryBeforeAndAfterCutover(t *testing.T) {
	for _, failure := range []string{"unprovisioned", "joining", "fencing", "remove-etcd", "lost-remove-response", "promote", "retire"} {
		t.Run(failure, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			converge(t, p, rc)
			op := &domain.Operation{ID: "failed-replacement", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"demop1"}`)}
			beforeCutover := failure == "unprovisioned" || failure == "joining"
			reached := false
			for range 120 {
				advanceReplacementOperation(t, p, rc, op)
				s, _ := load(rc.Cluster)
				cp := s.Replacement
				if cp == nil {
					t.Fatal("replacement completed before failure injection")
				}
				old, next := s.find(cp.Old), s.find(cp.New)
				switch failure {
				case "unprovisioned":
					reached = cp.Stage == "join" && next.ServiceID == ""
				case "joining":
					reached = cp.Stage == "join" && next.ServiceID != ""
				case "fencing":
					reached = cp.Stage == "fence" && old.Retirement != ""
				case "lost-remove-response":
					reached = cp.Stage == "remove-etcd" && old.Retirement == "stopped"
					if reached {
						// The side effect committed but its response/state save was lost.
						delete(b.etcd, old.EtcdID)
					}
				default:
					reached = cp.Stage == failure
				}
				if reached {
					break
				}
			}
			if !reached {
				t.Fatal("did not reach failure checkpoint", failure)
			}
			if !p.SupportsFailureCleanup(op) {
				t.Fatal("worker cannot clean terminal replacement failures")
			}
			result, err := p.CancelOperation(t.Context(), rc, op)
			if err != nil || !result.Done {
				t.Fatal("failed to transfer durable recovery to Sync", result, err)
			}
			s, _ := load(rc.Cluster)
			wantRecovery := "complete"
			if beforeCutover {
				wantRecovery = "rollback"
			}
			if s.Replacement == nil || s.Replacement.Recovery != wantRecovery {
				t.Fatal("partially executed journal cleared or wrong recovery direction", s.Replacement)
			}
			for range 100 {
				p = New(p.options)
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				s, _ = load(rc.Cluster)
				if s.Replacement == nil {
					break
				}
			}
			if s.Replacement != nil || len(s.data()) != 3 || len(b.etcd) != 3 {
				t.Fatal("terminal replacement journal did not recover", s.Replacement, len(s.data()), len(b.etcd))
			}
			retired, kept := "demop1", "demop3"
			if beforeCutover {
				retired, kept = kept, retired
			}
			if s.find(kept) == nil || b.services[kept] == nil || b.services[kept+"v"] == nil || s.find(retired) != nil || b.services[retired] != nil || b.services[retired+"v"] != nil {
				t.Fatal("recovery retired the wrong identity or leaked infrastructure", kept, retired)
			}
			// A terminal operation must not leave an owner blocking the next one.
			converge(t, p, rc)
			op = &domain.Operation{ID: "next-replacement", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"` + kept + `"}`)}
			done := false
			for range 150 {
				if advanceReplacementOperation(t, p, rc, op).Done {
					done = true
					break
				}
			}
			if !done {
				t.Fatal("new replacement blocked after failure recovery")
			}
		})
	}
}
