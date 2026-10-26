// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
)

func TestRecoveryFencesCleanupAndImperativeOperations(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(map[bool]string{false: "delete", true: "suspend"}[keep], func(t *testing.T) {
			p, rc, backend, _ := setup(t)
			ready(t, p, rc)
			s := state(t, rc)
			s.Members[0].RuntimeRecovery.Op = &infra.Op{ID: "accepted-start"}
			s.Members[0].RuntimeRecovery.Action = "start"
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			p = New(p.options)
			backend.done = false
			before := len(backend.log.snapshot())
			if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || allowed {
				t.Fatal("recovery did not fence claim", allowed, err)
			}
			for _, kind := range []string{domain.OperationRestart, domain.OperationRestore} {
				out, err := p.RunOperation(t.Context(), rc, &domain.Operation{Kind: kind})
				if err != nil || out.Done || len(backend.log.snapshot()) != before {
					t.Fatal("operation raced recovery", out, err)
				}
			}
			rc.Cluster.Deleting = true
			done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: keep, KeepBackups: true})
			if err != nil || done || len(backend.log.snapshot()) != before {
				t.Fatal("cleanup raced recovery", done, err)
			}
			got := state(t, rc)
			if got.Members[0].RuntimeRecovery.Op == nil || got.Suspended {
				t.Fatal("accepted journal lost or suspension finalized")
			}
			backend.done = true
			done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: keep, KeepBackups: true})
			if err != nil || done || len(backend.log.snapshot()) != before {
				t.Fatal("terminal observation combined lifecycle mutation", done, err)
			}
			if state(t, rc).Members[0].RuntimeRecovery.Op != nil {
				t.Fatal("terminal start not released")
			}
		})
	}
}
