// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package elastic

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
)

func TestRecoveryFencesCleanupAndImperativeOperations(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(map[bool]string{false: "delete", true: "suspend"}[keep], func(t *testing.T) {
			p, rc, backend, es := coreSetup(t, 1)
			s := coreConverge(t, p, rc, backend)
			s.Members[0].RuntimeRecovery.Op = &infra.Op{ID: "accepted-start"}
			s.Members[0].RuntimeRecovery.Action = "start"
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			p = New(p.options)
			backend.done = false
			before, nativeBefore := backend.mutations, len(es.mutations)
			if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || allowed {
				t.Fatal("recovery did not fence claim", allowed, err)
			}
			out, err := p.RunOperation(t.Context(), rc, &domain.Operation{Kind: domain.OperationRestart})
			if err != nil || out.Done || backend.mutations != before || len(es.mutations) != nativeBefore {
				t.Fatal("operation raced recovery", out, err)
			}
			rc.Cluster.Deleting = true
			done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: keep, KeepBackups: true})
			if err != nil || done || backend.mutations != before || len(es.mutations) != nativeBefore {
				t.Fatal("cleanup raced recovery", done, err)
			}
			got, _ := load(rc.Cluster)
			if got.Members[0].RuntimeRecovery.Op == nil || got.Suspended {
				t.Fatal("accepted journal lost or suspension finalized")
			}
			backend.done = true
			done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: keep, KeepBackups: true})
			if err != nil || done || backend.mutations != before {
				t.Fatal("terminal observation combined lifecycle mutation", done, err)
			}
			got, _ = load(rc.Cluster)
			if got.Members[0].RuntimeRecovery.Op != nil {
				t.Fatal("terminal start not released")
			}
		})
	}
}
