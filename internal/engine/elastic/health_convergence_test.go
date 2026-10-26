// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestSyncHealthReasonWithConvergedGeneration(t *testing.T) {
	for _, name := range []string{"initializing", "relocating", "yellow", "red", "probe", "placement-probe"} {
		t.Run(name, func(t *testing.T) {
			p, rc, f, es := coreSetup(t, 3)
			coreConverge(t, p, rc, f)
			rc.Cluster.Generation = 8
			rc.Cluster.Status.ObservedGeneration = 8
			rc.Cluster.Status.Phase = domain.PhaseReady
			es.observationFailures = map[string]bool{}
			want := ""
			wantError := false
			switch name {
			case "initializing":
				es.initializing = 2
				want = "Elasticsearch convergence: waiting for initializing or relocating shards; initializing=2, relocating=0, unassigned=0, active=0, activePrimaries=0, dataNodes=3"
			case "relocating":
				es.relocating = 1
				want = "Elasticsearch convergence: waiting for initializing or relocating shards; initializing=0, relocating=1, unassigned=0, active=0, activePrimaries=0, dataNodes=3"
			case "yellow":
				es.health, es.unassigned, es.replicas = "yellow", 1, 1
				es.shardRows = []map[string]string{
					{"index": "logs", "shard": "0", "state": "STARTED", "prirep": "p", "node": "demon0"},
					{"index": "logs", "shard": "0", "state": "UNASSIGNED", "prirep": "r"},
				}
				want = "Elasticsearch convergence: yellow shard health: unassigned shards are not proven unplaceable replicas; initializing=0, relocating=0, unassigned=1, active=0, activePrimaries=0, dataNodes=3"
			case "red":
				es.health, es.unassigned = "red", 2
				want = "Elasticsearch convergence: red shard health; initializing=0, relocating=0, unassigned=2, active=0, activePrimaries=0, dataNodes=3"
			case "probe":
				es.observationFailures["/_cluster/health"] = true
				want = "Elasticsearch health probe failed; shard health is unavailable"
				wantError = true
			case "placement-probe":
				es.health, es.unassigned = "yellow", 1
				es.observationFailures["/_cat/shards"] = true
				want = "Elasticsearch health probe failed while checking unassigned replica placement; initializing=0, relocating=0, unassigned=1, active=0, activePrimaries=0, dataNodes=3"
				wantError = true
			}
			_, err := p.Sync(t.Context(), rc)
			if (err != nil) != wantError {
				t.Fatalf("Sync error=%v, wantError=%v", err, wantError)
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if status.Phase != domain.PhaseUpdating || !strings.HasPrefix(status.Message, want) || rc.SyncWait != want || status.ObservedGeneration != 8 || rc.Cluster.Generation != 8 {
				t.Fatalf("status=%+v, wait=%q, want=%q", status, rc.SyncWait, want)
			}
			if strings.Contains(status.Message, "native observation deadline exceeded") {
				t.Fatal("raw probe response leaked into status")
			}
			es.health, es.initializing, es.relocating, es.unassigned = "green", 0, 0, 0
			es.observationFailures = nil
			coreConverge(t, p, rc, f)
			status, err = p.Status(t.Context(), rc)
			if err != nil || status.Phase != domain.PhaseReady || rc.SyncWait != "" || strings.Contains(status.Message, "Elasticsearch convergence:") || status.ObservedGeneration != 8 {
				t.Fatalf("recovered status=%+v, wait=%q, err=%v", status, rc.SyncWait, err)
			}
		})
	}
}
