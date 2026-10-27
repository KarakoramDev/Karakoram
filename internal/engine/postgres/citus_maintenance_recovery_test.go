// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"testing"
)

func TestMaintenanceCitusScaleInDoesNotBlockRuntimeRecovery(t *testing.T) {
	p, rc, b, _ := setupDistributed(t, 1)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":2}}`)
	converge(t, p, rc)
	before, _ := load(rc.Cluster)
	target := before.group(0).data()[0]
	generation := rc.Cluster.Status.ObservedGeneration
	closeRolloutWindow(rc)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":1}}`)
	rc.Cluster.Generation++
	delete(b.services, target.Name)
	recovered := false
	for range 80 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		s, _ := load(rc.Cluster)
		if b.services[target.Name] != nil && s.find(target.Name).Stage == "joined" {
			recovered = true
			break
		}
	}
	s, _ := load(rc.Cluster)
	if !recovered || len(s.group(2).data()) == 0 {
		t.Fatal("deferred group retirement blocked runtime recovery")
	}
	if rc.Cluster.Status.ObservedGeneration != generation {
		t.Fatal("deferred Citus scale-in acknowledged")
	}
}
