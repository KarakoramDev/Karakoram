// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// Exercise the engine's persisted deploy intent against the real Nomad adapter,
// not just direct backend deletion (which already knew how to supersede it).
func TestEngineCleanupDeletesNeverPlacedNomadJob(t *testing.T) {
	f, b, deploy := blockedDeployment(t, true)
	// Model resource exhaustion rather than ineligible-node rejection: the
	// volume's node remains healthy/eligible, but its evaluation stays blocked.
	f.mu.Lock()
	for id, n := range f.nodes {
		n.SchedulingEligibility = "eligible"
		f.nodes[id] = n
	}
	f.mu.Unlock()
	r, err := b.load(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	v, err := b.load(t.Context(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	internal, err := json.Marshal(map[string]any{
		"members":        []map[string]any{{"name": "runtime", "volume": "volume", "serviceId": r.ID, "volumeId": v.ID, "stage": "imported", "role": "mongod", "set": "rs0"}},
		"pending":        map[string]any{"op": deploy, "action": "deploy", "member": "runtime"},
		"cleanupBackups": true,
	})
	if err != nil {
		t.Fatal(err)
	}
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	cluster := &domain.Cluster{Name: "demo", Deleting: true, Internal: internal}
	p := psmdb.New(psmdb.Options{Store: st})
	p.ApplyDefaults(&cluster.Spec)
	rc := &engine.Context{Cluster: cluster, Backend: b, Store: st, Secrets: st}
	for range 30 {
		done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(cluster.Status.Message, "waiting for platform deploy") {
			t.Fatal("engine retained obsolete deployment intent")
		}
		if done {
			f.mu.Lock()
			remaining := len(f.jobs) + len(f.volumes)
			f.mu.Unlock()
			if remaining != 0 {
				t.Fatalf("backend resources remain: %d", remaining)
			}
			for _, name := range []string{"runtime", "volume"} {
				s, err := b.GetService(t.Context(), name)
				if err != nil || s != nil {
					t.Fatalf("owned service remains: %+v %v", s, err)
				}
			}
			return
		}
	}
	t.Fatal("engine waited for the never-placed deployment instead of deleting")
}
