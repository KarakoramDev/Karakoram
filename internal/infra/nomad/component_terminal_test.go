// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// Keep core observation unavailable without altering the component's real Nomad
// lifecycle. Optional-component failure handling must not depend on core health.
type unavailableCoreBackend struct{ *Backend }

func (b unavailableCoreBackend) GetService(ctx context.Context, name string) (*infra.Service, error) {
	if name != "runtime" {
		return nil, infra.ErrUnavailable
	}
	return b.Backend.GetService(ctx, name)
}

func TestComponentCorrectsTerminalResizeBeforeCoreReadiness(t *testing.T) {
	for _, outcome := range []string{"external-stop", "superseded-rollout"} {
		t.Run(outcome, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			spec := runtimeSpec("runtime", "")
			spec.Resources.MinRAM, spec.Resources.MaxRAM = 1, 1
			importFake(t, b, spec)
			deployFake(t, b, "runtime", "")
			f.setAutoTick(false)
			f.stallAllocs(true)
			oversized := spec.Resources
			oversized.MinRAM, oversized.MaxRAM = 2, 2
			op, err := b.SetResources(t.Context(), "runtime", oversized)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := b.OpStatus(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			if outcome == "external-stop" {
				if err := b.c.delete(t.Context(), "/v1/job/runtime", nil, nil); err != nil {
					t.Fatal(err)
				}
				f.mu.Lock()
				f.jobs[fakeKey("default", "runtime")].deployment["Status"] = "cancelled"
				for _, a := range f.allocs {
					f.setAllocStatus(a, "complete")
				}
				f.mu.Unlock()
			} else {
				f.mu.Lock()
				job := f.jobs[fakeKey("default", "runtime")]
				job.deployment["Status"] = "failed"
				job.row["Version"] = fakeInt(job.row, "Version") + 1
				fakeMap(job.row["Meta"])["karakoram.operation"] = "superseding-operation"
				f.mu.Unlock()
			}
			r, err := b.load(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			internal, err := json.Marshal(map[string]any{"supabase": map[string]any{
				"members": map[string]any{"storage": map[string]any{"name": "runtime", "component": "storage", "serviceId": r.ID, "stage": "joined", "replicas": 1, "resources": spec.Resources}},
				"pending": map[string]any{"op": op, "component": "storage", "action": "resize", "resources": oversized, "target": domain.ComponentSpec{Replicas: 1, Resources: oversized}},
			}})
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
			c := &domain.Cluster{Name: "demo", Generation: 4, Internal: internal, Status: domain.ClusterStatus{ObservedGeneration: 2}}
			p := supabase.New(postgres.Options{Store: st})
			p.ApplyDefaults(&c.Spec)
			c.Spec.Components.Extra["storage"] = domain.ComponentSpec{Replicas: 1, Resources: spec.Resources}
			if err := st.SetAgentToken(t.Context(), &domain.AgentToken{Namespace: c.Ref().Namespace, Cluster: c.Name, Member: "runtime", Token: "component-owner"}); err != nil {
				t.Fatal(err)
			}
			rc := &engine.Context{Cluster: c, Store: st, Secrets: st}
			for range 8 {
				rc.Backend = unavailableCoreBackend{fakeBackend(t, f, nil)}
				// Core stays unavailable; its independent observation error must
				// not prevent the corrected component receipt from advancing.
				_, _ = p.Sync(t.Context(), rc)
				f.stallAllocs(false)
				f.setAutoTick(true)
			}
			f.mu.Lock()
			job := f.jobs[fakeKey("default", "runtime")]
			stopped := job.row["Stop"] == true
			task := fakeRows(fakeRows(job.row["TaskGroups"])[0]["Tasks"])[0]
			memory := fakeInt(fakeMap(task["Resources"]), "MemoryMB")
			f.mu.Unlock()
			if stopped || memory != 1024 {
				t.Fatalf("terminal resize starved behind core readiness: stopped=%v memory=%d, want running intent/1024", stopped, memory)
			}
			b = fakeBackend(t, f, nil)
			r, err = b.load(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			awaitFake(t, b, encodeOp(*r.Pending))
			if registrations := jobRegistrations(f); registrations != 3 {
				t.Fatalf("corrected component registered %d times, want initial + resize + correction", registrations)
			}
		})
	}
}
