// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
)

func TestAgentConfigServiceReadAttribution(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	x := newSteadyBudgetFixture(t, "postgres", b)
	counts := countProviderGETs(f)
	ctx := infra.WithRequestCaller(t.Context(), infra.CallerAgentConfig)
	before := infra.PlatformCounters()
	if _, err := b.GetService(ctx, x.members[0]); err != nil {
		t.Fatal(err)
	}
	requests := counts.snapshotRequests()
	t.Logf("one joined PostgreSQL peer per proxy configuration: %d GETs %v", requestTotal(requests), requests)
	for _, after := range infra.PlatformCounters() {
		if after.Provider != "dokploy" {
			continue
		}
		for _, old := range before {
			if old.Provider != after.Provider {
				continue
			}
			for i, r := range after.Requests {
				delta := r.Reads - old.Requests[i].Reads
				if r.Caller == infra.CallerAgentConfig {
					if delta != uint64(requestTotal(requests)) || delta == 0 {
						t.Fatalf("agent-config reads=%d requests=%v", delta, requests)
					}
				} else if delta != 0 {
					t.Fatalf("unexpected caller %s reads=%d", r.Caller, delta)
				}
			}
		}
	}
}
