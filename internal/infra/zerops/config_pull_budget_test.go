// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
	"time"
)

// Agents have completed one full, freshly resolved configuration before the
// idle measurement starts. Retain those revisions across the simulated hour.
func (x *steadyBudgetFixture) configPullBaseline(t *testing.T) map[string]string {
	t.Helper()
	revisions := make(map[string]string, len(x.members))
	for _, member := range x.members {
		revision := engine.ConfigPullRevision(x.rc, member, nil, [32]byte{})
		if revision == "" {
			t.Fatal("missing steady configuration baseline", x.kind, member)
		}
		revisions[member] = revision
	}
	return revisions
}

func (x *steadyBudgetFixture) idleConfigPulls(t *testing.T, revisions map[string]string) int {
	t.Helper()
	pulls := 0
	for _, member := range x.members {
		current := engine.ConfigPullRevision(x.rc, member, nil, [32]byte{})
		for range x.instances[member] {
			_, unchanged, err := engine.PullMemberConfig(revisions[member], current, func() (agentapi.MemberConfig, error) {
				t.Fatalf("M4 %s %s rendered unchanged configuration", x.kind, member)
				return agentapi.MemberConfig{}, nil
			})
			if err != nil || !unchanged {
				t.Fatal("conditional idle pull failed", err)
			}
			pulls++
		}
	}
	return pulls
}

func TestPostgresConfigNoticeGlobalFallback(t *testing.T) {
	server, _ := newFakeAPIWithState(t)
	b := newBudgetBackend(t, server.URL, &providerGETCounts{requests: map[string]int{}, signed: map[string]int{}})
	now := time.Now()
	defer infra.SetSnapshotClockForTest(func() time.Time { return now })()
	snapshot := b.PlatformSnapshot()
	if !snapshot.EnsureListing(t.Context()) {
		t.Fatal("provider listing unavailable")
	}
	rc := &engine.Context{Backend: b, Cluster: &domain.Cluster{Internal: []byte(`{"postgres":{"members":[{"name":"peer"}]}}`)}}
	if _, scoped := engine.ClusterListingGeneration(b, rc.Cluster); scoped {
		t.Fatal("Zerops unexpectedly dropped its conservative global fallback")
	}
	fingerprint := "before"
	snapshot.SetListing(func(context.Context) (string, error) { return fingerprint, nil })
	now = now.Add(infra.SnapshotListingInterval)
	if !snapshot.EnsureListing(t.Context()) {
		t.Fatal("fallback listing unavailable")
	}
	before, content := engine.ConfigNoticeRevisions(rc, "payload", true)
	certificate := engine.ConfigPullRevision(rc, "proxy", nil, [32]byte{})
	fingerprint = "foreign-drift"
	now = now.Add(infra.SnapshotListingInterval)
	if !snapshot.EnsureListing(t.Context()) {
		t.Fatal("fallback drift unavailable")
	}
	after, nextContent := engine.ConfigNoticeRevisions(rc, "payload", true)
	if before == after || content == nextContent || certificate == engine.ConfigPullRevision(rc, "proxy", nil, [32]byte{}) {
		t.Fatal("global fallback hid target drift from notices or certificates")
	}
}
