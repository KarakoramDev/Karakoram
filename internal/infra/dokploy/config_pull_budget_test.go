// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"testing"
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
