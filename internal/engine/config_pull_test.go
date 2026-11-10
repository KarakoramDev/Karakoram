// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
	"time"
)

type configRevisionBackend struct {
	infra.Backend
	snapshot *infra.TargetSnapshot
}

func (b configRevisionBackend) PlatformSnapshot() *infra.TargetSnapshot { return b.snapshot }
func (b configRevisionBackend) Layout() infra.Layout                    { return infra.Layout{} }

func TestConfigPullRevision(t *testing.T) {
	now := time.Now()
	defer infra.SetSnapshotClockForTest(func() time.Time { return now })()
	snapshot := infra.PlatformSnapshot(t.Name() + "|target")
	fingerprint := "same"
	snapshot.SetListing(func(context.Context) (string, error) { return fingerprint, nil })
	refresh := func() {
		snapshot.Lookup(infra.WithSteadyObservation(infra.ObservationContext(t.Context())), "baseline")
	}
	refresh()
	rc := &Context{Cluster: &domain.Cluster{Generation: 1}, Backend: configRevisionBackend{snapshot: snapshot}, ObservationEpoch: "current"}
	revision := ConfigPullRevision(rc, "member", []string{"capability"}, [32]byte{})
	if revision == "" {
		t.Fatal("missing baseline")
	}
	if ConfigPullRevision(rc, "member", []string{"capability"}, [32]byte{}) != revision {
		t.Fatal("stable revision changed")
	}
	if ConfigPullRevision(rc, "other", []string{"capability"}, [32]byte{}) == revision {
		t.Fatal("member not bound")
	}
	if ConfigPullRevision(rc, "member", nil, [32]byte{}) == revision {
		t.Fatal("capability not bound")
	}
	if ConfigPullRevision(rc, "member", []string{"capability"}, [32]byte{1}) == revision {
		t.Fatal("secret not bound")
	}
	rc.Cluster.Generation++
	if ConfigPullRevision(rc, "member", []string{"capability"}, [32]byte{}) == revision {
		t.Fatal("generation not bound")
	}
	rc.Cluster.Generation--
	mutation := snapshot.BeginMutation(t.Context())
	if ConfigPullRevision(rc, "member", nil, [32]byte{}) != "" {
		t.Fatal("in-flight mutation certified")
	}
	mutation()
	if ConfigPullRevision(rc, "member", []string{"capability"}, [32]byte{}) == revision {
		t.Fatal("mutation not bound")
	}
	now = now.Add(infra.SnapshotListingInterval)
	if ConfigPullRevision(rc, "member", nil, [32]byte{}) != "" {
		t.Fatal("stale listing certified")
	}
	fingerprint = "changed"
	refresh()
	if ConfigPullRevision(rc, "member", []string{"capability"}, [32]byte{}) == revision {
		t.Fatal("drift not bound")
	}
}

func TestPullMemberConfigFreshGate(t *testing.T) {
	for _, pair := range [][2]string{{"", "current"}, {"old", "current"}, {"current", ""}} {
		calls := 0
		_, unchanged, err := PullMemberConfig(pair[0], pair[1], func() (agentapi.MemberConfig, error) { calls++; return agentapi.MemberConfig{}, nil })
		if err != nil || unchanged || calls != 1 {
			t.Fatal("fresh rendering skipped", pair, err)
		}
	}
}
