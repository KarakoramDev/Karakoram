// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type mountedInfra struct{ *retryInfra }

func (f *mountedInfra) OpStatus(context.Context, infra.Op) (infra.OpState, error) {
	return infra.OpState{Failed: true, Message: "This Local Storage volume is still mounted by other services"}, nil
}

func TestFailedDeleteProcessCrossesOrphanThreshold(t *testing.T) {
	p, rc, f, _, s, m, events := orphanFixture(t, "delete-volume")
	rc.Backend = &mountedInfra{retryInfra: f}
	r := s.PlatformRetries[platformRetryKey(s.Retry)]
	r.Failures, r.StillMountedFailures = 4, 4
	s.Pending = &pending{Action: "delete-volume", Member: m.Name, Op: infra.Op{ID: "latest-failed-process"}}
	s.Retry = nil
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sync(context.Background(), rc); err == nil || !strings.Contains(err.Error(), "recorded as orphaned") {
		t.Fatal(err)
	}
	if _, err := p.Sync(context.Background(), rc); err != nil {
		t.Fatal(err)
	}
	items, err := rc.Store.ListOrphanedVolumes(context.Background())
	if err != nil || len(items) != 1 || items[0].Attempts != 5 || items[0].LastProcessID != "latest-failed-process" || items[0].FirstError != "still mounted" || !strings.Contains(items[0].LastError, "other services") || len(events.items) != 1 {
		t.Fatal(items, events.items, err)
	}
	if retryState(t, rc).find(m.Name) != nil {
		t.Fatal("member still present after orphan handoff")
	}
}
