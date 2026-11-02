// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestCancelledFreshCoverageWaiterDoesNotRestartRefresh(t *testing.T) {
	fixture := delayedCoverageFixture(t, 500)
	fixture.delay.Store(int64(20 * time.Millisecond))
	agent := fixture.agent(t, t.TempDir())
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	_, err := agent.poll(ctx, true)
	cancel()
	if err == nil {
		t.Fatal("cold fresh waiter unexpectedly completed")
	}
	polls := waitRefresh(t, agent, 500, 15*time.Second)
	if got := fixture.gets.Load(); got != 500 {
		t.Fatalf("waiter cancellation restarted history: GETs=%d", got)
	}
	t.Logf("cancelledFreshWaiter continued to500 sidecars in%d polls without refetch", polls)
}

func TestLegacyCoverageClientReceivesWindowsOnlyAfterVerification(t *testing.T) {
	fixture := delayedCoverageFixture(t, 20)
	fixture.delay.Store(int64(20 * time.Millisecond))
	agent := fixture.agent(t, t.TempDir())
	legacyWindows := func(response myspec.PITRCoverageResponse) []myspec.PITRWindow {
		t.Helper()
		body, err := json.Marshal(response)
		if err != nil {
			t.Fatal(err)
		}
		var legacy struct {
			Windows []myspec.PITRWindow `json:"windows,omitempty"`
		}
		if err := json.Unmarshal(body, &legacy); err != nil {
			t.Fatal(err)
		}
		return legacy.Windows
	}
	if _, err := agent.pollRequest(t.Context(), agent.request); err == nil {
		t.Fatal("legacy pending request succeeded instead of returning observation error")
	}
	unnegotiatedFresh := agent.request
	unnegotiatedFresh.RequireFresh = true
	if _, err := agent.pollRequest(t.Context(), unnegotiatedFresh); err == nil {
		t.Fatal("unnegotiated fresh waiter returned success")
	}
	response, err := agent.poll(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	if !response.Pending || len(legacyWindows(response)) != 0 {
		t.Fatalf("legacy client saw an unverified window: %+v", response)
	}
	waitRefresh(t, agent, 20, 5*time.Second)
	response, err = agent.pollRequest(t.Context(), agent.request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Pending || len(legacyWindows(response)) == 0 || response.VerificationStartedAt.IsZero() {
		t.Fatalf("legacy client lost completed coverage: %+v", response)
	}
	service, err := agent.runner.coverageService()
	if err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	service.entries[coverageRequestKey(agent.request)].verified.VerifiedAt = time.Now().Add(-2 * coverageLease)
	service.mu.Unlock()
	if _, err := agent.pollRequest(t.Context(), agent.request); err == nil {
		t.Fatal("legacy expired proof succeeded")
	}
}
