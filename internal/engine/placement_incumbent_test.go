// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package engine

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestVotingFailureDomainsWithIneligibleIncumbent(t *testing.T) {
	// New volumes may use A or C, while the incumbent on ineligible B keeps
	// running. Replacing m1 can still restore all three physical failure domains.
	backend := &failureDomainBackend{eligible: []string{"node-a", "node-c"}}
	events := &deploymentGuardEvents{}
	rc := &Context{Backend: backend, Events: events, Cluster: &domain.Cluster{Name: "demo"}}
	status := domain.ClusterStatus{Phase: domain.PhaseReady, Members: []domain.MemberStatus{
		{Name: "m0", PlacementGroup: "node-a"},
		{Name: "m1", PlacementGroup: "node-a"},
		{Name: "m2", PlacementGroup: "node-b"},
	}}
	if err := ObserveFailureDomains(t.Context(), rc, &status, []string{"m0", "m1", "m2"}); err != nil {
		t.Fatal(err)
	}
	condition := status.Conditions[len(status.Conditions)-1]
	if condition.Status != "False" || condition.Reason != "SharedNode" || status.Phase != domain.PhaseDegraded ||
		!strings.Contains(condition.Message, "clusters replace-member demo m1 --wait") || len(events.events) != 0 {
		t.Fatalf("repairable placement hidden by admission eligibility: %+v events=%+v", status, events.events)
	}
}
