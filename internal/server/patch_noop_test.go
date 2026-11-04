// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestIdenticalPatchPreservesInFlightOperation(t *testing.T) {
	s, _, token := testServer(t)
	body := `{"spec":{"maintenance":{"timeZone":"UTC","windows":[{"schedule":"* * * * *","duration":"15m"}]}}}`
	first := call(s, "PATCH", "/api/v1/clusters/prod", body, token)
	if first.Code != 200 {
		t.Fatal(first.Code, first.Body.String())
	}
	id := first.Header().Get("Karakoram-Operation-Id")
	if id == "" {
		t.Fatal("changed patch has no operation")
	}
	ref := domain.ClusterRef{Name: "prod"}
	before, err := s.Store.GetCluster(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	for _, patch := range []string{body, `{"spec":{}}`} {
		response := call(s, "PATCH", "/api/v1/clusters/prod", patch, token)
		if response.Code != 200 {
			t.Fatal(response.Code, response.Body.String())
		}
		if got := response.Header().Get("Karakoram-Operation-Id"); got != "" && got != id {
			t.Fatalf("no-op created operation %s", got)
		}
		after, err := s.Store.GetCluster(t.Context(), ref)
		if err != nil || after.Generation != before.Generation {
			t.Fatal("no-op changed generation", after, err)
		}
		operations, err := s.Store.ListOperations(t.Context(), ref)
		if err != nil || len(operations) != 1 || operations[0].ID != id || operations[0].Terminal() {
			t.Fatal("no-op superseded in-flight work", operations, err)
		}
	}
	spy := s.Trigger.(*triggerSpy)
	spy.mu.Lock()
	triggers := len(spy.names)
	spy.mu.Unlock()
	for i := range 2 {
		response := call(s, "PATCH", "/api/v1/clusters/prod?ignoreMaintenanceWindow=true", body, token)
		if response.Code != 200 {
			t.Fatal(response.Code, response.Body.String())
		}
		after, err := s.Store.GetCluster(t.Context(), ref)
		if err != nil || after.Generation != before.Generation || after.MaintenanceOverrideGeneration != after.Generation {
			t.Fatal("unchanged override did not release the accepted generation", after, err)
		}
		operations, err := s.Store.ListOperations(t.Context(), ref)
		if err != nil || len(operations) != 1 || operations[0].ID != id || operations[0].Terminal() {
			t.Fatal("unchanged override replaced in-flight work", operations, err)
		}
		spy.mu.Lock()
		count := len(spy.names)
		spy.mu.Unlock()
		if count != triggers+1 {
			t.Fatal("new override must trigger exactly once, repeated overrides are no-ops", i, count, triggers)
		}
	}
}
