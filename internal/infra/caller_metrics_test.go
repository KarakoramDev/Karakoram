// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra

import (
	"context"
	"testing"
)

func TestKnownPlatformCallerMetrics(t *testing.T) {
	for _, provider := range []string{"dokploy", "zerops", "nomad"} {
		t.Run(provider, func(t *testing.T) {
			counters := &platformCounters{}
			s := &TargetSnapshot{metrics: counters}
			for _, caller := range requestCallers[1:] {
				ctx := WithRequestCaller(t.Context(), caller)
				s.ReadStart(ctx)
				s.BeginMutation(ctx)()
				i := requestCallerIndex(ctx)
				if got := counters.requests[i].reads.Load(); got != 1 {
					t.Fatalf("%s reads=%d", caller, got)
				}
				if got := counters.requests[i].mutations.Load(); got != 1 {
					t.Fatalf("%s mutations=%d", caller, got)
				}
			}
			s.SetListing(func(ctx context.Context) (string, error) {
				if RequestCaller(ctx) != CallerListing {
					t.Fatalf("listing caller=%q", RequestCaller(ctx))
				}
				s.ReadStart(ctx)
				return "unchanged", nil
			})
			ctx := WithSteadyObservation(ObservationContext(WithRequestCaller(t.Context(), CallerAvailability)))
			if !s.ensureListing(ctx) {
				t.Fatal("listing failed")
			}
			s.Populate("steady", s.ReadStart(ctx), SnapshotNow(), []byte("body"))
			if _, ok := s.Lookup(ctx, "steady"); !ok {
				t.Fatal("steady snapshot missed")
			}
			if got := &counters.requests[0]; got.reads.Load() != 0 || got.hits.Load() != 0 || got.mutations.Load() != 0 {
				t.Fatal("known caller produced other")
			}
		})
	}
}
