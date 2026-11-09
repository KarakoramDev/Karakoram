// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"testing"
)

func TestIdleReconcileZeroProviderMutations(t *testing.T) {
	for _, topology := range []string{"combined", "split"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, _, _ := fixture(t, topology)
			if result, err := p.Sync(t.Context(), rc); err != nil || result.Steady {
				t.Fatalf("initial work must not be steady: %+v, %v", result, err)
			}
			enginetest.AssertIdle(t, p, rc, nil)
			if result, err := p.Sync(t.Context(), rc); err != nil || !result.Steady || result.RequeueAfter != 0 {
				t.Fatalf("idle result: %+v, %v", result, err)
			}
		})
	}
}
