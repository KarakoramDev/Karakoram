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
			enginetest.AssertIdle(t, p, rc, nil)
		})
	}
}
