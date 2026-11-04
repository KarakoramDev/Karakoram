// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"testing"
)

func TestIdleReconcileZeroProviderMutations(t *testing.T) {
	for _, topology := range []string{"single", "ha", "distributed"} {
		t.Run(topology, func(t *testing.T) {
			if topology == "distributed" {
				p, rc, _, _ := setupDistributed(t, 1)
				enginetest.AssertIdle(t, p, rc, nil)
				return
			}
			p, rc, _ := setup(t, topology)
			enginetest.AssertIdle(t, p, rc, nil)
		})
	}
}
