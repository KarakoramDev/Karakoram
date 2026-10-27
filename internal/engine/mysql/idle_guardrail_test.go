// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"testing"
)

func TestIdleReconcileZeroProviderMutations(t *testing.T) {
	for _, topology := range []string{"single", "group", "pxc"} {
		t.Run(topology, func(t *testing.T) {
			if topology == "pxc" {
				p, rc, b := setupPXC(t)
				enginetest.AssertIdle(t, p, rc, func() { b.apply(t, p, rc) })
				return
			}
			p, rc, b := setup(t, topology)
			enginetest.AssertIdle(t, p, rc, func() { b.apply(t, p, rc) })
		})
	}
}
