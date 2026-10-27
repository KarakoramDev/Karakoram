// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"testing"
)

func TestIdleReconcileZeroProviderMutations(t *testing.T) {
	t.Run("single", func(t *testing.T) {
		p, rc, _, _ := setup(t)
		enginetest.AssertIdle(t, p, rc, nil)
	})
}
