// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"testing"
)

func TestIdleReconcileZeroProviderMutations(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("replicas%d", replicas), func(t *testing.T) {
			p, rc, _, _ := setup(t)
			rc.Cluster.Spec.Components.Engine.Replicas = replicas
			enginetest.AssertIdle(t, p, rc, nil)
		})
	}
}
