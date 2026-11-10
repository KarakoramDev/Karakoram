// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package dokploy_test

import (
	"github.com/KarakoramDev/Karakoram/internal/configpulltest"
	provider "github.com/KarakoramDev/Karakoram/internal/infra/dokploy"
	"testing"
)

func TestProductionConfigPullBudget(t *testing.T) {
	for _, kind := range []string{"mysql", "postgres"} {
		t.Run(kind, func(t *testing.T) {
			p, rc, advance, total := provider.ExportConfigPullFixture(t, kind)
			var withdraw func()
			if kind == "postgres" {
				withdraw = provider.ExportConfigPeerWithdrawal(t, rc)
			}
			configpulltest.Run(t, p, rc, advance, total, withdraw)
		})
	}
}
