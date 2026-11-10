// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package nomad_test

import (
	"github.com/KarakoramDev/Karakoram/internal/configpulltest"
	provider "github.com/KarakoramDev/Karakoram/internal/infra/nomad"
	"testing"
)

func TestProductionConfigPullBudget(t *testing.T) {
	for _, kind := range []string{"mysql", "postgres"} {
		t.Run(kind, func(t *testing.T) {
			p, rc, advance, total := provider.ExportConfigPullFixture(t, kind)
			configpulltest.Run(t, p, rc, advance, total, nil)
		})
	}
}
