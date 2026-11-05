// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package temporal

import (
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	provider "github.com/KarakoramDev/Karakoram/internal/engine/temporal"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"testing"
)

func TestCatalogVersionsPassSpecValidation(t *testing.T) {
	for _, v := range provider.New(provider.Options{}).Describe().Versions {
		for _, component := range []string{tspec.Server, tspec.Frontend, tspec.History, tspec.Matching, tspec.Worker} {
			t.Run(v.Version+"/"+component, func(t *testing.T) {
				s := testSpec()
				s.Version, s.Component = v.Version, component
				s.InstallDir = "/opt/karakoram/temporal-server-" + v.Version
				if err := s.Validate(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
	ui, err := nativeartifacts.Get("temporal-ui-server")
	if err != nil {
		t.Fatal(err)
	}
	s := testSpec()
	s.Component, s.Version = tspec.UI, ui.Version
	s.InstallDir = "/opt/karakoram/temporal-ui-server-" + ui.Version
	s.Persistence, s.FrontendAddress = nil, "frontend:7233"
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
}
