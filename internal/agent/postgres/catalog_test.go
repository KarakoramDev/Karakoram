// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	provider "github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"strconv"
	"testing"
)

func TestCatalogVersionsPassSpecValidation(t *testing.T) {
	for _, v := range provider.New(provider.Options{}).Describe().Versions {
		t.Run(v.Version, func(t *testing.T) {
			s := testSpec()
			s.Version = v.Version
			var err error
			s.Major, err = strconv.Atoi(v.Major)
			if err != nil {
				t.Fatal(err)
			}
			s.BinDir = "/usr/lib/postgresql/" + v.Major + "/bin"
			raw, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeSpec(agentapi.MemberConfig{Engine: "postgres", EngineSpec: raw}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
