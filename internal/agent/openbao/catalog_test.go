// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"encoding/json"
	provider "github.com/KarakoramDev/Karakoram/internal/engine/openbao"
	"testing"
)

func TestCatalogVersionsPassSpecValidation(t *testing.T) {
	for _, v := range provider.New(provider.ProviderOptions{}).Describe().Versions {
		t.Run(v.Version, func(t *testing.T) {
			s, c := testConfig()
			s.Version = v.Version
			var err error
			c.EngineSpec, err = json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := spec(c); err != nil {
				t.Fatal(err)
			}
		})
	}
}
