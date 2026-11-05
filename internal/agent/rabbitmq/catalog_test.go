// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	provider "github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq"
	"testing"
)

func TestCatalogVersionsPassSpecValidation(t *testing.T) {
	for _, v := range provider.New(provider.Options{}).Describe().Versions {
		t.Run(v.Version, func(t *testing.T) {
			s := testSpec()
			s.Version = v.Version
			if _, err := spec(memberConfig(s)); err != nil {
				t.Fatal(err)
			}
		})
	}
}
