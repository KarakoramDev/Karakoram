// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	provider "github.com/KarakoramDev/Karakoram/internal/engine/elastic"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
	"testing"
)

// All four component payloads pass through this same version gate in Apply.
func TestCatalogVersionsPassVersionValidation(t *testing.T) {
	for _, v := range provider.New(provider.ProviderOptions{}).Describe().Versions {
		for _, component := range []string{agentspec.ComponentElasticsearch, agentspec.ComponentKibana, agentspec.ComponentLogstash, agentspec.ComponentAPM} {
			t.Run(v.Version+"/"+component, func(t *testing.T) {
				if !stackVersion.MatchString(v.Version) {
					t.Fatal("catalog version rejected by agent")
				}
				if provider.ArtifactURL(component, v.Version) == "" {
					t.Fatal("catalog version rejected by artifact resolver")
				}
			})
		}
	}
}
