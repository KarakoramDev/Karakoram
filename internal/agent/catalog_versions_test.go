// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent_test

import (
	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	provider "github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
	"testing"
)

func TestCatalogVersionsPassMongoConfigValidation(t *testing.T) {
	for _, v := range provider.New(provider.Options{}).Describe().Versions {
		t.Run(v.Version, func(t *testing.T) {
			c := agentapi.MemberConfig{Engine: "psmdb", EngineVersion: v.Version, Role: agentapi.RoleReplicaSet, ReplSetName: "rs0", Keyfile: "synthetic-test-key"}
			if _, err := agent.RenderConfig(c, t.TempDir()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
