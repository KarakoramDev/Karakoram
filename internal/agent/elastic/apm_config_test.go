// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
)

func TestAPMUsesElasticsearchManagedAssetsAndDualStackIntake(t *testing.T) {
	r, h := fixture(t)
	apply(t, r, h, agentspec.Spec{
		Component: agentspec.ComponentAPM, Version: "8.19.23", CAPEM: "ca",
		APM: &agentspec.APMSpec{ESHosts: []string{"https://node:9200"}, Writer: agentspec.Credentials{Username: "apm", Password: "test-password"}},
	})
	config := readYAML(t, h, "apm-server.yml")
	if config["apm-server.host"] != "[::]:8200" || config["http.host"] != "127.0.0.1" || config["http.port"] != 5066 {
		t.Fatal("intake must be dual-stack; monitoring must remain local", config)
	}
	// Elasticsearch >= 8.15 installs APM assets itself. Keep modern APM data
	// stream defaults and do not introduce legacy setup/Fleet prerequisites.
	for key := range config {
		if strings.HasPrefix(key, "setup.") || strings.HasPrefix(key, "apm-server.data_streams") {
			t.Fatal("configuration contradicts Elasticsearch-managed APM assets", key)
		}
	}
}
