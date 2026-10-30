// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"os/exec"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal"
	"github.com/KarakoramDev/Karakoram/internal/infra/zerops"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
)

// TestEveryEngineIPv6NativeConfig is the single registry coverage gate. Native
// renderers are private and their realistic fixtures live in package-local test
// files. Run those adapters rather than exporting production APIs or maintaining
// a second set of fixtures. JSON test events prove each adapter actually ran:
// a missing/renamed test, skip or empty package cannot silently pass this gate.
func TestEveryEngineIPv6NativeConfig(t *testing.T) {
	type fixture struct{ pkg, test string }
	adapter := func(pkg, sub string) fixture {
		test := "TestIPv6NativeConfig"
		if sub != "" {
			test += "/" + sub
		}
		return fixture{"github.com/KarakoramDev/Karakoram/internal/agent" + pkg, test}
	}
	coverage := map[string]map[string]fixture{
		"psmdb":         {"engine": adapter("", "mongod-rs"), "configServer": adapter("", "mongod-cfg"), "router": adapter("", "mongos")},
		"postgres":      {"engine": adapter("/postgres", ""), "proxy": adapter("/postgres", "")},
		"mysql":         {"engine": adapter("/mysql", ""), "proxy": adapter("/mysql", "")},
		"pxc":           {"engine": adapter("/mysql", "cluster"), "proxy": adapter("/mysql", "")},
		"supabase":      {"engine": adapter("/postgres", ""), "proxy": adapter("/postgres", "")},
		"elasticsearch": {"engine": adapter("/elastic", ""), "master": adapter("/elastic", ""), "kibana": adapter("/elastic", "kibana"), "apm": adapter("/elastic", "apm-server"), "logstash": adapter("/elastic", "logstash")},
		"rabbitmq":      {"engine": adapter("/rabbitmq", "")},
		"openbao":       {"engine": adapter("/openbao", "")},
		"neo4j":         {"engine": adapter("/neo4j", "")},
		"keycloak":      {"engine": adapter("/keycloak", "")},
		"weaviate":      {"engine": adapter("/weaviate", "")},
		"temporal":      {"server": adapter("/temporal", "server"), "frontend": adapter("/temporal", "frontend"), "history": adapter("/temporal", "history"), "matching": adapter("/temporal", "matching"), "worker": adapter("/temporal", "worker"), "ui": adapter("/temporal", "ui")},
	}
	for _, component := range sbspec.Components {
		coverage["supabase"][component] = adapter("/supabase", component)
	}
	required := []fixture{adapter("/datamcp", "mongodb-mcp-server"), adapter("/datamcp", "pgedge-postgres-mcp")}
	seen := map[string]bool{}
	for _, provider := range engineProviders(engineOptions{Bindings: pgbinding.New(pgbinding.Options{})}) {
		name := provider.Name()
		seen[name] = true
		components, ok := coverage[name]
		if !ok {
			t.Errorf("registered engine %s has no IPv6 render adapter", name)
			continue
		}
		componentSeen := map[string]bool{}
		for _, topology := range provider.Describe().Topologies {
			for component := range topology.Components {
				componentSeen[component] = true
				f, ok := components[component]
				if !ok {
					t.Errorf("registered component %s/%s has no IPv6 render adapter", name, component)
					continue
				}
				required = append(required, f)
			}
		}
		for component := range components {
			if !componentSeen[component] {
				t.Errorf("IPv6 adapter for unregistered component %s/%s", name, component)
			}
		}
	}
	for name := range coverage {
		if !seen[name] {
			t.Errorf("IPv6 adapter for unregistered engine %s", name)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
	// Explicit support exemption, not a skip: Temporal's renderer is exercised,
	// but production admission must continue rejecting IPv6-only backends until
	// route discovery and native membership are qualified in that family.
	err := temporal.New(temporal.Options{}).ValidateBackend(new(zerops.Backend))
	if !errors.Is(err, engine.ErrInvalid) || !strings.Contains(err.Error(), "private IPv4") {
		t.Fatalf("Temporal IPv6 admission exemption changed: %v", err)
	}
	packages := []string{}
	packageSeen := map[string]bool{}
	for _, f := range required {
		if !packageSeen[f.pkg] {
			packages = append(packages, f.pkg)
			packageSeen[f.pkg] = true
		}
	}
	args := append([]string{"test", "-p=1", "-count=1", "-json", "-run=^TestIPv6NativeConfig$"}, packages...)
	cmd := exec.CommandContext(t.Context(), "go", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	data, err := cmd.Output()
	var diagnostics strings.Builder
	passed := map[fixture]bool{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	for decoder.More() {
		var event struct{ Action, Package, Test, Output string }
		if err := decoder.Decode(&event); err != nil {
			t.Fatal(err)
		}
		diagnostics.WriteString(event.Output)
		if event.Action == "pass" {
			passed[fixture{event.Package, event.Test}] = true
		}
	}
	if err != nil {
		t.Fatalf("IPv6 rendering adapters: %v\n%s\n%s", err, stderr.String(), diagnostics.String())
	}
	for _, f := range required {
		if !passed[f] {
			t.Errorf("IPv6 rendering adapter did not pass: %s %s", f.pkg, f.test)
		}
	}
}
