// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestCatalogOptionsMatchValidation(t *testing.T) {
	p := New(Options{})
	info := p.Describe()
	groups := info.EngineOptions["pluginGroups"].([]pluginGroup)
	if len(groups) != 3 {
		t.Fatal(groups)
	}
	all := []string{}
	for _, group := range groups {
		if group.ID == "" || group.Label == "" || len(group.Plugins) == 0 {
			t.Fatal(group)
		}
		all = append(all, group.Plugins...)
		raw, err := json.Marshal(engineOptions{Plugins: group.Plugins})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := parseOptions(raw); err != nil {
			t.Fatalf("advertised plugin group %s rejected: %v", group.ID, err)
		}
		for plugin, dependencies := range group.Dependencies {
			if !slices.Contains(group.Plugins, plugin) || len(dependencies) == 0 {
				t.Fatal(group)
			}
			for _, dependency := range dependencies {
				if !slices.Contains(group.Plugins, dependency) {
					t.Fatal(group)
				}
				withoutDependency := slices.DeleteFunc(slices.Clone(group.Plugins), func(value string) bool { return value == dependency })
				raw, _ := json.Marshal(engineOptions{Plugins: withoutDependency})
				if _, err := parseOptions(raw); err == nil || !strings.Contains(err.Error(), "requires") {
					t.Fatalf("published dependency %s -> %s not enforced: %v", plugin, dependency, err)
				}
			}
		}
	}
	if len(all) != 6 {
		t.Fatal(all)
	}
	for _, plugin := range info.EngineOptions["alwaysEnabledPlugins"].([]string) {
		if slices.Contains(all, plugin) {
			t.Fatalf("always-enabled plugin listed as optional: %s", plugin)
		}
		raw, _ := json.Marshal(engineOptions{Plugins: []string{plugin}})
		if _, err := parseOptions(raw); err == nil {
			t.Fatalf("always-enabled plugin accepted as optional: %s", plugin)
		}
	}
	management := info.EngineOptions["managementUI"].(map[string]any)["public"].(map[string]any)
	if management["type"] != "boolean" || management["default"] != false {
		t.Fatal(management)
	}
	for _, mode := range []string{"disabled", "preferTLS", "requireTLS"} {
		c := &domain.Cluster{Name: "brokers", Spec: domain.ClusterSpec{TLS: domain.TLSConfig{Mode: mode}, EngineOptions: json.RawMessage(`{"managementUI":{"public":true}}`)}}
		p.ApplyDefaults(&c.Spec)
		err := p.Validate(context.Background(), nil, c)
		blocked := slices.Contains(management["unavailableTLSModes"].([]string), mode)
		if blocked != (err != nil) {
			t.Fatalf("management metadata disagrees for %s: %v", mode, err)
		}
	}
}

func TestCatalogConfigMatchesValidation(t *testing.T) {
	info := New(Options{}).Describe()
	if info.EngineConfig["format"] != "rabbitmq.conf" || info.EngineConfig["syntax"] != "key = value" || info.EngineConfig["managedKeyMatching"] == "" {
		t.Fatal(info.EngineConfig)
	}
	prefixes := info.EngineConfig["managedKeyPrefixes"].([]string)
	if len(prefixes) == 0 {
		t.Fatal("no managed keys published")
	}
	for _, prefix := range prefixes {
		for _, key := range []string{strings.TrimSuffix(prefix, "."), prefix + "custom"} {
			if err := validateEngineConfig(key + " = 1"); err == nil || !strings.Contains(err.Error(), "managed") {
				t.Errorf("advertised managed key accepted: %s: %v", key, err)
			}
		}
	}
	if err := validateEngineConfig("# comment\nconsumer_timeout = 600000"); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogMetadataDoesNotMutateValidatorTables(t *testing.T) {
	p := New(Options{})
	info := p.Describe()
	info.EngineOptions["pluginGroups"].([]pluginGroup)[0].Plugins[0] = "other"
	info.EngineOptions["pluginGroups"].([]pluginGroup)[0].Dependencies["rabbitmq_shovel_management"][0] = "other"
	info.EngineOptions["alwaysEnabledPlugins"].([]string)[0] = "other"
	info.EngineConfig["managedKeyPrefixes"].([]string)[0] = "other"
	fresh := p.Describe()
	if fresh.EngineOptions["pluginGroups"].([]pluginGroup)[0].Plugins[0] != "rabbitmq_shovel" || fresh.EngineOptions["pluginGroups"].([]pluginGroup)[0].Dependencies["rabbitmq_shovel_management"][0] != "rabbitmq_shovel" || fresh.EngineOptions["alwaysEnabledPlugins"].([]string)[0] != "rabbitmq_management" || fresh.EngineConfig["managedKeyPrefixes"].([]string)[0] != "listeners." {
		t.Fatal("catalog shares mutable validator tables")
	}
}
