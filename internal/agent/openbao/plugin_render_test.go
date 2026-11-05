// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
)

func TestRenderDualDatabasePluginVersionsAndAWSKMS(t *testing.T) {
	s, c := testConfig()
	s.PluginDirectory = "/opt/karakoram/openbao-plugins"
	s.Plugins = []baospec.Plugin{
		{Type: "database", Name: "mongodb-database-plugin", Command: "openbao-plugin-database-mongodb_linux_amd64_v1", Version: "v0.0.1"},
		{Type: "database", Name: "mongodb-database-plugin", Command: "openbao-plugin-database-mongodb_0.0.2", Version: "v0.0.2"},
		{Type: "kms", Name: "awskms", Command: "openbao-plugin-kms-aws_linux_amd64_v1", Version: "v0.1.1"},
	}
	s.Seal = baospec.Seal{Type: baospec.SealAWSKMS, AWSKMS: &baospec.AWSKMSSeal{Region: "eu-west-1", KMSKeyID: "existing-key"}}
	files, err := render(t.TempDir(), s, c)
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		Directory string `json:"plugin_directory"`
		Plugins   []map[string]map[string]struct {
			Command string `json:"command"`
			Version string `json:"version"`
		} `json:"plugin"`
		Seal map[string]map[string]any `json:"seal"`
	}
	if err = json.Unmarshal(files["openbao/server.json"], &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Directory != s.PluginDirectory || len(cfg.Plugins) != 3 {
		t.Fatalf("lost plugin registration: %+v", cfg)
	}
	for i, p := range s.Plugins {
		got := cfg.Plugins[i][p.Type][p.Name]
		if got.Command != p.Command || got.Version != p.Version {
			t.Fatalf("registration %d: %+v", i, got)
		}
	}
	if cfg.Seal["awskms"]["kms_key_id"] != "existing-key" {
		t.Fatal("AWS key changed during plugin update")
	}
}
