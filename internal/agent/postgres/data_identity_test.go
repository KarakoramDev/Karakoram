// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"gopkg.in/yaml.v3"
)

func TestInitializedSingletonNeverReinitializesMissingData(t *testing.T) {
	for _, authorized := range []bool{true, false} {
		p := dataPaths(t.TempDir())
		if err := privateDir(p.root); err != nil {
			t.Fatal(err)
		}
		if err := durableJSON(filepath.Join(p.root, "initialized.json"), true); err != nil {
			t.Fatal(err)
		}
		s := testSpec()
		s.Bootstrap = &pgspec.Bootstrap{Method: "initdb", AllowInitDB: authorized}
		// No node or initdb binary is needed: the guard must run before either.
		if err := initSingle(t.Context(), nil, s, p); err == nil || !strings.Contains(err.Error(), "restore") {
			t.Fatalf("missing initialized PGDATA was not refused: %v", err)
		}
	}
}

func TestFormedSingletonNeverInitializesFreshVolume(t *testing.T) {
	p := dataPaths(t.TempDir())
	s := testSpec()
	s.Bootstrap = &pgspec.Bootstrap{Method: "initdb"}
	if err := initSingle(t.Context(), nil, s, p); err == nil || !strings.Contains(err.Error(), "restore") {
		t.Fatalf("fresh mount was not refused: %v", err)
	}
}

func TestExistingSingletonRecordsInitializationBeforeReturning(t *testing.T) {
	p := dataPaths(t.TempDir())
	if err := privateDir(p.pgdata); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p.pgdata, "PG_VERSION"), []byte("17\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := initSingle(t.Context(), nil, testSpec(), p); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(p.root, "initialized.json")); err != nil {
		t.Fatalf("existing data has no durable initialization receipt: %v", err)
	}
}

func TestFormedPatroniCanCloneButCannotBootstrapEmpty(t *testing.T) {
	s := testSpec()
	s.Topology = pgspec.TopologyHA
	s.Bootstrap = &pgspec.Bootstrap{Method: "initdb"}
	data, err := renderPatroni(s, dataPaths(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := yaml.Unmarshal([]byte(data), &config); err != nil {
		t.Fatal(err)
	}
	if _, ok := config["bootstrap"]; ok {
		t.Fatal("formed HA cluster can bootstrap an empty database")
	}
	pg := config["postgresql"].(map[string]any)
	if len(pg["create_replica_methods"].([]any)) == 0 {
		t.Fatal("safe replica cloning disabled")
	}
	s.Bootstrap.AllowInitDB = true
	data, err = renderPatroni(s, dataPaths(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal([]byte(data), &config); err != nil {
		t.Fatal(err)
	}
	if _, ok := config["bootstrap"]; !ok {
		t.Fatal("new HA cluster cannot initialize")
	}
}

func TestFirstInitializationAuthorizationIsConsumedDurably(t *testing.T) {
	p := dataPaths(t.TempDir())
	if err := privateDir(p.root); err != nil {
		t.Fatal(err)
	}
	s := testSpec()
	s.Bootstrap = &pgspec.Bootstrap{Method: "initdb", AllowInitDB: true}
	if err := guardInitialization(s, p); err != nil {
		t.Fatal("new bootstrap was refused", err)
	}
	if err := guardInitialization(s, p); err == nil {
		t.Fatal("interrupted initialization authorized a second empty identity")
	}
}

func TestCompletedRestoreReceiptCannotHideMissingData(t *testing.T) {
	p := dataPaths(t.TempDir())
	if err := privateDir(p.root); err != nil {
		t.Fatal(err)
	}
	if err := durableJSON(filepath.Join(p.root, "bootstrap-generation.json"), 4); err != nil {
		t.Fatal(err)
	}
	s := testSpec()
	s.Bootstrap = &pgspec.Bootstrap{Method: "restore", Generation: 4}
	if err := initSingle(t.Context(), nil, s, p); err == nil || !strings.Contains(err.Error(), "restore") {
		t.Fatalf("completed receipt hid missing restored data: %v", err)
	}
	if !dataRecoveryRequired(s, p) {
		t.Fatal("missing data after completed restore has no precise recovery status")
	}
	s.Bootstrap.Generation = 5
	if dataRecoveryRequired(s, p) {
		t.Fatal("authorized new restore generation was mistaken for abandoned data loss")
	}
}

func TestPatroniBootstrapRecordsInitializationBeforeRolePublication(t *testing.T) {
	s := testSpec()
	s.Topology = pgspec.TopologyHA
	p := dataPaths(t.TempDir())
	files, err := render(s, p, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if file.name != "bootstrap-roles.sh" {
			continue
		}
		receipt := strings.Index(file.content, shellQuote(filepath.Join(p.root, "initialized.json")))
		publication := strings.Index(file.content, "exec ")
		if receipt < 0 || publication < 0 || receipt > publication || !strings.Contains(file.content, "sync "+shellQuote(p.root)) {
			t.Fatal("HA publishes bootstrap roles without a durable initialized receipt")
		}
		return
	}
	t.Fatal("HA bootstrap hook missing")
}
