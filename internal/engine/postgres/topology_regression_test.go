// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

var updateTopologyFixtures = flag.Bool("update-topology-fixtures", false, "update single/HA compatibility fixtures (review byte-level changes)")

// These fixtures are captured before introducing the distributed topology. Use
// fixed, visibly synthetic secrets rather than redacting the rendered spec: a
// redaction could hide an accidental change in the secret-bearing wire shape.
func TestTopologyCompatibilityFixtures(t *testing.T) {
	for _, topology := range []string{"single", "ha"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, _ := setup(t, topology)
			s := converge(t, p, rc)
			s.UID = "compatibility-fixture"
			for _, name := range systemNames {
				if err := putSecret(t.Context(), rc, "users/"+name, []byte("fixture-password-"+name)); err != nil {
					t.Fatal(err)
				}
			}
			if err := putSecret(t.Context(), rc, "tls/ca.crt", []byte("fixture-ca")); err != nil {
				t.Fatal(err)
			}
			for _, m := range s.Members {
				for _, kind := range []string{"servers", "agents"} {
					for _, ext := range []string{"crt", "key"} {
						key := "tls/" + kind + "/" + m.Name + "." + ext
						if err := putSecret(t.Context(), rc, key, []byte("fixture-"+key)); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			type memberFixture struct {
				Name string
				// A string preserves the exact marshaled EngineSpec bytes,
				// including field order and omission, inside the fixture.
				EngineSpec       string
				EngineSpecSHA256 string
				Runtime          infra.RuntimeSpec
			}
			fixture := struct {
				ConfigHash string
				Members    []memberFixture
			}{ConfigHash: s.ConfigHash}
			for _, m := range s.Members {
				config, err := p.MemberConfig(t.Context(), rc, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				runtime, err := p.runtimeSpec(rc, m)
				if err != nil {
					t.Fatal(err)
				}
				digest := sha256.Sum256(config.EngineSpec)
				fixture.Members = append(fixture.Members, memberFixture{
					Name: m.Name, EngineSpec: string(config.EngineSpec),
					EngineSpecSHA256: hex.EncodeToString(digest[:]), Runtime: runtime,
				})
			}
			raw, err := json.MarshalIndent(fixture, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			raw = append(raw, '\n')
			path := filepath.Join("testdata", "topology-"+topology+".json")
			if *updateTopologyFixtures {
				if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, raw, 0644); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(raw, want) {
				t.Fatalf("%s compatibility fixture changed; inspect member EngineSpec bytes, runtime/proxy specs and digests before updating %s", topology, path)
			}
		})
	}
}
