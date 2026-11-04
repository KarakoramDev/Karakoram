// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"gopkg.in/yaml.v3"
)

type peerActivationSession struct {
	t                     *testing.T
	active, hba, mapping  string
	interrupted           bool
	reloads, observations int
}

func (s *peerActivationSession) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	return receiptRow{scan: func(dest ...any) error {
		hba, err := os.ReadFile(s.hba)
		if err != nil {
			return err
		}
		if strings.Contains(string(hba), " peer ") {
			s.t.Fatal("peer HBA published before active-map observation")
		}
		switch sql {
		case "SELECT current_setting('ident_file'), pg_conf_load_time()":
			*dest[0].(*string) = s.active
			*dest[1].(*time.Time) = time.Unix(100, 0)
		case "SELECT pg_catalog.pg_reload_conf()":
			data, err := os.ReadFile(s.active)
			if err != nil || !strings.Contains(string(data), s.mapping) {
				s.t.Fatal("reload preceded durable map installation")
			}
			s.reloads++
			*dest[0].(*bool) = true
		case localPeerMappingObservedSQL:
			if s.reloads == 0 || args[0] != s.active {
				s.t.Fatal("mapping observation did not follow current-path reload")
			}
			s.observations++
			*dest[0].(*bool) = !s.interrupted
		default:
			return errors.New("unexpected peer-activation query")
		}
		return nil
	}}
}

func TestLocalPeerExistingClusterActivationStagesLiveAndStartupMaps(t *testing.T) {
	for _, patroni := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "patroni-managed"}[patroni], func(t *testing.T) {
			s := testSpec()
			s.LocalPeerAuth = true
			if patroni {
				s.Topology = "ha"
			}
			p := dataPaths(t.TempDir())
			if err := os.MkdirAll(p.pgdata, 0700); err != nil {
				t.Fatal(err)
			}
			active := filepath.Join(p.pgdata, "pg_ident.conf")
			original := "# native identity map\n\napplication-map application-os application-role\n"
			if err := os.WriteFile(active, []byte(original), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(p.root, 0700); err != nil {
				t.Fatal(err)
			}
			hbaFile := filepath.Join(p.root, "pg_hba.conf")
			if err := os.WriteFile(hbaFile, []byte("local all all scram-sha-256\n"), 0600); err != nil {
				t.Fatal(err)
			}
			owner, mapping, err := localPeerIdentity()
			if err != nil {
				t.Fatal(err)
			}
			fixture := &peerActivationSession{t: t, active: active, hba: hbaFile, mapping: mapping}
			if err := activateLocalPeerMap(t.Context(), fixture, &p, owner, mapping); err != nil {
				t.Fatal(err)
			}
			if p.ident != filepath.Join(p.root, "pg_ident.conf") || parameters(s, p)["ident_file"] != p.ident || fixture.reloads != 1 || fixture.observations == 0 {
				t.Fatal("activation skipped native observation or left startup dependent on PGDATA")
			}
			data, err := os.ReadFile(active)
			if err != nil || string(data) != original+mapping {
				t.Fatal("activation erased unrelated native mappings", err)
			}
			receipt, err := os.ReadFile(filepath.Join(p.root, "peer-auth.json"))
			if err != nil {
				t.Fatal("active-map observation was not persisted", err)
			}
			var observed localPeerObservation
			if json.Unmarshal(receipt, &observed) != nil || observed.IdentFile != active || observed.Owner != owner {
				t.Fatal("incorrect durable active-map observation")
			}
			files, err := render(s, p, nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, file := range files {
				if file.name == "pg_hba.conf" && !strings.HasPrefix(file.content, "local all postgres peer map=karakoram-local\n") {
					t.Fatal("peer rules missing after native observation")
				}
				if file.name == "patroni.yml" {
					var config struct {
						Postgresql struct {
							Parameters map[string]any `yaml:"parameters"`
							Ident      []string       `yaml:"pg_ident"`
							HBA        []string       `yaml:"pg_hba"`
						} `yaml:"postgresql"`
					}
					if err := yaml.Unmarshal([]byte(file.content), &config); err != nil {
						t.Fatal(err)
					}
					if config.Postgresql.Parameters["ident_file"] != p.ident || len(config.Postgresql.Ident) != 0 || config.Postgresql.HBA[0] != hba(s)[0] {
						t.Fatal("Patroni startup lost its stable identity file or would replace staged native maps")
					}
				}
			}
		})
	}
}

func TestLocalPeerInterruptedActivationRetainsSCRAMAndReplays(t *testing.T) {
	p := dataPaths(t.TempDir())
	if err := os.MkdirAll(p.pgdata, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(p.root, 0700); err != nil {
		t.Fatal(err)
	}
	active := filepath.Join(p.pgdata, "pg_ident.conf")
	if err := os.WriteFile(active, nil, 0600); err != nil {
		t.Fatal(err)
	}
	hbaFile := filepath.Join(p.root, "pg_hba.conf")
	scram := "local all all scram-sha-256\n"
	if err := os.WriteFile(hbaFile, []byte(scram), 0600); err != nil {
		t.Fatal(err)
	}
	owner, mapping, err := localPeerIdentity()
	if err != nil {
		t.Fatal(err)
	}
	fixture := &peerActivationSession{t: t, active: active, hba: hbaFile, mapping: mapping, interrupted: true}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := activateLocalPeerMap(ctx, fixture, &p, owner, mapping); err == nil {
		t.Fatal("interrupted reload was treated as observed")
	}
	if _, err := os.Stat(filepath.Join(p.root, "peer-auth.json")); !os.IsNotExist(err) {
		t.Fatal("unobserved activation published a receipt")
	}
	hbaData, err := os.ReadFile(hbaFile)
	if err != nil || string(hbaData) != scram {
		t.Fatal("interrupted activation replaced working SCRAM rules")
	}
	fixture.interrupted = false
	if err := activateLocalPeerMap(t.Context(), fixture, &p, owner, mapping); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(active)
	if err != nil || strings.Count(string(data), "karakoram-local") != 1 || p.ident != filepath.Join(p.root, "pg_ident.conf") {
		t.Fatal("activation replay duplicated mappings or lost the stable startup map")
	}
}
