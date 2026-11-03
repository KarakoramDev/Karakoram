// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"gopkg.in/yaml.v3"
)

func TestLocalPeerStartupIdentitySurvivesPreActivationMaterialization(t *testing.T) {
	for _, method := range []string{"patroni-pgbackrest-rebuild", "patroni-basebackup-reinit", "single-in-place-restore", "single-clone"} {
		t.Run(method, func(t *testing.T) {
			s := testSpec()
			s.LocalPeerAuth = true
			patroni := strings.HasPrefix(method, "patroni")
			if patroni {
				s.Topology = "ha"
			}
			p := dataPaths(t.TempDir())
			if err := os.MkdirAll(p.pgdata, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(p.root, 0700); err != nil {
				t.Fatal(err)
			}
			active := filepath.Join(p.pgdata, "pg_ident.conf")
			if err := os.WriteFile(active, []byte("# pre-activation native map\n"), 0600); err != nil {
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
			startupIdentity := parameters(s, p)["ident_file"].(string)
			if patroni {
				text, err := renderPatroni(s, p)
				if err != nil {
					t.Fatal(err)
				}
				var config struct {
					Postgresql struct {
						Parameters map[string]any `yaml:"parameters"`
					} `yaml:"postgresql"`
				}
				if err := yaml.Unmarshal([]byte(text), &config); err != nil {
					t.Fatal(err)
				}
				startupIdentity = config.Postgresql.Parameters["ident_file"].(string)
			}
			// Rebuild can be initiated autonomously by Patroni, so deliberately do not
			// run another Apply between materialization and this startup observation.
			if err := os.RemoveAll(p.pgdata); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(p.pgdata, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(active, []byte("# identity file from an old base backup\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if startupIdentity != filepath.Join(p.root, "pg_ident.conf") {
				t.Fatal("startup still depends on the backed-up identity file")
			}
			data, err := os.ReadFile(startupIdentity)
			if err != nil || !strings.Contains(string(data), mapping) {
				t.Fatal("materialization removed the active startup identity", err)
			}
			attempts := 0
			_, err = localConnectWith(t.Context(), s, p, func(_ context.Context, cfg *pgx.ConnConfig) (*pgx.Conn, error) {
				attempts++
				data, err := os.ReadFile(startupIdentity)
				if err != nil || !strings.Contains(string(data), mapping) || cfg.Password != "" {
					return nil, errors.New("restored administrator password is not a known candidate")
				}
				return &pgx.Conn{}, nil
			})
			if err != nil || attempts != 1 {
				t.Fatal("post-materialization startup was not password-independent", err)
			}
		})
	}
}

func TestLocalPeerMissingLiveMapRepairsWithoutSQLAndReplays(t *testing.T) {
	for _, priorGone := range []bool{false, true} {
		t.Run(map[bool]string{false: "active-PGDATA-map", true: "restarted-external-map"}[priorGone], func(t *testing.T) {
			p := dataPaths(t.TempDir())
			if err := os.MkdirAll(p.root, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(p.pgdata, 0700); err != nil {
				t.Fatal(err)
			}
			active := filepath.Join(p.pgdata, "pg_ident.conf")
			owner, mapping, err := localPeerIdentity()
			if err != nil {
				t.Fatal(err)
			}
			if err := durableJSON(filepath.Join(p.root, "peer-auth.json"), localPeerObservation{IdentFile: active, Owner: owner}); err != nil {
				t.Fatal(err)
			}
			if priorGone {
				if err := os.RemoveAll(p.pgdata); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(active, []byte("# restored file missing the managed map\n"), 0600); err != nil {
				t.Fatal(err)
			}
			actual := active
			if priorGone {
				actual = filepath.Join(p.root, "pg_ident.conf")
			}
			reloaded := false
			reloads := 0
			reload := func(context.Context) error {
				reloads++
				data, err := os.ReadFile(actual)
				if err != nil || !strings.Contains(string(data), mapping) {
					t.Fatal("process reload preceded map repair")
				}
				reloaded = true
				return nil
			}
			probe := func(context.Context) (*pgx.Conn, error) {
				if !reloaded {
					return nil, errors.New("peer map not loaded")
				}
				return &pgx.Conn{}, nil
			}
			interrupted, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := repairLocalPeerMapWith(interrupted, p, owner, mapping, reload, probe); err == nil || reloads != 0 {
				t.Fatal("interrupted repair advanced native reload")
			}
			for range 2 {
				if _, err := repairLocalPeerMapWith(t.Context(), p, owner, mapping, reload, probe); err != nil {
					t.Fatal(err)
				}
			}
			data, err := os.ReadFile(actual)
			if err != nil || strings.Count(string(data), "karakoram-local") != 1 || reloads != 2 {
				t.Fatal("live recovery lost or duplicated the identity map", err)
			}
		})
	}
}
