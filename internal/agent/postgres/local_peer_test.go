// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
	"gopkg.in/yaml.v3"
)

func TestLocalPeerRecoveryAcrossAdministratorRotations(t *testing.T) {
	for _, scenario := range []string{"single-clone-one-rotation", "single-in-place-multiple-rotations", "patroni-reconnect-before-publication", "citus-promotion-next-group-unavailable"} {
		t.Run(scenario, func(t *testing.T) {
			s := testSpec()
			s.LocalPeerAuth = true
			s.Superuser = pgspec.Credentials{Username: "postgres", Password: "desired-synthetic-generation-3"}
			s.AlternateSuperuser = &pgspec.Credentials{Username: "postgres", Password: "previous-synthetic-generation-2"}
			s.Bootstrap = &pgspec.Bootstrap{Method: "restore", Generation: 8, RecoverySuperuser: &pgspec.Credentials{Username: "postgres", Password: "base-synthetic-generation-0"}}
			// The recovered WAL target can contain generation 1, absent from every
			// password candidate. A promoted primary before publication can similarly
			// contain a password different from Patroni's still-assigned desired one.
			if strings.Contains(scenario, "patroni") || strings.Contains(scenario, "citus") {
				s.Topology = pgspec.TopologyHA
				s.Bootstrap = nil
			}
			if strings.Contains(scenario, "citus") {
				s.Topology = pgspec.TopologyDistributed
				s.Citus = &pgspec.Citus{Group: 0, Database: "app"}
			}
			p := dataPaths(t.TempDir())
			attempts := 0
			_, err := localConnectWith(t.Context(), s, p, func(_ context.Context, cfg *pgx.ConnConfig) (*pgx.Conn, error) {
				attempts++
				if cfg.Host != p.socket || cfg.User != "postgres" || cfg.Password != "" {
					return nil, errors.New("no password candidate matches recovered native generation")
				}
				return &pgx.Conn{}, nil
			})
			if err != nil || attempts != 1 {
				t.Fatal("local management depended on restored/published password", err)
			}
			if hba(s)[0] != "local all postgres peer map=karakoram-local" || parameters(s, p)["ident_file"] != filepath.Join(p.root, "pg_ident.conf") {
				t.Fatal("local peer authentication was not rendered before SCRAM")
			}
			if strings.Contains(scenario, "patroni") || strings.Contains(scenario, "citus") {
				rendered, err := renderPatroni(s, p)
				if err != nil {
					t.Fatal(err)
				}
				var config struct {
					Postgresql struct {
						UseUnixSocket bool           `yaml:"use_unix_socket"`
						Parameters    map[string]any `yaml:"parameters"`
						HBA           []string       `yaml:"pg_hba"`
					} `yaml:"postgresql"`
				}
				if err := yaml.Unmarshal([]byte(rendered), &config); err != nil {
					t.Fatal(err)
				}
				if !config.Postgresql.UseUnixSocket || config.Postgresql.HBA[0] != hba(s)[0] || config.Postgresql.Parameters["ident_file"] != parameters(s, p)["ident_file"] {
					t.Fatal("Patroni reconnect/promotion does not share password-independent local management")
				}
			}
		})
	}
}

func TestLocalPeerIdentityIsExactRuntimeOwnerAndLegacyIsUnchanged(t *testing.T) {
	s := testSpec()
	p := dataPaths(t.TempDir())
	if strings.Contains(strings.Join(hba(s), "\n"), "peer") || parameters(s, p)["ident_file"] != nil {
		t.Fatal("legacy configuration gained peer authentication")
	}
	s.LocalPeerAuth = true
	files, err := render(s, p, nil)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := user.LookupId(strconv.Itoa(os.Geteuid()))
	if err != nil {
		t.Fatal(err)
	}
	want := "karakoram-local \"" + strings.ReplaceAll(owner.Username, `"`, `""`) + "\" postgres\n"
	found := false
	for _, file := range files {
		if file.name == "pg_ident.conf" {
			found = true
			if file.content != want {
				t.Fatal("identity map was not restricted to effective runtime owner")
			}
		}
	}
	if !found {
		t.Fatal("identity map missing")
	}
	for _, line := range hba(s) {
		if strings.HasPrefix(line, "host") && strings.Contains(line, "peer") {
			t.Fatal("remote authentication weakened")
		}
	}
	s.LocalPeerAuth = false
	attempts := []string{}
	_, err = localConnectWith(t.Context(), s, p, func(_ context.Context, cfg *pgx.ConnConfig) (*pgx.Conn, error) {
		attempts = append(attempts, cfg.Password)
		return &pgx.Conn{}, nil
	})
	if err != nil || len(attempts) != 1 || attempts[0] != s.Superuser.Password {
		t.Fatal("old control plane/new agent no longer uses legacy password path")
	}
}

func TestTransitionInvalidatesCachedAdministratorBootstrapSQL(t *testing.T) {
	runner := New()
	s := testSpec()
	if _, err := runner.systemRoleSQL(s); err != nil {
		t.Fatal(err)
	}
	s.AlternateSuperuser = &pgspec.Credentials{Username: "postgres", Password: "pending-synthetic"}
	statements, err := runner.systemRoleSQL(s)
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range statements {
		if strings.HasPrefix(sql, "ALTER ROLE \"postgres\"") {
			t.Fatal("cached pre-transition SQL could restore the old administrator password")
		}
	}
}

func TestAdministratorRotationUnavailableBeforePeerActivation(t *testing.T) {
	s := testSpec()
	s.LocalPeerAuth = true
	status := New().status(t.Context(), s, dataPaths(t.TempDir()))
	if status.AdminRotation {
		t.Fatal("rotation advertised without an observed password-independent local connection")
	}
	s.LocalPeerAuth = false
	status = New().status(t.Context(), s, dataPaths(t.TempDir()))
	if status.AdminRotation {
		t.Fatal("legacy control-plane configuration advertised administrator rotation")
	}
}
