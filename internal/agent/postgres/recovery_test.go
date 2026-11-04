// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"gopkg.in/yaml.v3"
)

func TestHARecoveryUsesTargetSystemIdentities(t *testing.T) {
	s := testSpec()
	s.Topology = pgspec.TopologyHA
	s.Bootstrap = &pgspec.Bootstrap{Method: "restore", Generation: 3, Restore: &pgspec.Restore{Source: pgspec.Repository{Stanza: "kk", Path: "/backups/source", S3: pgspec.S3{Endpoint: "https://objects.example", Bucket: "backups", Region: "region"}}, Set: "20261017-021859F"}}
	p := dataPaths(t.TempDir())
	files, err := render(s, p, nil)
	if err != nil {
		t.Fatal(err)
	}
	rendered := map[string]string{}
	for _, f := range files {
		rendered[f.name] = f.content
	}
	var config map[string]any
	if err := yaml.Unmarshal([]byte(rendered["patroni.yml"]), &config); err != nil {
		t.Fatal(err)
	}
	pg := config["postgresql"].(map[string]any)
	auth := pg["authentication"].(map[string]any)
	for _, tc := range []struct {
		name        string
		credentials pgspec.Credentials
	}{{"superuser", s.Superuser}, {"replication", s.Replication}, {"rewind", s.Rewind}} {
		credentials := auth[tc.name].(map[string]any)
		if credentials["username"] != tc.credentials.Username || credentials["password"] != tc.credentials.Password {
			t.Fatal("HA recovery replaced target identity", tc.name)
		}
	}
	// Patroni custom bootstrap sets its configured superuser password before this
	// hook; the hook then applies our SCRAM verifiers, not source passwords.
	if !strings.Contains(rendered["bootstrap.pgpass"], s.Superuser.Password) || !strings.Contains(rendered["bootstrap-roles.sh"], "roles.sql") {
		t.Fatal("bootstrap cannot authenticate with target identity")
	}
	for _, credentials := range []pgspec.Credentials{s.Superuser, s.Monitor, s.Replication, s.Rewind} {
		if strings.Contains(rendered["roles.sql"], credentials.Password) {
			t.Fatal("plaintext system password in recovery SQL")
		}
	}
	if !strings.Contains(rendered["roles.sql"], "SCRAM-SHA-256$") {
		t.Fatal("recovery SQL omitted system rekey")
	}
	bootstrap := config["bootstrap"].(map[string]any)
	if bootstrap["method"] != "pgbackrest" || bootstrap["post_bootstrap"] != filepath.Join(p.root, "bootstrap-roles.sh") {
		t.Fatal("HA custom bootstrap bypassed system rekey hook")
	}
}

func TestRecoveryPasswordOnlyActionValidation(t *testing.T) {
	verifier := testVerifier()
	request := pgspec.AccessRequest{Operation: "password", Role: &pgspec.RoleWrite{Role: pgspec.Role{Name: `app"quoted`}, Password: &verifier}}
	if err := validateAccessRequest(pgspec.ActionRoles, request); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"postgres", "kk_monitor", "pg_monitor"} {
		request.Role.Name = name
		if err := validateAccessRequest(pgspec.ActionRoles, request); err == nil {
			t.Fatal("recovery password action accepted protected role", name)
		}
	}
	request.Role.Name = "app"
	plaintext := "plaintext-password"
	request.Role.Password = &plaintext
	if err := validateAccessRequest(pgspec.ActionRoles, request); err == nil {
		t.Fatal("recovery password action accepted plaintext")
	}
}

func TestRecoveryPasswordBindingValidation(t *testing.T) {
	verifier := testVerifier()
	for _, name := range []string{"app", "kb_0123456789abcdef0123456789abcdef"} {
		request := pgspec.AccessRequest{Binding: true, Operation: "password", Role: &pgspec.RoleWrite{Role: pgspec.Role{Name: name}, Password: &verifier}}
		if err := validateAccessRequest(pgspec.ActionRoles, request); err == nil {
			t.Fatal("ordinary password mutation bypassed binding identity fence")
		}
		request.Operation = "recover-password"
		if err := validateAccessRequest(pgspec.ActionRoles, request); err != nil {
			t.Fatal("restore-only password request rejected", err)
		}
		for _, protected := range []string{"postgres", "kk_monitor", "pg_monitor"} {
			request.Role.Name = protected
			if err := validateAccessRequest(pgspec.ActionRoles, request); err == nil {
				t.Fatal("recovery accepted protected identity", protected)
			}
		}
	}
}

func TestRecoveryPasswordLocalRequiresExplicitFanout(t *testing.T) {
	for _, operation := range []string{"password", "recover-password", "recover-password-local"} {
		for _, citus := range []bool{false, true} {
			for _, held := range []bool{false, true} {
				s := testSpec()
				s.Postgres.RecoveryClientsHeld = held
				if citus {
					s.Citus = &pgspec.Citus{Database: "app"}
				}
				req := pgspec.AccessRequest{Operation: operation}
				local, err := recoveryPasswordLocal(s, pgspec.ActionRoles, req)
				wantLocal := operation == "recover-password-local" && citus && held
				wantError := operation != "password" && !held || operation == "recover-password-local" && !citus
				if local != wantLocal || (err != nil) != wantError {
					t.Fatalf("operation=%s citus=%t held=%t: local=%t err=%v", operation, citus, held, local, err)
				}
				// Old control planes only send legacy operations: even with
				// new agents and held Citus clients they must still propagate.
				if operation != "recover-password-local" && local {
					t.Fatal("legacy coordinator-only recovery silently became local")
				}
			}
		}
	}
	verifier := testVerifier()
	for _, binding := range []bool{false, true} {
		name := "application"
		if binding {
			name = "kb_0123456789abcdef0123456789abcdef"
		}
		req := pgspec.AccessRequest{Operation: "recover-password-local", Binding: binding, Role: &pgspec.RoleWrite{Role: pgspec.Role{Name: name}, Password: &verifier}}
		if err := validateAccessRequest(pgspec.ActionRoles, req); err != nil {
			t.Fatal("explicit local recovery rejected", err)
		}
		for _, reserved := range []string{"postgres", "kk_monitor", "pg_monitor"} {
			req.Role.Name = reserved
			if err := validateAccessRequest(pgspec.ActionRoles, req); err == nil {
				t.Fatal("local recovery accepted protected identity", reserved)
			}
		}
	}
}
