// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/xdg-go/scram"
)

func assertRecoveryPassword(t *testing.T, verifier, password string) {
	t.Helper()
	parts := strings.Split(verifier, "$")
	if len(parts) != 3 {
		t.Fatal("invalid recovery verifier")
	}
	_, encodedSalt, ok := strings.Cut(parts[1], ":")
	if !ok {
		t.Fatal("invalid recovery salt")
	}
	salt, err := base64.StdEncoding.DecodeString(encodedSalt)
	if err != nil {
		t.Fatal(err)
	}
	client, err := scram.SHA256.NewClient("", password, "")
	if err != nil {
		t.Fatal(err)
	}
	stored := client.GetStoredCredentials(scram.KeyFactors{Salt: string(salt), Iters: 4096})
	expected := base64.StdEncoding.EncodeToString(stored.StoredKey) + ":" + base64.StdEncoding.EncodeToString(stored.ServerKey)
	if parts[2] != expected {
		t.Fatal("recovered SQL verifier does not match routed password")
	}
}

func TestManagedRoleRecoveryManifestCloneAndCurrentRotation(t *testing.T) {
	for _, clone := range []bool{true, false} {
		name := "in-place-current-rotation"
		if clone {
			name = "clone-snapshot"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, _ := backupFixture(t, "single")
			const oldPassword = "snapshot-app-password"
			if err := putSecret(t.Context(), rc, "access/roles", []byte(`["app","not-recovered"]`)); err != nil {
				t.Fatal(err)
			}
			for _, role := range []string{"app", "not-recovered"} {
				if err := putSecret(t.Context(), rc, rolePasswordKey(role), []byte(oldPassword)); err != nil {
					t.Fatal(err)
				}
			}
			backup := &domain.Backup{Namespace: "default", RecoveryIdentity: "snapshot"}
			if err := p.captureManagedRoles(t.Context(), rc, backup); err != nil {
				t.Fatal(err)
			}
			if clone {
				if err := p.restoreManagedRoles(t.Context(), rc, backup); err != nil {
					t.Fatal(err)
				}
			}
			var appliedVerifier string
			rotating := false
			expected := oldPassword
			installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/postgres/status":
					json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "standalone"})
				case "/v1/postgres/roles":
					var request pgspec.AccessRequest
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
					}
					switch request.Operation {
					case "capabilities":
						w.WriteHeader(http.StatusBadRequest)
					case "list":
						json.NewEncoder(w).Encode([]Role{{Name: "app", Login: true, ConnectionLimit: 42, MemberOf: []string{"pg_read_all_data"}}})
					case "update":
						if !rotating || request.Role.Password == nil {
							t.Error("recovery overwrote catalog attributes")
						}
						json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
					case "password":
						if request.Binding || request.Role.Name != "app" || request.Role.Password == nil {
							t.Error("ordinary recovery password action entered binding mutation scope")
						} else {
							appliedVerifier = *request.Role.Password
						}
						json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
					default:
						t.Error("recovery must not overwrite catalog attributes", request.Operation)
						w.WriteHeader(400)
					}
				default:
					t.Error("unexpected recovery route", r.URL.Path)
					w.WriteHeader(400)
				}
			})
			if !clone {
				rotating = true
				var err error
				expected, err = p.RotateRolePassword(t.Context(), rc, "app")
				if err != nil {
					t.Fatal(err)
				}
				rotating = false
			}
			if err := p.reconcileRecoveredRoles(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			users, err := p.proxyUsers(t.Context(), rc)
			if err != nil || len(users) != 1 || users[0].Username != "app" || users[0].Password != expected {
				t.Fatal("incorrect restored proxy manifest", err)
			}
			assertRecoveryPassword(t, appliedVerifier, users[0].Password)
			if _, err := secret(t.Context(), rc, rolePasswordKey("not-recovered")); !missing(err) {
				t.Fatal("absent catalog role retained proxy password", err)
			}
			if strings.Contains(string(rc.Cluster.Internal), expected) {
				t.Fatal("recovery password leaked into cluster metadata")
			}
		})
	}
}

func TestRecoverySuperuserRemainsTransientAndTargetOwned(t *testing.T) {
	p, rc, s := backupFixture(t, "single")
	b := &domain.Backup{Namespace: "default", Cluster: "source", Name: "base", Storage: "archive", Provider: "postgres", Phase: domain.PhaseSucceeded, SourceVersion: rc.Cluster.Spec.Version, RecoveryIdentity: "snapshot", Postgres: &domain.PostgresBackup{Label: "20261017-021859F", Repository: "prefix/postgres/default/source"}}
	if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	for _, user := range systemNames {
		if err := rc.Secrets.SetClusterSecret(t.Context(), domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}, "users/"+user, []byte("source-only-password")); err != nil {
			t.Fatal(err)
		}
	}
	if err := rc.Secrets.SetClusterSecret(t.Context(), domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}, "access/manifest", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: b.Cluster, Backup: b.Name}
	s.Members[0].Stage = "planned"
	if _, err := p.syncBackups(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	sp := pgspec.Spec{Postgres: &pgspec.Postgres{}, Bootstrap: &pgspec.Bootstrap{Method: "initdb"}}
	if err := p.configureBackup(t.Context(), rc, s, &sp); err != nil {
		t.Fatal(err)
	}
	if sp.Bootstrap.RecoverySuperuser == nil || sp.Bootstrap.RecoverySuperuser.Password != "source-only-password" {
		t.Fatal("missing transient bootstrap credential")
	}
	for _, user := range systemNames {
		value, err := secret(t.Context(), rc, "users/"+user)
		if err != nil || string(value) != "protected-password" {
			t.Fatal("restore replaced target-owned system credential", user, err)
		}
	}
	if strings.Contains(string(rc.Cluster.Internal), "source-only-password") {
		t.Fatal("transient recovery credential leaked into state")
	}
}

func TestPITRRejectsUploadTimeBeforeAnyRestoreStop(t *testing.T) {
	p, rc, _ := backupFixture(t, "single")
	start := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	covered := start.Add(time.Minute)
	upload := start.Add(2 * time.Minute)
	target := covered.Add(30 * time.Second)
	rc.Cluster.Spec.Backup.PITR.Enabled = true
	b := &domain.Backup{Namespace: "default", Cluster: "demo", Name: "base", Storage: "archive", Provider: "postgres", Phase: domain.PhaseSucceeded, CompletedAt: &start, SourceVersion: rc.Cluster.Spec.Version, Postgres: &domain.PostgresBackup{Label: "20261017-021859F", Repository: "prefix/postgres/default/demo-incarnation", SystemID: "system"}}
	if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	stops := 0
	installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/postgres/status" {
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "standalone", SystemID: "system", LastArchivedAt: upload.Format(time.RFC3339Nano), RecoverableUntil: covered.Format(time.RFC3339Nano)})
			return
		}
		stops++
		t.Error("invalid target mutated an agent", r.URL.Path)
		w.WriteHeader(400)
	})
	windows, err := p.ValidatePITRRestore(t.Context(), rc, covered)
	if err != nil || len(windows) != 1 || !windows[0].End.Equal(covered) {
		t.Fatal("valid content boundary rejected", err)
	}
	if _, err = p.ValidatePITRRestore(t.Context(), rc, target); err == nil {
		t.Fatal("archive upload wall time accepted as WAL coverage")
	}
	r := &domain.Restore{Namespace: "default", Cluster: "demo", Name: "invalid", Phase: domain.PhasePending, Source: domain.RestoreSource{Backup: b.Name, PITRTime: &target}}
	if err := rc.Store.CreateRestore(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "invalid-restore", Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"invalid"}`)}
	before := string(rc.Cluster.Internal)
	if _, err := p.runBackupOperation(t.Context(), rc, op); err == nil {
		t.Fatal("unsafe queued restore accepted")
	}
	if stops != 0 || string(rc.Cluster.Internal) != before {
		t.Fatal("validation occurred after destructive state transition")
	}
}

func TestRecoveryProxyHoldIncludesSystemLogin(t *testing.T) {
	p, rc, _ := setup(t, "single")
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.allocate(rc.Cluster)
	if err = p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if err = save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if err = putSecret(t.Context(), rc, "access/roles", []byte(`["app"]`)); err != nil {
		t.Fatal(err)
	}
	if err = putSecret(t.Context(), rc, rolePasswordKey("app"), []byte("application-password")); err != nil {
		t.Fatal(err)
	}
	for _, held := range []bool{true, false} {
		if err = saveBackupState(rc.Cluster, backupState{ManagedRolesHeld: held}); err != nil {
			t.Fatal(err)
		}
		config, err := p.MemberConfig(t.Context(), rc, "demopx")
		if err != nil {
			t.Fatal(err)
		}
		var spec pgspec.Spec
		if err = json.Unmarshal(config.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		want := 2
		if held {
			want = 0
		}
		if len(spec.Proxy.Users) != want {
			t.Fatal("recovery proxy hold did not cover every login", held, len(spec.Proxy.Users))
		}
	}
}

func TestDeletingRecoveryProvenanceDeletesManagedManifest(t *testing.T) {
	p, rc, _ := backupFixture(t, "single")
	backup := domain.Backup{Namespace: "default", RecoveryIdentity: "snapshot-delete"}
	if err := p.captureManagedRoles(t.Context(), rc, &backup); err != nil {
		t.Fatal(err)
	}
	if err := deleteRecoverySecrets(t.Context(), rc.Secrets, backup); err != nil {
		t.Fatal(err)
	}
	if _, err := rc.Secrets.GetClusterSecret(t.Context(), domain.ClusterRef{Namespace: backup.Namespace, Name: backup.RecoveryIdentity}, "access/manifest"); !missing(err) {
		t.Fatal("deleted backup retained its sealed managed passwords", err)
	}
}

func TestRecoveredBindingPasswordUsesHeldOnlyAction(t *testing.T) {
	p, rc, _ := backupFixture(t, "single")
	const name = "kb_0123456789abcdef0123456789abcdef"
	if err := putSecret(t.Context(), rc, "access/roles", []byte(`["`+name+`"]`)); err != nil {
		t.Fatal(err)
	}
	if err := putSecret(t.Context(), rc, rolePasswordKey(name), []byte("current-binding-password")); err != nil {
		t.Fatal(err)
	}
	applied := false
	installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/postgres/status":
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "standalone"})
		case "/v1/postgres/roles":
			var req pgspec.AccessRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			if req.Operation == "list" {
				json.NewEncoder(w).Encode([]Role{{Name: name, Login: true}})
				return
			}
			if req.Operation != "recover-password" || !req.Binding || req.Role == nil || req.Role.Name != name || req.Role.Password == nil {
				t.Error("binding recovery bypassed restore-only password protocol")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			assertRecoveryPassword(t, *req.Role.Password, "current-binding-password")
			applied = true
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	if err := p.reconcileRecoveredRoles(t.Context(), rc); err != nil || !applied {
		t.Fatal("binding password recovery did not complete", err)
	}
}
