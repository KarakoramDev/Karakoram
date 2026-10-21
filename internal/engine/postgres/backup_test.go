// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func backupFixture(t *testing.T, topology string) (*Core, *engine.Context, *state) {
	t.Helper()
	p, rc, _ := setup(t, topology)
	p.restoreSetPresent = func(context.Context, *domain.BackupStorage, string) (bool, error) { return true, nil }
	p.options.Store = rc.Store
	s, _ := load(rc.Cluster)
	s.UID = "incarnation"
	s.Topology = topology
	s.allocate(rc.Cluster)
	for _, m := range s.Members {
		m.Stage = "joined"
		m.Version = rc.Cluster.Spec.Version
		m.Topology = topology
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	for _, user := range systemNames {
		if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), "users/"+user, []byte("protected-password")); err != nil {
			t.Fatal(err)
		}
	}
	rc.Cluster.Spec.Backup.Storage = "archive"
	storage := &domain.BackupStorage{Namespace: rc.Cluster.Ref().Namespace, Name: "archive", Type: "s3", EndpointURL: "https://objects.example", Bucket: "backups", Region: "region", Prefix: "prefix", ForcePathStyle: true, VerifyTLS: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
	if err := rc.Store.(*store.Store).CreateBackupStorage(t.Context(), storage); err != nil {
		t.Fatal(err)
	}
	return p, rc, s
}
func installBackupAgent(t *testing.T, rc *engine.Context, handler http.HandlerFunc) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	rc.Agents = fakeAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
}
func TestBackupStanzaWaitsForProvisioning(t *testing.T) {
	p, rc, s := backupFixture(t, "ha")
	s.data()[0].Stage = "planned"
	calls := 0
	installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Error(w, "not provisioned", http.StatusServiceUnavailable)
	})
	if handled, err := p.syncBackups(t.Context(), rc, s); handled || err != nil {
		t.Fatalf("backup setup blocked provisioning: handled=%v err=%v", handled, err)
	}
	if calls != 0 {
		t.Fatal("backup setup tried to resolve agents before provisioning")
	}
}

func TestBackupCheckpointResumeAndMetadata(t *testing.T) {
	p, rc, _ := backupFixture(t, "single")
	clock := &backupCleanupClock{at: time.Date(2026, 10, 23, 17, 0, 0, 0, time.UTC)}
	rc.Clock = clock
	// Desired major may already differ while a queued old-major backup runs.
	rc.Cluster.Spec.Version = "18.6.1"
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"profiles":["postgis","pgvector","postgis"]}`)
	calls := 0
	inventory := pgspec.BackupInfoResponse{Backups: []pgspec.BackupInfo{{Label: "20261017-021859F", Type: "full", StartedAt: "2026-10-17T02:18:59Z", StoppedAt: "2026-10-17T02:19:06Z", WALStart: "00000001000000000000000A", WALStop: "00000001000000000000000A", SystemID: "1234567890123456789", Version: "17", SizeBytes: 500, RepoBytes: 100}}}
	installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/postgres/status":
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Role: "standalone", Running: true, Timeline: 1, SystemID: "1234567890123456789", Version: "17.11.1"})
		case "/v1/postgres/stanza-create", "/v1/reload":
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/postgres/backup":
			var req pgspec.BackupRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			wantReceipt := "durable-operation"
			if calls > 0 {
				wantReceipt += "/backup-retry-1"
			}
			if req.Receipt != wantReceipt || req.Repository == nil || req.Repository.Path != "/prefix/postgres/default/demo-incarnation" {
				t.Error("missing durable receipt or scoped repository")
			}
			calls++
			state := "failed"
			if calls == 2 {
				state = "running"
			} else if calls > 2 {
				state = "done"
			}
			json.NewEncoder(w).Encode(pgspec.BackupResponse{State: state, Label: inventory.Backups[0].Label, Message: "pgBackRest error [082]: required WAL segment was not archived"})
		case "/v1/postgres/backup-info":
			json.NewEncoder(w).Encode(inventory)
		default:
			t.Errorf("unexpected route %s", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	b := &domain.Backup{Namespace: "default", Name: "baseline", Cluster: "demo", Storage: "archive", Type: "full", Trigger: "restore", Phase: domain.PhasePending}
	if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "durable-operation", Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"baseline"}`)}
	for range 10 {
		// Reconstructing Core on each checkpoint simulates CP process replacement.
		p = New(Options{Store: rc.Store})
		result, err := p.runBackupOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
		clock.at = clock.at.Add(result.RequeueAfter)
		// A later source selection must not rewrite the snapshot requirements.
		rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"profiles":["pgvector"]}`)
		if result.Done {
			break
		}
	}
	b, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), b.Name)
	if err != nil || b.Phase != domain.PhaseSucceeded || b.Postgres == nil || b.Error != "" {
		t.Fatal(b, err)
	}
	if b.Postgres.SystemID != "1234567890123456789" || b.Postgres.RepoBytes != 100 || b.Size != 500 || b.CompletedAt == nil || b.Postgres.Label != inventory.Backups[0].Label {
		t.Fatalf("metadata lost: %+v", b)
	}
	if b.SourceVersion != "17.11.1" || b.Postgres.Version != "17" {
		t.Fatalf("backup stamped desired instead of observed/native version: %+v", b)
	}
	if strings.Join(b.Postgres.Profiles, ",") != "pgvector,postgis" {
		t.Fatalf("snapshot profile requirements lost or rewritten: %+v", b.Postgres)
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil || !bs.Replicas {
		t.Fatal("first full did not enable pgBackRest replica rebuild", bs, err)
	}
	before := calls
	if result, err := p.runBackupOperation(t.Context(), rc, op); err != nil || !result.Done || calls != before {
		t.Fatal("terminal checkpoint started another backup", err)
	}
}

func TestBackupFailurePreservesNativeDiagnostic(t *testing.T) {
	for _, message := range []string{
		"pgBackRest error [082]: required WAL segment was not archived before the archive timeout",
		"",
	} {
		t.Run(message, func(t *testing.T) {
			p, rc, _ := backupFixture(t, "single")
			installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/postgres/status" {
					json.NewEncoder(w).Encode(pgspec.StatusResponse{Role: "standalone", Running: true, Timeline: 2})
					return
				}
				if r.URL.Path != "/v1/postgres/backup" {
					t.Errorf("unexpected action: %s", r.URL.Path)
				}
				json.NewEncoder(w).Encode(pgspec.BackupResponse{State: "failed", Message: message})
			})
			b := &domain.Backup{Namespace: "default", Cluster: "demo", Name: "failed", Storage: "archive", Type: "full", Phase: domain.PhaseRunning}
			if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			cp, _ := json.Marshal(backupCheckpoint{Step: "backup", Member: "demop0", Timeline: 2})
			op := &domain.Operation{ID: "failed-native", Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"failed"}`), Checkpoint: cp}
			result, err := p.runBackupOperation(t.Context(), rc, op)
			saved, getErr := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), b.Name)
			if getErr != nil || err == nil || !result.Done || saved.Phase != domain.PhaseFailed || saved.CompletedAt == nil || saved.Error == "" || saved.Error != err.Error() {
				t.Fatalf("failure diagnostic not preserved: result=%+v backup=%+v error=%v get=%v", result, saved, err, getErr)
			}
			if message != "" && saved.Error != message {
				t.Fatal("native error replaced with generic failure", saved.Error)
			}
		})
	}
}

func TestRestoreFreshBackupRetriesAreBoundedAndDurable(t *testing.T) {
	_, rc, _ := backupFixture(t, "single")
	clock := &backupCleanupClock{at: time.Date(2026, 10, 23, 17, 0, 0, 0, time.UTC)}
	rc.Clock = clock
	var receipts []string
	installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/postgres/status" {
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Role: "standalone", Running: true, Timeline: 2})
			return
		}
		if r.URL.Path != "/v1/postgres/backup" {
			t.Errorf("retry invoked non-backup action: %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		var request pgspec.BackupRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		receipts = append(receipts, request.Receipt)
		json.NewEncoder(w).Encode(pgspec.BackupResponse{State: "failed", Message: "pgBackRest error [082]: required WAL segment was not archived"})
	})
	b := &domain.Backup{Namespace: "default", Cluster: "demo", Name: "fresh", Trigger: "restore", Storage: "archive", Type: "full", Phase: domain.PhaseRunning}
	if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	checkpoint, _ := json.Marshal(backupCheckpoint{Step: "backup", Member: "demop0", Timeline: 2})
	op := &domain.Operation{ID: "fresh-backup", Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"fresh"}`), Checkpoint: checkpoint}
	wantReceipts := []string{"fresh-backup", "fresh-backup/backup-retry-1", "fresh-backup/backup-retry-2"}
	for attempt := range 3 {
		p := New(Options{Store: rc.Store})
		result, err := p.runBackupOperation(t.Context(), rc, op)
		op.Checkpoint = result.Checkpoint
		if len(receipts) != attempt+1 || receipts[attempt] != wantReceipts[attempt] {
			t.Fatalf("deadline did not dispatch expected receipt: %v", receipts)
		}
		if attempt < 2 {
			if err != nil || result.Done || result.RequeueAfter != 30*time.Second || !strings.Contains(result.Message, "[082]") {
				t.Fatalf("retry lost bound/reason: %+v %v", result, err)
			}
			var saved backupCheckpoint
			if err := json.Unmarshal(op.Checkpoint, &saved); err != nil {
				t.Fatal(err)
			}
			deadline := clock.at.Add(30 * time.Second)
			if !saved.BackupRetryNotBefore.Equal(deadline) {
				t.Fatalf("retry deadline not durable: %s, want %s", saved.BackupRetryNotBefore, deadline)
			}
			for _, remaining := range []time.Duration{30 * time.Second, 25 * time.Second, time.Second} {
				clock.at = deadline.Add(-remaining)
				// Restore the entire operation from serialized state, as a
				// controller restart would, rather than retaining a live checkpoint.
				raw, marshalErr := json.Marshal(op)
				if marshalErr != nil {
					t.Fatal(marshalErr)
				}
				var resumed domain.Operation
				if err := json.Unmarshal(raw, &resumed); err != nil {
					t.Fatal(err)
				}
				p := New(Options{Store: rc.Store})
				early, earlyErr := p.runBackupOperation(t.Context(), rc, &resumed)
				if earlyErr != nil || early.Done || early.RequeueAfter != remaining || len(receipts) != attempt+1 {
					t.Fatalf("early reconcile dispatched backup or lost delay: result=%+v err=%v receipts=%v", early, earlyErr, receipts)
				}
				op.Checkpoint = early.Checkpoint
			}
			clock.at = deadline
		} else if err == nil || !result.Done || !strings.Contains(err.Error(), "after 3 attempts") || !strings.Contains(err.Error(), "[082]") {
			t.Fatalf("exhaustion not terminal/truthful: %+v %v", result, err)
		}
	}
	if strings.Join(receipts, ",") != "fresh-backup,fresh-backup/backup-retry-1,fresh-backup/backup-retry-2" {
		t.Fatal("retry receipts were not distinct/durable", receipts)
	}
	var completed backupCheckpoint
	if err := json.Unmarshal(op.Checkpoint, &completed); err != nil || !completed.BackupRetryNotBefore.IsZero() {
		t.Fatalf("due retry did not clear its deadline: %+v %v", completed, err)
	}
}

func TestBackupRoleChangeFailsHonestly(t *testing.T) {
	p, rc, _ := backupFixture(t, "ha")
	installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		role := "replica"
		if name == "demop1" {
			role = "primary"
		}
		json.NewEncoder(w).Encode(pgspec.StatusResponse{Role: role, Running: true, Timeline: 2})
	})
	b := &domain.Backup{Namespace: "default", Name: "changing", Cluster: "demo", Storage: "archive", Type: "full", Phase: domain.PhaseRunning}
	if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	cp, _ := json.Marshal(backupCheckpoint{Step: "backup", Member: "demop0", Timeline: 1})
	op := &domain.Operation{Kind: domain.OperationBackup, Checkpoint: cp, Payload: json.RawMessage(`{"backup":"changing"}`)}
	if _, err := p.runBackupOperation(t.Context(), rc, op); err == nil {
		t.Fatal("role-changing backup reported success")
	}
	b, _ = rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), b.Name)
	if b.Phase != domain.PhaseFailed {
		t.Fatal("backup failure not durable")
	}
}
func TestRetentionDependenciesAndWindow(t *testing.T) {
	stop := time.Date(2026, 10, 17, 2, 0, 0, 0, time.UTC)
	items := []domain.Backup{
		{Provider: "postgres", Type: "full", Phase: domain.PhaseSucceeded, CompletedAt: &stop, Postgres: &domain.PostgresBackup{Label: "full", SystemID: "system"}},
		{Postgres: &domain.PostgresBackup{Label: "diff", Prior: "full"}},
		{Postgres: &domain.PostgresBackup{Label: "incr", References: []string{"full", "diff"}}},
		{Postgres: &domain.PostgresBackup{Label: "other"}},
	}
	removed := removedBackupLabels(items, "full")
	if !removed["full"] || !removed["diff"] || !removed["incr"] || removed["other"] {
		t.Fatal("invalid dependency closure", removed)
	}
	windows := consistentWindow(items, "postgres", "system", stop.Add(time.Minute))
	if len(windows) != 1 || !windows[0].Start.Equal(stop) {
		t.Fatal(windows)
	}
	if len(consistentWindow(items, "supabase", "system", stop.Add(time.Minute))) != 0 || len(consistentWindow(items, "postgres", "other", stop.Add(time.Minute))) != 0 || len(consistentWindow(items, "postgres", "system", stop.Add(-time.Minute))) != 0 {
		t.Fatal("unsafe PITR window")
	}
}
func TestRestoreChoreography(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		topology string
		cross    bool
		recovery bool
	}{
		{"single", "single", false, false},
		{"ha", "ha", false, false},
		{"single-cross-source", "single", true, false},
		{"ha-cross-source", "ha", true, false},
		{"single-recovery", "single", false, true},
		{"ha-recovery", "ha", false, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			topology := scenario.topology
			p, rc, s := backupFixture(t, topology)
			stop := time.Now().UTC().Add(-time.Hour)
			b := &domain.Backup{Namespace: "default", Name: "source", Cluster: "demo", Storage: "archive", Provider: "postgres", Type: "full", Phase: domain.PhaseSucceeded, SourceVersion: "17.11.1", CompletedAt: &stop, Postgres: &domain.PostgresBackup{Label: "20261017-021859F", Repository: "prefix/postgres/default/demo-incarnation"}}
			b.RecoveryIdentity = "snapshot-identity"
			for _, user := range systemNames {
				if err := rc.Secrets.SetClusterSecret(t.Context(), domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}, "users/"+user, []byte("restored-password")); err != nil {
					t.Fatal(err)
				}
			}
			if scenario.cross {
				b.Cluster = "other"
				b.Postgres.Repository = "prefix/postgres/default/other-incarnation"
			}
			if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			r := &domain.Restore{Namespace: "default", Cluster: "demo", Name: "recovery", Source: domain.RestoreSource{Backup: b.Name, SourceCluster: b.Cluster}, Phase: domain.PhasePending}
			if err := rc.Store.CreateRestore(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			running := map[string]bool{}
			for _, m := range s.data() {
				running[m.Name] = true
			}
			var actions []string
			finished := 0
			published := false
			reloaded := map[string]bool{}
			installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
				member := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
				switch r.URL.Path {
				case "/v1/postgres/status":
					role := "standalone"
					if topology == "ha" {
						role = "replica"
						if member == "demop0" {
							role = "primary"
						}
					}
					json.NewEncoder(w).Encode(pgspec.StatusResponse{Role: role, Running: running[member], Streaming: true})
				case "/v1/postgres/prepare-restore":
					var request pgspec.PrepareRestoreRequest
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
					}
					if !request.Wipe {
						password, err := secret(r.Context(), rc, "users/postgres")
						if err != nil || string(password) != "protected-password" {
							t.Error("snapshot credentials adopted before every old member was fenced")
						}
					}
					actions = append(actions, "stop:"+member)
					running[member] = false
					json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
				case "/v1/postgres/dcs-reset":
					for _, live := range running {
						if live {
							t.Error("DCS reset with running member")
						}
					}
					actions = append(actions, "dcs-reset")
					json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
				case "/v1/postgres/restore":
					if running[member] {
						t.Error("online restore")
					}
					actions = append(actions, "restore")
					json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
				case "/v1/processes/postgres/start", "/v1/processes/patroni/start":
					if !reloaded[member] {
						t.Error("restored process started before publishing recovery configuration")
					}
					running[member] = true
					password, err := secret(r.Context(), rc, "users/kk_proxy_admin")
					if err != nil || string(password) != "protected-password" {
						t.Error("restore replaced target-local ProxySQL admin credential")
					}
					actions = append(actions, "start:"+member)
					json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
				case "/v1/reload":
					password, err := secret(r.Context(), rc, "users/postgres")
					if err != nil || string(password) != "protected-password" {
						t.Error("restore replaced target-owned system credentials")
					}
					reloaded[member] = true
					bs, err := loadBackupState(rc.Cluster)
					if err != nil {
						t.Fatal(err)
					}
					if bs.Restore == nil {
						published = true
					}
					json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
				case "/v1/postgres/finish-restore":
					if !published {
						t.Error("archive override cleared before publishing target repository")
					}
					finished++
					json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
				case "/v1/postgres/rekey":
					if !running[member] {
						t.Error("rekey requires a running recovered primary")
					}
					json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
				case "/v1/postgres/stanza-create", "/v1/postgres/stanza-upgrade":
					if strings.HasSuffix(r.URL.Path, "stanza-upgrade") {
						t.Error("restore must create its isolated stanza, not upgrade another lineage")
					}
					if scenario.cross && finished != 0 {
						t.Error("fresh stanza was not initialized before enabling archiving")
					}
					json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
				default:
					t.Errorf("unexpected restore route %s", r.URL.Path)
					http.NotFound(w, r)
				}
			})
			op := &domain.Operation{ID: "restore-operation", Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"recovery"}`)}
			if scenario.recovery {
				// Failure cleanup acquires recovery ownership only after a
				// durable hold, not for an untouched preflight-only request.
				first, err := p.runBackupOperation(t.Context(), rc, op)
				if err != nil {
					t.Fatal(err)
				}
				op.Checkpoint = first.Checkpoint
				if _, err := p.CancelOperation(t.Context(), rc, op); err != nil {
					t.Fatal(err)
				}
			}
			completed := false
			for range 80 {
				p = New(Options{Store: rc.Store})
				p.restoreSetPresent = func(context.Context, *domain.BackupStorage, string) (bool, error) {
					return true, nil
				}
				var result engine.OpResult
				var err error
				if scenario.recovery {
					current, loadErr := load(rc.Cluster)
					if loadErr != nil {
						t.Fatal(loadErr)
					}
					_, err = p.reconcileRestoreRecovery(t.Context(), rc, current)
					if saveErr := save(rc.Cluster, current); saveErr != nil {
						t.Fatal(saveErr)
					}
					if current.RestoreRecovery == nil {
						result.Done = true
					} else {
						result.Checkpoint = current.RestoreRecovery.Checkpoint
					}
				} else {
					result, err = p.runBackupOperation(t.Context(), rc, op)
				}
				if err != nil {
					t.Fatalf("restore checkpoint %s: %v", result.Step, err)
				}
				op.Checkpoint = result.Checkpoint
				if result.Done {
					completed = true
					break
				}
			}
			if !completed {
				t.Fatal("restore never completed", actions)
			}
			r, _ = rc.Store.GetRestore(t.Context(), rc.Cluster.Ref(), r.Name)
			if r.Phase != domain.PhaseSucceeded {
				t.Fatal(r.Phase)
			}
			for member, live := range running {
				if !live {
					t.Errorf("restore did not explicitly release stop hold for %s", member)
				}
			}
			if topology == "ha" && !strings.Contains(strings.Join(actions, ","), "dcs-reset") {
				t.Fatal("HA DCS not reset", actions)
			}
			wantFinished := len(s.data())
			if finished != wantFinished {
				t.Fatalf("archive cutover count = %d, want %d", finished, wantFinished)
			}
		})
	}
}
func TestRestoreProviderMatchExact(t *testing.T) {
	p, rc, _ := backupFixture(t, "single")
	b := &domain.Backup{Namespace: "default", Name: "source", Cluster: "demo", Storage: "archive", Provider: "supabase", Type: "full", Phase: domain.PhaseSucceeded, SourceVersion: "17.11.1", Postgres: &domain.PostgresBackup{Label: "20261017-021859F", Repository: "prefix/postgres/default/demo-incarnation"}}
	if err := rc.Store.CreateBackup(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.resolveRestore(t.Context(), rc, restoreSource{Namespace: "default", Cluster: "demo", Backup: b.Name}); err == nil {
		t.Fatal("supabase backup accepted by postgres")
	}
}

func TestCloneQueuesOnlyAfterProvisioningAndOnlyOnce(t *testing.T) {
	p, rc, s := backupFixture(t, "single")
	b := &domain.Backup{
		Namespace: "default", Cluster: "source", Name: "base", Storage: "archive",
		Provider: "postgres", Type: "full", Phase: domain.PhaseSucceeded,
		SourceVersion: "17.11.1", RecoveryIdentity: "snapshot-credentials",
		Postgres: &domain.PostgresBackup{Label: "20261017-021859F", Repository: "prefix/postgres/default/source-old"},
	}
	if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	for _, user := range systemNames {
		if err := rc.Secrets.SetClusterSecret(t.Context(), domain.ClusterRef{Namespace: "default", Name: b.RecoveryIdentity}, "users/"+user, []byte("snapshot-secret")); err != nil {
			t.Fatal(err)
		}
	}
	if err := rc.Secrets.SetClusterSecret(t.Context(), domain.ClusterRef{Namespace: "default", Name: b.RecoveryIdentity}, "access/manifest", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: b.Cluster, Backup: b.Name}
	s.Members[0].Stage = "planned"
	if _, err := p.syncBackups(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	bs, _ := loadBackupState(rc.Cluster)
	if !bs.CloneCredentials || bs.InitialQueued {
		t.Fatal("restore queued before provisioning, or recovery credentials not prepared", bs)
	}
	password, err := rc.Secrets.GetClusterSecret(t.Context(), rc.Cluster.Ref(), "users/postgres")
	if err != nil || string(password) != "protected-password" {
		t.Fatal("clone replaced target-owned system credentials", err)
	}
	proxyPassword, err := secret(t.Context(), rc, "users/kk_proxy_admin")
	if err != nil || string(proxyPassword) != "protected-password" {
		t.Fatal("clone replaced target-local ProxySQL admin credential", err)
	}
	s.Members[0].Stage = "joined"
	for _, member := range s.Members {
		if member.Role == "proxy" {
			// Native data recovery must finish even if this desired proxy
			// cannot be placed; its correction is fenced while recovery runs.
			member.Stage = "planned"
		}
	}
	for range 2 {
		if _, err := p.syncBackups(t.Context(), rc, s); err != nil {
			t.Fatal(err)
		}
	}
	bs, _ = loadBackupState(rc.Cluster)
	if !bs.InitialQueued {
		t.Fatal("initial restore not queued")
	}
	operation, err := rc.Store.GetOperation(t.Context(), rc.Cluster.Ref().Key()+"-"+s.UID+"-initial-restore")
	if err != nil || operation.Kind != domain.OperationRestore {
		t.Fatal("missing deterministic initial restore operation", err)
	}
	if strings.Contains(string(rc.Cluster.Internal), "snapshot-secret") {
		t.Fatal("snapshot password leaked into internal state")
	}
}

func TestRetentionPreservesNativeChains(t *testing.T) {
	p := New(Options{})
	backup := func(name, label, kind, prior, repo string) domain.Backup {
		return domain.Backup{Name: name, Type: kind, Storage: "archive", Phase: domain.PhaseSucceeded, Postgres: &domain.PostgresBackup{Label: label, Prior: prior, Repository: repo}}
	}
	full := backup("full", "20261017-010000F", "full", "", "repo")
	diff := backup("diff", "20261017-010000F_20261017-010100D", "diff", full.Postgres.Label, "repo")
	incr := backup("incr", "20261017-010000F_20261017-010200I", "incr", diff.Postgres.Label, "repo")
	newFull := backup("newfull", "20261017-010300F", "full", "", "repo")
	otherRepo := backup("otherrepo", "20261017-010400F", "full", "", "other")
	for _, tc := range []struct {
		name       string
		backups    []domain.Backup
		candidates []domain.Backup
		want       int
	}{
		{"only-full", []domain.Backup{full}, []domain.Backup{full}, 0},
		{"newest-full", []domain.Backup{full, newFull}, []domain.Backup{full, newFull}, 1},
		{"other-repository-not-newer", []domain.Backup{full, otherRepo}, []domain.Backup{full}, 0},
		{"retained-dependent", []domain.Backup{full, diff, incr, newFull}, []domain.Backup{full, diff}, 0},
		{"manual-dependent", []domain.Backup{full, diff, newFull}, []domain.Backup{full}, 0},
		{"whole-old-chain", []domain.Backup{full, diff, incr, newFull}, []domain.Backup{full, diff, incr}, 3},
		{"skipped-until-new-full", []domain.Backup{full, diff, incr}, []domain.Backup{full}, 0},
	} {
		for _, topology := range []string{"single", "ha"} {
			t.Run(topology+"/"+tc.name, func(t *testing.T) {
				backups := append([]domain.Backup(nil), tc.backups...)
				candidates := append([]domain.Backup(nil), tc.candidates...)
				for i := range backups {
					backups[i].SourceTopology.Type = topology
				}
				for i := range candidates {
					candidates[i].SourceTopology.Type = topology
				}
				got := p.FilterRetention(backups, candidates)
				if len(got) != tc.want {
					t.Fatalf("got %v candidates, want %d", got, tc.want)
				}
			})
		}
	}
}

func TestDeleteBackupFailureReleasesQueue(t *testing.T) {
	for _, mode := range []string{"newest-full", "rejected", "not-removed"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, _ := backupFixture(t, "single")
			label := "20261017-010000F"
			b := &domain.Backup{Namespace: "default", Cluster: "demo", Name: "old", Storage: "archive", Type: "full", Phase: domain.PhaseDeleting, Postgres: &domain.PostgresBackup{Label: label, Repository: "prefix/postgres/default/demo-incarnation"}}
			if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			calls := 0
			installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/postgres/status":
					json.NewEncoder(w).Encode(pgspec.StatusResponse{Role: "standalone", Running: true})
				case "/v1/postgres/backup-info":
					sets := []pgspec.BackupInfo{{Label: label, Type: "full"}}
					if mode != "newest-full" {
						sets = append(sets, pgspec.BackupInfo{Label: "20261017-010300F", Type: "full"})
					}
					json.NewEncoder(w).Encode(pgspec.BackupInfoResponse{Backups: sets})
				case "/v1/postgres/expire":
					calls++
					if mode == "rejected" {
						http.Error(w, "rejected", http.StatusConflict)
					} else {
						json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
					}
				default:
					t.Errorf("unexpected route %s", r.URL.Path)
				}
			})
			op := &domain.Operation{Kind: domain.OperationDeleteBackup, Payload: json.RawMessage(`{"backup":"old"}`)}
			result, err := p.runBackupOperation(t.Context(), rc, op)
			if mode == "not-removed" {
				if err != nil || result.Done {
					t.Fatal(result, err)
				}
				op.Checkpoint = result.Checkpoint
				result, err = p.runBackupOperation(t.Context(), rc, op)
			}
			if err == nil || !result.Done || !strings.Contains(err.Error(), "pgBackRest") {
				t.Fatal("failed expire did not terminate honestly", result, err)
			}
			op.Checkpoint = result.Checkpoint
			replay, replayErr := p.runBackupOperation(t.Context(), rc, op)
			if !replay.Done || replayErr == nil || replayErr.Error() != err.Error() {
				t.Fatal("failure checkpoint was not durable", replay, replayErr)
			}
			saved, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), b.Name)
			if err != nil || saved.Phase != domain.PhaseSucceeded {
				t.Fatal("rejected backup not available for retry", saved, err)
			}
			if mode == "newest-full" && calls != 0 {
				t.Fatal("attempted native expiration of newest full")
			}
		})
	}
}

func TestBackupNativeInventoryCorrectsSourceMajor(t *testing.T) {
	p, rc, _ := backupFixture(t, "single")
	rc.Cluster.Spec.Version = "18.6.1"
	b := &domain.Backup{
		Namespace: "default", Name: "native-version", Cluster: "demo",
		Storage: "archive", Type: "diff", Phase: domain.PhaseRunning,
		SourceVersion: "17.11.1", Postgres: &domain.PostgresBackup{Version: "17.11.1"},
	}
	if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/postgres/status":
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Role: "standalone", Running: true, Timeline: 1, Version: "17.11.1"})
		case "/v1/postgres/backup-info":
			json.NewEncoder(w).Encode(pgspec.BackupInfoResponse{Backups: []pgspec.BackupInfo{{
				Label: "native-label", Version: "16", StartedAt: "2026-10-17T02:18:59Z", StoppedAt: "2026-10-17T02:19:06Z",
			}}})
		default:
			t.Errorf("unexpected route %s", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	cp, _ := json.Marshal(backupCheckpoint{Step: "inventory", Member: "demop0", Timeline: 1, Label: "native-label"})
	op := &domain.Operation{Kind: domain.OperationBackup, Checkpoint: cp, Payload: json.RawMessage(`{"backup":"native-version"}`)}
	result, err := p.runBackupOperation(t.Context(), rc, op)
	if err != nil || !result.Done {
		t.Fatal(result, err)
	}
	b, err = rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), b.Name)
	if err != nil || b.SourceVersion != "16" || b.Postgres.Version != "16" {
		t.Fatalf("native source major was ignored: %+v %v", b, err)
	}
}
