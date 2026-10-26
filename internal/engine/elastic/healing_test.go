// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
)

func TestRestoreSubmissionRequiresDurableReceipt(t *testing.T) {
	for _, mode := range []string{"client-index", "accepted-replay", "lost-response", "feature-only-lost-response", "receipt-write-failure", "accepted-receipt-write-failure", "legacy-submit"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			p, rc, s, st := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/_restore"):
					calls++
					if strings.Contains(mode, "lost-response") {
						http.Error(w, "storage unavailable", http.StatusServiceUnavailable)
						return
					}
					if mode == "client-index" {
						http.Error(w, "index already exists", http.StatusBadRequest)
						return
					}
					opsJSON(w, map[string]any{"snapshot": map[string]any{"shards": map[string]int{"total": 2, "successful": 2, "failed": 0}}})
				case r.URL.Path == "/_cat/indices":
					rows := []map[string]string{}
					if mode == "client-index" || calls > 0 && mode == "accepted-replay" {
						rows = append(rows, map[string]string{"index": "a"})
					}
					opsJSON(w, rows)
				case strings.HasSuffix(r.URL.Path, "/snapshot-one"):
					indices := []string{"a", "b"}
					if mode == "feature-only-lost-response" {
						indices = nil
					}
					opsJSON(w, map[string]any{"snapshots": []esapi.SnapshotInfo{{State: "SUCCESS", Indices: indices}}})
				default:
					opsJSON(w, map[string]any{"acknowledged": true, "nodes": map[string]any{}})
				}
			})
			st.backup.Phase, st.backup.Provider, st.backup.SourceVersion = domain.PhaseSucceeded, "elasticsearch", "9.5.5"
			st.backup.Location = snapshotLocation(&st.storage, clusterBasePath(&st.storage, rc.Cluster, s), "kk-archive", st.backup.Name)
			st.restore = domain.Restore{Name: "restore-one", Source: domain.RestoreSource{Backup: st.backup.Name}, Phase: domain.PhaseRunning}
			s.RestoreActive = true
			cp := operationState{Stage: "submit-receipted"}
			op := &domain.Operation{ID: "restore-receipt", Kind: domain.OperationRestore}
			if mode == "receipt-write-failure" {
				rc.Secrets = failingSubmissionSecrets{rc.Secrets}
			} else if mode == "accepted-receipt-write-failure" {
				rc.Secrets = &failAcceptedSubmissionSecrets{SecretStore: rc.Secrets}
			} else if mode == "legacy-submit" {
				cp.Stage = "submit"
			}
			result, err := p.runRestore(t.Context(), rc, s, op, cp, st.restore.Name)
			if mode == "accepted-replay" {
				if err != nil || opsCheckpoint(t, result).Stage != "recover" {
					t.Fatalf("submission: %+v %v", result, err)
				}
				result, err = p.runRestore(t.Context(), rc, s, op, cp, st.restore.Name)
				if err != nil || calls != 1 || opsCheckpoint(t, result).Stage != "recover" {
					t.Fatalf("accepted replay: calls=%d result=%+v err=%v", calls, result, err)
				}
				return
			}
			if mode == "legacy-submit" {
				if err == nil || calls != 0 || result.Done || !strings.Contains(err.Error(), "reconcile") {
					t.Fatalf("legacy receipt replayed: calls=%d result=%+v err=%v", calls, result, err)
				}
				return
			}
			wantDone := mode != "receipt-write-failure" && mode != "accepted-receipt-write-failure"
			if err == nil || result.Done != wantDone || opsCheckpoint(t, result).Stage != "submit-receipted" || !s.RestoreActive || st.restore.Phase == domain.PhaseSucceeded {
				t.Fatalf("unsafe submission outcome: %+v %v restore=%+v calls=%d", result, err, st.restore, calls)
			}
			if mode == "receipt-write-failure" {
				if calls != 0 {
					t.Fatal("restore submitted without durable intent")
				}
				return
			}
			before := calls
			result, err = p.runRestore(t.Context(), rc, s, op, cp, st.restore.Name)
			if err == nil || calls != before || !result.Done || !strings.Contains(err.Error(), "reconcile") {
				t.Fatalf("ambiguous submission replayed: calls=%d result=%+v err=%v", calls, result, err)
			}
		})
	}
}

func TestRestoreSubmissionNativeErrorIsDurableUnknownOutcome(t *testing.T) {
	for _, tc := range []struct {
		name, kind string
		status     int
	}{
		{"open-index", "snapshot_restore_exception", 500},
		{"invalid-request", "illegal_argument_exception", 400},
		{"missing-snapshot", "snapshot_missing_exception", 404},
		{"untyped-500", "", 500},
		{"other-500", "internal_server_error", 500},
		{"gateway", "snapshot_restore_exception", 502},
		{"unavailable", "snapshot_restore_exception", 503},
		{"gateway-timeout", "snapshot_restore_exception", 504},
		{"request-timeout", "illegal_argument_exception", 408},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			p, rc, s, st := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/_restore") {
					calls++
					w.WriteHeader(tc.status)
					opsJSON(w, map[string]any{"error": map[string]string{
						"type": tc.kind, "reason": "[repo:snapshot] cannot restore index [client-created] because an open index with same name already exists in the cluster",
					}})
					return
				}
				opsJSON(w, map[string]any{"acknowledged": true, "nodes": map[string]any{}})
			})
			st.backup.Phase, st.backup.Provider, st.backup.SourceVersion = domain.PhaseSucceeded, "elasticsearch", "9.5.5"
			st.backup.Location = snapshotLocation(&st.storage, clusterBasePath(&st.storage, rc.Cluster, s), "kk-archive", st.backup.Name)
			st.restore = domain.Restore{Name: "restore-one", Source: domain.RestoreSource{Backup: st.backup.Name}, Phase: domain.PhaseRunning}
			s.RestoreActive, s.Phase = true, "restore"
			cp := operationState{Stage: "submit-receipted"}
			op := &domain.Operation{ID: "unknown-restore", Kind: domain.OperationRestore}
			result, err := p.runRestore(t.Context(), rc, s, op, cp, st.restore.Name)
			if err == nil || !errors.Is(err, engine.ErrInvalid) || !result.Done || calls != 1 {
				t.Fatalf("first attempt: result=%+v err=%v posts=%d", result, err, calls)
			}
			if !s.RestoreActive || s.Phase != "restore" || st.restore.Phase == domain.PhaseSucceeded {
				t.Fatalf("restore hold released: state=%+v restore=%+v", s, st.restore)
			}
			if st.restore.Phase != domain.PhaseFailed || st.restore.CompletedAt == nil ||
				!strings.Contains(err.Error(), "client-created") || !strings.Contains(err.Error(), "keepVolumes: true") {
				t.Fatalf("missing terminal diagnostic: restore=%+v err=%v", st.restore, err)
			}
			first := err.Error()
			raw, e := secret(t.Context(), rc, "operations/"+op.ID+"/restore-submission")
			if e != nil {
				t.Fatal(e)
			}
			var source struct {
				Repository string `json:"repository"`
				Snapshot   string `json:"snapshot"`
			}
			if e = json.Unmarshal(raw, &source); e != nil {
				t.Fatal(e)
			}
			// Exercise the receipt itself, rather than the already-failed Restore
			// guard: a restarted operation with only durable submission evidence.
			client, e := p.es(t.Context(), rc, s)
			if e != nil {
				t.Fatal(e)
			}
			defer client.Close()
			for range 3 {
				err = submitRestoreOnce(t.Context(), rc, client, op.ID, source.Repository, source.Snapshot)
				if err == nil || calls != 1 || !errors.Is(err, engine.ErrInvalid) {
					t.Fatalf("replay: posts=%d err=%v", calls, err)
				}
				if err.Error() != first {
					t.Fatalf("native evidence changed: first=%q replay=%q", first, err)
				}
				if !strings.Contains(err.Error(), "outcome is unknown") {
					t.Fatalf("ambiguous outcome lost: %v", err)
				}
			}
			raw, e = secret(t.Context(), rc, "operations/"+op.ID+"/restore-submission")
			if e != nil {
				t.Fatal(e)
			}
			var receipt struct {
				Status      string `json:"status"`
				Completed   bool   `json:"completed"`
				NativeError string `json:"nativeError"`
			}
			if e = json.Unmarshal(raw, &receipt); e != nil || receipt.Completed || receipt.Status != "" || !strings.Contains(receipt.NativeError, "client-created") {
				t.Fatalf("receipt=%s err=%v", raw, e)
			}
		})
	}
}

func TestRestoreSubmissionIntentOnlyIsTerminal(t *testing.T) {
	calls := 0
	p, rc, s, _ := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		opsJSON(w, map[string]any{})
	})
	key := "operations/intent-only/restore-submission"
	if err := putSecret(t.Context(), rc, key, []byte(`{"repository":"repo","snapshot":"snap","completed":false}`)); err != nil {
		t.Fatal(err)
	}
	client, err := p.es(t.Context(), rc, s)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	original := rc.Secrets
	rc.Secrets = failingSubmissionReadSecrets{original}
	err = submitRestoreOnce(t.Context(), rc, client, "intent-only", "repo", "snap")
	if err == nil || errors.Is(err, engine.ErrInvalid) || calls != 0 {
		t.Fatalf("transient receipt read terminalized: calls=%d err=%v", calls, err)
	}
	rc.Secrets = original
	for range 3 {
		err = submitRestoreOnce(t.Context(), rc, client, "intent-only", "repo", "snap")
		if !errors.Is(err, engine.ErrInvalid) || !strings.Contains(err.Error(), "outcome is unknown") || calls != 0 {
			t.Fatalf("intent-only replay: calls=%d err=%v", calls, err)
		}
	}
}

type failingSubmissionReadSecrets struct{ engine.SecretStore }

func (s failingSubmissionReadSecrets) GetClusterSecret(ctx context.Context, ref domain.ClusterRef, key string) ([]byte, error) {
	if strings.HasSuffix(key, "/restore-submission") {
		return nil, errors.New("receipt read unavailable")
	}
	return s.SecretStore.GetClusterSecret(ctx, ref, key)
}

type failingSubmissionSecrets struct{ engine.SecretStore }

func (s failingSubmissionSecrets) SetClusterSecret(ctx context.Context, ref domain.ClusterRef, key string, value []byte) error {
	if strings.HasSuffix(key, "/restore-submission") {
		return errors.New("receipt persistence unavailable")
	}
	return s.SecretStore.SetClusterSecret(ctx, ref, key, value)
}

type failAcceptedSubmissionSecrets struct {
	engine.SecretStore
	writes int
}

func (s *failAcceptedSubmissionSecrets) SetClusterSecret(ctx context.Context, ref domain.ClusterRef, key string, value []byte) error {
	if strings.HasSuffix(key, "/restore-submission") {
		s.writes++
		if s.writes == 2 {
			return errors.New("successful receipt persistence unavailable")
		}
	}
	return s.SecretStore.SetClusterSecret(ctx, ref, key, value)
}

func TestNodeRejoinConsumesCorrectedConfiguration(t *testing.T) {
	for _, reason := range []string{"config", "upgrade"} {
		t.Run(reason, func(t *testing.T) {
			p, rc, f, _ := coreSetup(t, 3)
			s := coreConverge(t, p, rc, f)
			m := s.find("demon0")
			rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"elasticsearch":{"settings":{"review.invalid_setting":true}}}`)
			if err := p.publishConfig(t.Context(), rc, s, m, rc.Cluster.Spec.Version, ""); err != nil {
				t.Fatal(err)
			}
			badHash := m.ConfigHash
			s.Rolling = &rolling{Reason: reason, Version: rc.Cluster.Spec.Version, Order: []string{m.Name, "demon1"}, Current: m.Name, Step: "rejoin"}
			delete(f.nodes, m.Name)
			otherHash := s.find("demon1").ConfigHash
			rc.Cluster.Spec.EngineOptions = nil
			if done, _, err := p.rollingStep(t.Context(), rc, s); err != nil || done {
				t.Fatalf("repair: done=%v err=%v", done, err)
			}
			if m.ConfigHash == badHash || s.Rolling.Current != m.Name || s.Rolling.Step != "rejoin" || s.find("demon1").ConfigHash != otherHash {
				t.Fatal("corrected config not published only to failed member", s.Rolling)
			}
			if m.Version != rc.Cluster.Spec.Version {
				t.Fatal("repair changed upgrade target")
			}
		})
	}
}

func TestSnapshotLocationSurvivesPrefixEdit(t *testing.T) {
	storage := &domain.BackupStorage{Name: "archive", Bucket: "backups", Prefix: "old"}
	b := &domain.Backup{Storage: storage.Name, Provider: "elasticsearch", Location: snapshotLocation(storage, "old/default/test/identity", "kk-archive", "snapshot")}
	storage.Prefix = "new"
	base, _, _, err := recordedSnapshot(storage, b)
	if err != nil || base != "old/default/test/identity" {
		t.Fatalf("immutable location invalidated: base=%q err=%v", base, err)
	}
	storage.Bucket = "different"
	if _, _, _, err := recordedSnapshot(storage, b); err == nil {
		t.Fatal("bucket identity mismatch accepted")
	}
}

func TestManagedSecurityRepairsUnchangedDesiredSecrets(t *testing.T) {
	writes := map[string]int{}
	p, rc, s, _ := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" && strings.HasPrefix(r.URL.Path, "/_security/") {
			writes[r.URL.Path]++
		}
		if r.URL.Path == "/_security/_authenticate" {
			opsJSON(w, map[string]string{"username": "elastic/kibana"})
			return
		}
		opsJSON(w, map[string]any{})
	})
	if err := p.ensureSecurity(t.Context(), rc, s, false); err != nil {
		t.Fatal(err)
	}
	clear(writes)
	if err := p.ensureSecurity(t.Context(), rc, s, false); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/_security/role/" + LogstashWriterRole, "/_security/role/" + APMWriterRole, "/_security/user/" + LogstashUser, "/_security/user/" + APMUser} {
		if writes[path] != 1 {
			t.Errorf("managed object not reconciled: %s writes=%d", path, writes[path])
		}
	}
}

func TestWriteBlocksDegradeGreenClusterAndClearNatively(t *testing.T) {
	p, rc, f, es := coreSetup(t, 1)
	coreConverge(t, p, rc, f)
	for _, key := range []string{"index.blocks.read_only_allow_delete", "index.blocks.read_only", "index.blocks.write"} {
		es.writeBlocks = map[string]string{key: "true"}
		status, err := p.Status(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, condition := range status.Conditions {
			if condition.Type == "IndexWriteBlocked" && condition.Status == "True" && strings.Contains(condition.Message, "logs") && strings.Contains(condition.Message, key) {
				found = true
			}
		}
		if status.Phase != domain.PhaseDegraded || !found {
			t.Fatalf("write block hidden by green shards: %+v", status)
		}
	}
	es.writeBlocks = map[string]string{"index.blocks.write": "invalid"}
	unknown, err := p.Status(t.Context(), rc)
	if err != nil || unknown.Phase != domain.PhaseDegraded {
		t.Fatalf("failed block observation reported Ready: %+v %v", unknown, err)
	}
	foundUnknown := false
	for _, condition := range unknown.Conditions {
		foundUnknown = foundUnknown || condition.Type == "IndexWriteBlocked" && condition.Status == "Unknown"
	}
	if !foundUnknown {
		t.Fatal("failed block observation did not report Unknown")
	}
	es.writeBlocks = nil
	status, err := p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseReady {
		t.Fatalf("native block release did not restore readiness: %+v %v", status, err)
	}
}

func TestHistoricalSnapshotOperationsKeepRecordedBasePath(t *testing.T) {
	for _, action := range []string{"restore", "delete", "backup-poll"} {
		t.Run(action, func(t *testing.T) {
			var bases []string
			p, rc, s, st := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "PUT" && strings.HasPrefix(r.URL.Path, "/_snapshot/") {
					var repository esapi.Repository
					if err := json.NewDecoder(r.Body).Decode(&repository); err != nil {
						t.Fatal(err)
					}
					bases = append(bases, repository.Settings["base_path"].(string))
				}
				if strings.HasSuffix(r.URL.Path, "/snapshot-one") && r.Method == "GET" {
					opsJSON(w, map[string]any{"snapshots": []esapi.SnapshotInfo{{State: "IN_PROGRESS"}}})
					return
				}
				opsJSON(w, map[string]any{"acknowledged": true, "nodes": map[string]any{}})
			})
			oldBase := clusterBasePath(&st.storage, rc.Cluster, s)
			st.backup.Provider, st.backup.SourceVersion = "elasticsearch", rc.Cluster.Spec.Version
			st.backup.Location = snapshotLocation(&st.storage, oldBase, RepositoryName(st.storage.Name), st.backup.Name)
			st.storage.Prefix = "changed"
			st.restore = domain.Restore{Name: "restore", Source: domain.RestoreSource{Backup: st.backup.Name}}
			var err error
			switch action {
			case "restore":
				_, err = p.runRestore(t.Context(), rc, s, &domain.Operation{ID: "historical-restore"}, operationState{Stage: "destructive"}, st.restore.Name)
			case "delete":
				_, err = p.runDeleteBackup(t.Context(), rc, s, &domain.Operation{ID: "historical-delete"}, operationState{Stage: "delete"}, st.backup.Name)
			default:
				_, err = p.runBackup(t.Context(), rc, s, &domain.Operation{ID: "historical-backup"}, operationState{Stage: "poll"}, st.backup.Name)
			}
			if err != nil || len(bases) != 1 || bases[0] != oldBase {
				t.Fatalf("recorded base not used: %v err=%v", bases, err)
			}
		})
	}
}
