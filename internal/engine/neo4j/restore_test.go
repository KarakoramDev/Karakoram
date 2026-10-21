// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"encoding/json"
	"net/http"
	"path"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func successfulDump(t *testing.T, rc *engine.Context, storage domain.BackupStorage, s3 *dumpStore) *domain.Backup {
	t.Helper()
	b := newDumpBackup(t, rc, storage, "snapshot")
	b.Phase = domain.PhaseSucceeded
	b.SourceVersion = defaultVersion
	if err := rc.Store.UpdateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(manifestFor(b.PBMPrefix))
	s3.mu.Lock()
	s3.objects[path.Join(b.PBMPrefix, "manifest.json")] = raw
	s3.mu.Unlock()
	return b
}
func createRestore(t *testing.T, rc *engine.Context, b *domain.Backup, name string) *domain.Operation {
	t.Helper()
	r := &domain.Restore{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Name: name, SourceNamespace: b.Namespace, Source: domain.RestoreSource{SourceCluster: b.Cluster, Backup: b.Name}, Phase: domain.PhasePending}
	if err := rc.Store.CreateRestore(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]string{"restore": name})
	return &domain.Operation{ID: name + "-op", Kind: domain.OperationRestore, Payload: payload}
}
func TestRestoreValidation(t *testing.T) {
	for _, test := range []string{"provider", "newer", "missing", "bad-json", "bad-object", "unknown-field", "oversize", "failed-backup"} {
		t.Run(test, func(t *testing.T) {
			p, rc, _, _ := setup(t)
			converge(t, p, rc)
			storage, s3 := setupDumpStore(t, rc)
			b := successfulDump(t, rc, storage, s3)
			m := manifestFor(b.PBMPrefix)
			switch test {
			case "provider":
				b.Provider = "postgres"
			case "newer":
				m.Neo4jVersion = "2027.01.0"
				b.SourceVersion = m.Neo4jVersion
			case "bad-object":
				m.Databases[0].Key = "other-cluster/system.dump"
			case "failed-backup":
				b.Phase = domain.PhaseFailed
			}
			raw, _ := json.Marshal(m)
			switch test {
			case "missing":
				raw = nil
			case "bad-json":
				raw = []byte(`{"format":`)
			case "unknown-field":
				raw = []byte(strings.TrimSuffix(string(raw), "}") + `,"password":"secret"}`)
			case "oversize":
				raw = []byte(strings.Repeat(" ", 65537))
			}
			key := path.Join(b.PBMPrefix, "manifest.json")
			if raw == nil {
				delete(s3.objects, key)
			} else {
				s3.objects[key] = raw
			}
			if err := rc.Store.UpdateBackup(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			op := createRestore(t, rc, b, "restore")
			if _, err := p.RunOperation(t.Context(), rc, op); err == nil {
				t.Fatal("invalid source accepted")
			}
			s, _ := load(rc.Cluster)
			if s.Maintenance != "" || s.RestoreRecovery != nil {
				t.Fatal("invalid restore entered maintenance")
			}
		})
	}
}
func TestRestoreHappyPathAndDurableRecovery(t *testing.T) {
	p, rc, _, _ := setup(t)
	converge(t, p, rc)
	storage, s3 := setupDumpStore(t, rc)
	b := successfulDump(t, rc, storage, s3)
	targetPassword := []byte("target-current-managed-password")
	if err := putSecret(t.Context(), rc, "users/neo4j", targetPassword); err != nil {
		t.Fatal(err)
	}
	calls := 0
	installJobAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		var req neo4jspec.RestoreRequest
		json.NewDecoder(r.Body).Decode(&req)
		if req.AdminPassword != string(targetPassword) || req.Receipt != "restore-op" || req.Source.Prefix != b.PBMPrefix || req.Manifest.Format != neo4jspec.ManifestFormat {
			t.Error("wrong target credentials or source")
		}
		calls++
		state := neo4jspec.JobRunning
		if calls > 1 {
			state = neo4jspec.JobDone
		}
		json.NewEncoder(w).Encode(neo4jspec.JobResponse{State: state})
	})
	op := createRestore(t, rc, b, "restore")
	result, err := p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = result.Checkpoint
	s, _ := load(rc.Cluster)
	if s.Maintenance != op.ID || s.RestoreRecovery == nil {
		t.Fatal("restore not durably fenced")
	}
	// The cluster copy also recovers a lost queue checkpoint.
	op.Checkpoint = nil
	runDumpOperation(t, p, rc, op)
	s, _ = load(rc.Cluster)
	if s.Maintenance != "" || s.RestoreRecovery != nil {
		t.Fatal("maintenance not released")
	}
	got, err := rc.Store.GetRestore(t.Context(), rc.Cluster.Ref(), "restore")
	if err != nil || got.Phase != domain.PhaseSucceeded || got.CompletedAt == nil {
		t.Fatal(got, err)
	}
	if calls != 2 {
		t.Fatal(calls)
	}
}
func TestCloneUsesTargetCredentialsAndReadOnlySource(t *testing.T) {
	p, rc, _, _ := setup(t)
	converge(t, p, rc)
	storage, s3 := setupDumpStore(t, rc)
	b := successfulDump(t, rc, storage, s3)
	sourceCluster := rc.Cluster
	target := &domain.Cluster{Name: "clone", Namespace: sourceCluster.Namespace, Spec: sourceCluster.Spec}
	target.Spec.DataSource = &domain.DataSource{Cluster: sourceCluster.Name, Backup: b.Name}
	if err := rc.Store.(*store.Store).CreateCluster(t.Context(), target); err != nil {
		t.Fatal(err)
	}
	rc.Cluster = target
	s := &state{UID: "target-incarnation", ConfigVersion: 1, Member: &member{Name: "clonen0", Stage: "ready", Version: defaultVersion}}
	if err := save(target, s); err != nil {
		t.Fatal(err)
	}
	if err := putSecret(t.Context(), rc, "users/neo4j", []byte("clone-managed-password")); err != nil {
		t.Fatal(err)
	}
	if err := p.queueClone(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if err := save(target, s); err != nil {
		t.Fatal(err)
	}
	if err := p.queueClone(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	installJobAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		var req neo4jspec.RestoreRequest
		json.NewDecoder(r.Body).Decode(&req)
		if req.AdminPassword != "clone-managed-password" || req.Source.Prefix != b.PBMPrefix {
			t.Error("clone did not use target credentials and source prefix")
		}
		json.NewEncoder(w).Encode(neo4jspec.JobResponse{State: neo4jspec.JobDone})
	})
	op, err := rc.Store.GetOperation(t.Context(), target.Ref().Key()+"-"+s.UID+"-initial-restore")
	if err != nil {
		t.Fatal(err)
	}
	runDumpOperation(t, p, rc, op)
	s, _ = load(target)
	if !s.CloneComplete || !s.CloneQueued {
		t.Fatal("clone not complete")
	}
	s3.mu.Lock()
	defer s3.mu.Unlock()
	if len(s3.deleted) != 0 || len(s3.objects) != 1 {
		t.Fatal("clone modified source storage")
	}
	if backupPrefix(storage, target, s, "new") == b.PBMPrefix {
		t.Fatal("clone shares source destination")
	}
}
func TestManifestValidation(t *testing.T) {
	m := manifestFor("production/neo4j/default/source-uid/snapshot")
	if err := validateManifest(m, "production/neo4j/default/source-uid/snapshot", defaultVersion); err != nil {
		t.Fatal(err)
	}
	m.Databases[1] = m.Databases[0]
	if err := validateManifest(m, "production/neo4j/default/source-uid/snapshot", defaultVersion); err == nil {
		t.Fatal("duplicate database accepted")
	}
}
