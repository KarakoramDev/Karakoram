// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func backupFixture(t *testing.T, topology string) (*Core, *engine.Context, *fakeBackend) {
	t.Helper()
	p, rc, b := setup(t, topology)
	converge(t, p, rc, b)
	for name, status := range b.native {
		status.PhysicalIdentity = "physical-" + name
		b.native[name] = status
	}
	storage := &domain.BackupStorage{Namespace: "default", Name: "archive", Type: "s3", EndpointURL: "https://objects.example", Bucket: "backups", Region: "region", Prefix: "prefix", ForcePathStyle: true, VerifyTLS: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
	if err := rc.Store.(*store.Store).CreateBackupStorage(t.Context(), storage); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Backup.Storage = storage.Name
	return p, rc, b
}
func inventoryBackup(t *testing.T, rc *engine.Context, name, kind, parent string, lsn uint64, stamp time.Time) domain.Backup {
	t.Helper()
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	root := name
	if parent != "" {
		root = "base"
	}
	prefix := "prefix/" + rc.Cluster.Spec.Provider + "/default/demo-" + s.UID
	b := domain.Backup{Namespace: "default", Cluster: "demo", Name: name, Provider: rc.Cluster.Spec.Provider, Storage: "archive", Type: kind, SourceVersion: rc.Cluster.Spec.Version, Phase: domain.PhaseSucceeded, CompletedAt: &stamp, RecoveryIdentity: "fixture-" + name, MySQL: &domain.MySQLBackup{Label: name, Repository: prefix, Type: kind, ParentLabel: parent, ChainRootLabel: root, ClusterUID: s.UID, ServerVersion: rc.Cluster.Spec.Version, ToolVersion: "8.4.0-7", GTIDSet: "11111111-2222-3333-4444-000000000001:1-10", ToLSN: lsn, EndTime: stamp, ObjectPaths: []string{prefix + "/base/" + name}}}
	b.MySQL.PhysicalIdentity = "fixture-source"
	if parent != "" {
		b.MySQL.FromLSN = lsn - 10
	}
	if err = rc.Store.CreateBackup(t.Context(), &b); err != nil {
		t.Fatal(err)
	}
	if err := captureFixtureBackupAccess(t.Context(), rc, &b); err != nil {
		t.Fatal(err)
	}
	return b
}
func TestBackupSecondaryReceiptAndIncrementalChain(t *testing.T) {
	p, rc, b := backupFixture(t, "group")
	calls := 0
	var dispatched myspec.BackupRequest
	member := ""
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		switch r.URL.Path {
		case "/v1/mysql/backup":
			calls++
			member = name
			if err := json.NewDecoder(r.Body).Decode(&dispatched); err != nil {
				t.Error(err)
			}
			json.NewEncoder(w).Encode(myspec.BackupResponse{Receipt: dispatched.Receipt, State: "running"})
			return true
		case "/v1/mysql/backup-info":
			json.NewEncoder(w).Encode(myspec.BackupResponse{Receipt: dispatched.Receipt, State: "done", Result: &myspec.BackupResult{ManifestDigest: fixtureManifestDigest, ManifestSize: 1, BackupType: dispatched.Type, ServerVersion: b.native[name].Version, PhysicalIdentity: dispatched.PhysicalIdentity, ToolVersion: "8.4.0-7", FromLSN: dispatched.BaseLSN, ToLSN: dispatched.BaseLSN + 10, EndTime: time.Now().UTC(), ObjectPrefix: dispatched.ObjectPrefix}})
			return true
		}
		return false
	}
	for _, kind := range []string{"full", "incremental"} {
		name := kind
		item := &domain.Backup{Namespace: "default", Cluster: "demo", Name: name, Storage: "archive", Type: kind, Phase: domain.PhasePending}
		if err := rc.Store.CreateBackup(t.Context(), item); err != nil {
			t.Fatal(err)
		}
		payload, _ := json.Marshal(map[string]string{"backup": name})
		op := &domain.Operation{ID: "receipt-" + name, Kind: domain.OperationBackup, Payload: payload}
		for range 8 {
			result, err := p.RunOperation(t.Context(), rc, op)
			if err != nil {
				t.Fatal(err)
			}
			op.Checkpoint = result.Checkpoint
			if result.Done {
				break
			}
		}
		saved, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), name)
		if err != nil || saved.Phase != domain.PhaseSucceeded {
			t.Fatalf("backup not inventoried: %+v %v", saved, err)
		}
		if b.native[member].MemberRole != "SECONDARY" {
			t.Fatalf("backup dispatched to %s, not secondary", member)
		}
		if dispatched.Receipt != op.ID {
			t.Fatal("operation ID not used for receipt")
		}
		before := calls
		if result, err := p.RunOperation(t.Context(), rc, op); err != nil || !result.Done || calls != before {
			t.Fatal("completed receipt redispatched", result, err)
		}
		if kind == "incremental" {
			if dispatched.BaseLSN != 10 || saved.MySQL.ParentLabel == "" || saved.MySQL.ChainRootLabel == saved.MySQL.Label {
				t.Fatalf("incremental chain lost: %+v request %+v", saved.MySQL, dispatched)
			}
		}
	}
	if calls != 2 {
		t.Fatalf("backup dispatched %d times", calls)
	}
}
func TestIncrementalRequiresParent(t *testing.T) {
	p, rc, _ := backupFixture(t, "single")
	b := &domain.Backup{Namespace: "default", Cluster: "demo", Name: "orphan", Storage: "archive", Type: "incremental", Phase: domain.PhasePending}
	if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	_, err := p.RunOperation(t.Context(), rc, &domain.Operation{ID: "orphan", Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"orphan"}`)})
	if err == nil {
		t.Fatal("orphan incremental accepted")
	}
}
func TestRetentionPreservesIncrementalParents(t *testing.T) {
	p, rc, _ := backupFixture(t, "single")
	base := inventoryBackup(t, rc, "base", "full", "", 10, time.Unix(10, 0))
	inc := inventoryBackup(t, rc, "inc", "incremental", "base", 20, time.Unix(20, 0))
	leaf := inventoryBackup(t, rc, "leaf", "incremental", "inc", 30, time.Unix(30, 0))
	all := []domain.Backup{base, inc, leaf}
	if got := p.FilterRetention(all, []domain.Backup{base, inc}); len(got) != 0 {
		t.Fatalf("removed retained dependency: %+v", got)
	}
	if got := p.FilterRetention(all, all); len(got) != 3 {
		t.Fatalf("complete expired closure protected: %+v", got)
	}
	chain, err := restoreChain(all, leaf)
	if err != nil || len(chain) != 3 || chain[0] != base.MySQL.ObjectPaths[0] || chain[2] != leaf.MySQL.ObjectPaths[0] {
		t.Fatal("restore chain order", chain, err)
	}
}
func TestPITRGapConservativeWindow(t *testing.T) {
	p, rc, b := backupFixture(t, "single")
	rc.Cluster.Spec.Backup.PITR.Enabled = true
	inventoryBackup(t, rc, "base", "full", "", 10, time.Unix(10, 0))
	archive := myspec.BinlogArchiveStatus{GTIDSet: "11111111-2222-3333-4444-000000000001:11-30", LastEventTime: time.Unix(30, 0), Gap: true, ContiguousEndTime: time.Unix(20, 0), ContiguousGTIDSet: "11111111-2222-3333-4444-000000000001:11-20", GapReason: "purged"}
	coverage := myspec.PITRCoverageResponse{Windows: []myspec.PITRWindow{{Start: time.Unix(10, 0), End: time.Unix(20, 0)}}}
	b.action = func(w http.ResponseWriter, r *http.Request, _ string) bool {
		switch r.URL.Path {
		case "/v1/mysql/archive-status":
			json.NewEncoder(w).Encode(archive)
		case "/v1/mysql/pitr-coverage":
			var request myspec.PITRCoverageRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.ObjectPaths) != 1 || request.Source.Path == "" || len(request.ObjectManifests) != 1 || request.ObjectManifests[0].Digest != fixtureManifestDigest {
				t.Error("coverage omitted immutable snapshot provenance")
			}
			json.NewEncoder(w).Encode(coverage)
		default:
			return false
		}
		return true
	}
	windows, err := p.PITRWindow(t.Context(), rc)
	if err != nil || len(windows) != 1 || !windows[0].Start.Equal(time.Unix(10, 0)) || !windows[0].End.Equal(time.Unix(20, 0)) {
		t.Fatal(windows, err)
	}
	if _, err = p.ValidatePITRRestore(t.Context(), rc, time.Unix(25, 0)); err == nil {
		t.Fatal("target beyond gap accepted")
	}
	// An unchanged raw numeric union cannot override an unprovable lineage.
	coverage.Windows = nil
	windows, err = p.PITRWindow(t.Context(), rc)
	if err != nil || len(windows) != 0 {
		t.Fatal("GTID hole accepted", windows, err)
	}
}

const fixtureManifestDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func captureFixtureBackupAccess(ctx context.Context, rc *engine.Context, backup *domain.Backup) error {
	if err := captureBackupAccess(ctx, rc, backup); err != nil {
		return err
	}
	return sealBackupManifest(ctx, rc, backup, fixtureManifestDigest, 1)
}
