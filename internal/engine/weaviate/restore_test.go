// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvapi"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func restoreStep(t *testing.T, f *coreFixture, s *state, cp operationState) (operationState, engine.OpResult, error) {
	t.Helper()
	r, e := f.p.runRestore(context.Background(), f.rc, s, &domain.Operation{ID: "restore-op"}, cp, "recover")
	if json.Unmarshal(r.Checkpoint, &cp) != nil {
		t.Fatal("invalid checkpoint")
	}
	return cp, r, e
}

func bindRestoreStorage(t *testing.T, f *coreFixture, b *domain.Backup, n *backupNative) {
	t.Helper()
	key := "/" + b.Weaviate.Bucket + "/" + b.Weaviate.Path + "/" + b.Weaviate.ID + "/restore_config.json"
	endpoint, _, _ := fakeBackupS3(t, func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != key {
			t.Errorf("unexpected restore metadata key %s", r.URL.Path)
			http.NotFound(w, r)
			return true
		}
		if n.restoreMetadata == nil {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("<Error><Code>NoSuchKey</Code></Error>"))
			return true
		}
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		_ = json.NewEncoder(w).Encode(n.restoreMetadata)
		return true
	})
	storage, err := f.rc.Store.GetBackupStorage(context.Background(), b.Namespace, b.Storage)
	if err != nil {
		t.Fatal(err)
	}
	storage.EndpointURL = endpoint
	storage.ForcePathStyle = true
	storage.VerifyTLS = false
	if err = f.rc.Store.(*store.Store).UpdateBackupStorage(context.Background(), storage); err != nil {
		t.Fatal(err)
	}
	b.Weaviate.Endpoint = endpoint
	b.Location = backupLocation(b.Weaviate)
	if err = f.rc.Store.UpdateBackup(context.Background(), b); err != nil {
		t.Fatal(err)
	}
}
func TestRestoreOrderMappingAndResume(t *testing.T) {
	for _, clone := range []bool{false, true} {
		t.Run(map[bool]string{true: "clone", false: "in-place"}[clone], func(t *testing.T) {
			f, s, n, b := backupFixture(t, 3)
			b = completeBackup(t, f, s, n, b)
			if clone {
				b.Weaviate.Members = []string{"source-0", "source-1", "source-2"}
				b.Weaviate.ClusterUID = "source-uid"
				b.Weaviate.Path = "old-prefix/default/demo/source-uid"
				b.Location = backupLocation(b.Weaviate)
				if e := f.rc.Store.UpdateBackup(context.Background(), b); e != nil {
					t.Fatal(e)
				}
			}
			bindRestoreStorage(t, f, b, n)
			r := &domain.Restore{Name: "recover", Namespace: f.rc.Cluster.Ref().Namespace, Cluster: f.rc.Cluster.Name, Source: domain.RestoreSource{Backup: b.Name}, Phase: domain.PhasePending, CreatedAt: time.Now()}
			if e := f.rc.Store.CreateRestore(context.Background(), r); e != nil {
				t.Fatal(e)
			}
			n.collections = []string{"Article", "Book"}
			n.order = nil
			cp := operationState{}
			for range 8 {
				var e error
				cp, _, e = restoreStep(t, f, s, cp)
				if e != nil {
					t.Fatal(e)
				}
				if cp.Stage == "submit" {
					break
				}
			}
			if cp.Stage != "submit" || n.restores != 0 || !reflect.DeepEqual(n.order, []string{"collections", "delete", "collections", "delete", "collections"}) {
				t.Fatalf("destructive order %+v cp %+v", n.order, cp)
			}
			submit := cp
			cp, _, e := restoreStep(t, f, s, cp)
			if e != nil {
				t.Fatal(e)
			}
			// Crash after POST but before recording acceptance or the operation
			// checkpoint: native status must resolve the durable intent.
			raw, e := secret(context.Background(), f.rc, "operations/restore-op/restore-submission")
			if e != nil {
				t.Fatal(e)
			}
			var receipt restoreReceipt
			if e = json.Unmarshal(raw, &receipt); e != nil {
				t.Fatal(e)
			}
			receipt.Accepted = false
			raw, _ = json.Marshal(receipt)
			if e = putSecret(context.Background(), f.rc, "operations/restore-op/restore-submission", raw); e != nil {
				t.Fatal(e)
			}
			cp, _, e = restoreStep(t, f, s, submit)
			if e != nil {
				t.Fatal(e)
			}
			if n.restores != 1 {
				t.Fatalf("restore POST count %d", n.restores)
			}
			expected := wvapi.RestoreAll
			if clone {
				expected = wvapi.RestoreNone
			}
			if n.restoreRequest.Config.UsersOptions != expected || n.restoreRequest.Config.RolesOptions != expected || n.restoreRequest.Config.Path != b.Weaviate.Path {
				t.Fatalf("request %+v", n.restoreRequest)
			}
			if clone {
				want := map[string]string{"source-0": NodeHostname("demo", 0), "source-1": NodeHostname("demo", 1), "source-2": NodeHostname("demo", 2)}
				if !reflect.DeepEqual(n.restoreRequest.NodeMapping, want) {
					t.Fatalf("mapping %+v", n.restoreRequest.NodeMapping)
				}
			} else if len(n.restoreRequest.NodeMapping) != 0 {
				t.Fatal("identity mapping not omitted")
			}
			n.restore = wvapi.BackupSuccess
			cp, _, e = restoreStep(t, f, s, cp)
			if e != nil || cp.Stage != "health" {
				t.Fatalf("health checkpoint %+v %v", cp, e)
			}
			f.quorum = false
			_, out, e := restoreStep(t, f, s, cp)
			if e != nil || out.Done {
				t.Fatalf("unhealthy completed %+v %v", out, e)
			}
			f.quorum = true
			_, out, e = restoreStep(t, f, s, cp)
			if e != nil || !out.Done {
				t.Fatalf("healthy completion %+v %v", out, e)
			}
			saved, _ := f.rc.Store.GetRestore(context.Background(), f.rc.Cluster.Ref(), r.Name)
			if saved.Phase != domain.PhaseSucceeded || saved.CompletedAt == nil {
				t.Fatalf("restore metadata %+v", saved)
			}
		})
	}
}
func TestRestoreMemberCountAndStorageAdmission(t *testing.T) {
	f, s, n, b := backupFixture(t, 1)
	b = completeBackup(t, f, s, n, b)
	b.Weaviate.Members = append(b.Weaviate.Members, "extra")
	if _, e := f.p.restoreAdmission(context.Background(), f.rc.Store, f.rc.Cluster, b); e == nil {
		t.Fatal("accepted member mismatch")
	}
	b.Weaviate.Members = b.Weaviate.Members[:1]
	b.Weaviate.Endpoint = "https://other.example.com"
	if _, e := f.p.restoreAdmission(context.Background(), f.rc.Store, f.rc.Cluster, b); e == nil {
		t.Fatal("accepted changed endpoint")
	}
}
func TestInitialRestoreAtomicAdoptionAndDataSourceDefaults(t *testing.T) {
	f, s, n, b := backupFixture(t, 1)
	b = completeBackup(t, f, s, n, b)
	f.rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: f.rc.Cluster.Name, Backup: b.Name}
	f.rc.Cluster.Spec.Backup.Storage = ""
	if e := f.p.Validate(context.Background(), nil, f.rc.Cluster); e != nil {
		t.Fatal(e)
	}
	if f.rc.Cluster.Spec.Backup.Storage != b.Storage {
		t.Fatal("storage not defaulted")
	}
	if busy, e := f.p.initialRestore(context.Background(), f.rc, s); e != nil || !busy {
		t.Fatalf("enqueue %v %v", busy, e)
	}
	original, e := f.rc.Store.GetRestore(context.Background(), f.rc.Cluster.Ref(), "initial-restore")
	if e != nil {
		t.Fatal(e)
	}
	// Restart before saving s.InitialRestore, while atomic enqueue already committed.
	s.InitialRestore = ""
	p := New(Options{Store: f.rc.Store})
	if busy, e := p.initialRestore(context.Background(), f.rc, s); e != nil || !busy {
		t.Fatalf("adopt %v %v", busy, e)
	}
	adopted, e := f.rc.Store.GetRestore(context.Background(), f.rc.Cluster.Ref(), "initial-restore")
	if e != nil || original.OperationID != adopted.OperationID || original.OperationID == "" {
		t.Fatalf("not adopted %+v %+v %v", original, adopted, e)
	}
	op, e := f.rc.Store.GetOperation(context.Background(), original.OperationID)
	if e != nil || op.Kind != domain.OperationRestore {
		t.Fatalf("atomic operation %+v %v", op, e)
	}
	if s.InitialRestore != "initial-restore" {
		t.Fatal("adoption receipt missing")
	}
}

func TestRestoreExistingBackupAttemptIdentity(t *testing.T) {
	for _, scenario := range []string{"before-post", "after-post", "earlier-clock", "ambiguous", "accepted-old-success"} {
		t.Run(scenario, func(t *testing.T) {
			f, s, n, b := backupFixture(t, 1)
			b = completeBackup(t, f, s, n, b)
			bindRestoreStorage(t, f, b, n)
			n.restore = wvapi.BackupSuccess
			n.restoreMetadata = &restoreIdentity{ID: b.Weaviate.ID, StartedAt: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), CompletedAt: time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC), Status: wvapi.BackupSuccess}
			r := &domain.Restore{Name: "recover", Namespace: f.rc.Cluster.Ref().Namespace, Cluster: f.rc.Cluster.Name, Source: domain.RestoreSource{Backup: b.Name}, Phase: domain.PhasePending, CreatedAt: time.Now()}
			if err := f.rc.Store.CreateRestore(context.Background(), r); err != nil {
				t.Fatal(err)
			}
			n.collections = []string{"Article"}
			cp, _, err := restoreStep(t, f, s, operationState{})
			if err != nil {
				t.Fatal(err)
			}
			raw, err := secret(context.Background(), f.rc, "operations/restore-op/restore-submission")
			var receipt restoreReceipt
			if err != nil || json.Unmarshal(raw, &receipt) != nil || receipt.Baseline == nil || !receipt.BaselineCaptured || len(n.collections) != 1 {
				t.Fatalf("baseline not durable before clearing: %s %v", raw, err)
			}
			for cp.Stage != "submit" {
				cp, _, err = restoreStep(t, f, s, cp)
				if err != nil {
					t.Fatal(err)
				}
			}
			switch scenario {
			case "after-post":
				// A successful second restore finishes before its POST receipt
				// can be marked accepted: both native statuses are SUCCESS.
				n.restoreMetadata.StartedAt = n.restoreMetadata.StartedAt.Add(24 * time.Hour)
				n.restores = 1
			case "earlier-clock":
				n.restoreMetadata.StartedAt = n.restoreMetadata.StartedAt.Add(-24 * time.Hour)
				n.restores = 1
			case "ambiguous":
				n.restoreMetadata.StartedAt = time.Time{}
			case "accepted-old-success":
				receipt.Accepted = true
				raw, _ = json.Marshal(receipt)
				if err = putSecret(context.Background(), f.rc, "operations/restore-op/restore-submission", raw); err != nil {
					t.Fatal(err)
				}
			}
			cp, out, err := restoreStep(t, f, s, cp)
			if scenario == "ambiguous" || scenario == "accepted-old-success" {
				if err == nil || !strings.Contains(err.Error(), "ambiguous") || cp.Stage == "health" {
					t.Fatalf("ambiguous native status adopted: %+v %+v %v", cp, out, err)
				}
				saved, _ := f.rc.Store.GetRestore(context.Background(), f.rc.Cluster.Ref(), r.Name)
				if saved.Phase == domain.PhaseSucceeded || n.restores != 0 {
					t.Fatalf("ambiguous restore succeeded or resubmitted: %+v posts=%d", saved, n.restores)
				}
				return
			}
			if err != nil || cp.Stage != "poll" || n.restores != 1 {
				t.Fatalf("second restore submission: %+v posts=%d %v", cp, n.restores, err)
			}
			n.restore = wvapi.BackupSuccess
			cp, _, err = restoreStep(t, f, s, cp)
			if err != nil || cp.Stage != "health" {
				t.Fatalf("second restore polling: %+v %v", cp, err)
			}
			_, out, err = restoreStep(t, f, s, cp)
			if err != nil || !out.Done {
				t.Fatalf("second restore completion: %+v %v", out, err)
			}
			// Successful completion relinquishes the source immediately.
			st := f.rc.Store.(*store.Store)
			acquired, err := st.AcquireBackupRestore(context.Background(), f.rc.Cluster.Ref(), b.Name, domain.ClusterRef{Name: "next-target"}, "next", "next-operation")
			if err != nil || !acquired {
				t.Fatalf("completed restore retained ownership: %v %v", acquired, err)
			}
		})
	}
}

func TestRestoreSharedSourceWaitsBeforeClearingOrAdopting(t *testing.T) {
	f, s, n, b := backupFixture(t, 1)
	b = completeBackup(t, f, s, n, b)
	bindRestoreStorage(t, f, b, n)
	ctx := context.Background()
	r := &domain.Restore{Name: "recover", Namespace: f.rc.Cluster.Ref().Namespace, Cluster: f.rc.Cluster.Name, Source: domain.RestoreSource{Backup: b.Name}, Phase: domain.PhasePending}
	if err := f.rc.Store.CreateRestore(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, _, err := restoreStep(t, f, s, operationState{}); err != nil {
		t.Fatal(err)
	}
	target := *f.rc.Cluster
	target.Name = "second"
	if err := f.rc.Store.(*store.Store).CreateCluster(ctx, &target); err != nil {
		t.Fatal(err)
	}
	second := *r
	second.Cluster = target.Name
	second.Source.SourceCluster = f.rc.Cluster.Name
	second.SourceNamespace = f.rc.Cluster.Ref().Namespace
	if err := f.rc.Store.CreateRestore(ctx, &second); err != nil {
		t.Fatal(err)
	}
	rc := *f.rc
	rc.Cluster = &target
	// A native SUCCESS belongs to the first target, not the second one.
	n.restore = wvapi.BackupSuccess
	n.restoreMetadata = &restoreIdentity{ID: b.Weaviate.ID, StartedAt: time.Now(), Status: wvapi.BackupSuccess}
	n.collections = []string{"DoNotClear"}
	n.order = nil
	out, err := f.p.runRestore(ctx, &rc, s, &domain.Operation{ID: "second-op"}, operationState{}, second.Name)
	if err != nil || out.Done || out.Step != "waiting-for-source-backup" || n.restores != 0 || len(n.order) != 0 || len(n.collections) != 1 {
		t.Fatalf("second target did not wait: %+v posts=%d order=%v %v", out, n.restores, n.order, err)
	}
	if _, err = secret(ctx, &rc, "operations/second-op/restore-submission"); !missing(err) {
		t.Fatalf("waiting target captured another target's restore baseline: %v", err)
	}
	saved, err := rc.Store.GetRestore(ctx, target.Ref(), second.Name)
	if err != nil || saved.Phase == domain.PhaseSucceeded || !strings.Contains(saved.Error, "Waiting") {
		t.Fatalf("waiting target status: %+v %v", saved, err)
	}
}

func TestInitialDataSourceAppliedRequiresCompletedClone(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		want, bad bool
	}{
		{"empty", `{}`, false, false},
		{"queued", `{"initialRestore":"initial-restore"}`, false, false},
		{"other stage", `{"initialRestore":"health"}`, false, false},
		{"complete", `{"initialRestore":"done"}`, true, false},
		{"invalid", `{`, false, true},
		{"invalid receipt", `{"initialRestore":true}`, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &domain.Cluster{Spec: domain.ClusterSpec{DataSource: &domain.DataSource{Cluster: "source", Backup: "base"}}, Internal: json.RawMessage(tc.raw)}
			c.Status.ObservedGeneration = 1
			p := &Provider{} // Reconstruct from the persisted receipt, without native calls.
			var observer engine.InitialDataSourceObserver = p
			applied, err := observer.InitialDataSourceApplied(c)
			if applied != tc.want || (err != nil) != tc.bad {
				t.Fatalf("clone outcome: applied=%v err=%v", applied, err)
			}
			status := domain.ClusterStatus{Phase: domain.PhaseReady}
			err = engine.ApplyInitialDataSourceStatus(p, c, &status)
			if (err != nil) != tc.bad || (status.Phase == domain.PhaseReady) != tc.want {
				t.Fatalf("generic readiness: status=%+v err=%v", status, err)
			}
		})
	}
}
