// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestRestoreReformsNewGroupAndRecoversEveryCheckpoint(t *testing.T) {
	for _, checkpoint := range []string{"fence", "stopped", "dispatch", "poll", "reform", "activate"} {
		t.Run(checkpoint, func(t *testing.T) {
			p, rc, b := backupFixture(t, "group")
			s, _ := load(rc.Cluster)
			oldGroup := s.Formation.GroupName
			oldEpoch := s.Formation.FormationEpoch
			base := inventoryBackup(t, rc, "base", "full", "", 10, time.Unix(10, 0))
			base.RecoveryIdentity = "backup-source"
			if err := captureFixtureBackupAccess(t.Context(), rc, &base); err != nil {
				t.Fatal(err)
			}
			if err := rc.Store.UpdateBackup(t.Context(), &base); err != nil {
				t.Fatal(err)
			}
			restore := &domain.Restore{Namespace: "default", Cluster: "demo", Name: "recover", Phase: domain.PhasePending, Source: domain.RestoreSource{Backup: base.Name}}
			if err := rc.Store.CreateRestore(t.Context(), restore); err != nil {
				t.Fatal(err)
			}
			var req myspec.RestoreRequest
			receipts := map[string]bool{}
			dispatches := 0
			b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				switch r.URL.Path {
				case "/v1/mysql/offline-gtid":
					st := b.native[name]
					json.NewEncoder(w).Encode(myspec.OfflineGTIDResponse{Stopped: st.MemberState == "OFFLINE", ServerUUID: st.ServerUUID, GTIDExecuted: st.GTIDExecuted, DataIdentity: st.DataIdentity})
					return true
				case "/v1/mysql/restore":
					var request myspec.RestoreRequest
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
					}
					if !receipts[request.Receipt] {
						receipts[request.Receipt] = true
						dispatches++
						req = request
					}
					json.NewEncoder(w).Encode(myspec.RestoreStatus{Receipt: request.Receipt, State: "running"})
					return true
				case "/v1/mysql/restore-status":
					st := b.native[name]
					st.ServerUUID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
					st.DataIdentity = myspec.DataIdentity{Initialized: true, ServerUUID: st.ServerUUID}
					st.MemberState = "OFFLINE"
					st.MemberRole = ""
					b.native[name] = st
					json.NewEncoder(w).Encode(myspec.RestoreStatus{Receipt: req.Receipt, State: "done", Stage: "done", ServerUUID: st.ServerUUID})
					return true
				}
				return false
			}
			op := &domain.Operation{ID: "restore-receipt", Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"recover"}`)}
			handedOff := false
			done := false
			sawPendingFollowerAfterSeed := false
			for range 100 {
				var result engine.OpResult
				var err error
				if !handedOff {
					result, err = p.RunOperation(t.Context(), rc, op)
					op.Checkpoint = result.Checkpoint
					if result.Step == checkpoint {
						if _, e := p.CancelOperation(t.Context(), rc, op); e != nil {
							t.Fatal(e)
						}
						handedOff = true
					}
				} else {
					current, _ := load(rc.Cluster)
					_, err = p.reconcileRestoreRecovery(t.Context(), rc, current)
					if e := save(rc.Cluster, current); e != nil {
						t.Fatal(e)
					}
					bs, e := loadBackupState(rc.Cluster)
					if e != nil {
						t.Fatal(e)
					}
					done = bs.RestoreOperation == nil
				}
				if err != nil {
					t.Fatal(err)
				}
				sawPendingFollowerAfterSeed = assertRestoreCloneConfig(t, rc) || sawPendingFollowerAfterSeed
				b.apply(t, p, rc)
				if done {
					break
				}
				p = New(Options{})
			}
			if !done || !handedOff {
				t.Fatalf("restore did not recover from %s", checkpoint)
			}
			if !sawPendingFollowerAfterSeed {
				t.Fatal("restore never exposed outstanding follower clone after seed formation")
			}
			if assertRestoreCloneConfig(t, rc) {
				t.Fatal("completed restore retained pending member clone")
			}
			s, _ = load(rc.Cluster)
			if s.Formation.GroupName == oldGroup || s.Formation.GroupName != req.Restore.NewGroupName || s.Formation.FormationEpoch <= oldEpoch || s.Formation.State != "formed" {
				t.Fatalf("restored group identity not replaced: %+v", s.Formation)
			}
			if dispatches != 1 || req.Receipt != op.ID || len(req.Restore.ObjectPaths) != 1 {
				t.Fatal("restore receipt/chain invalid", dispatches, req)
			}
			for _, m := range s.data() {
				if m.Offline || !m.Initialized || m.Stage != "joined" {
					t.Fatalf("member not recovered: %+v", m)
				}
			}
			saved, err := rc.Store.GetRestore(t.Context(), rc.Cluster.Ref(), restore.Name)
			if err != nil || saved.Phase != domain.PhaseSucceeded {
				t.Fatal(saved, err)
			}
		})
	}
}
func TestRestoreRejectsMissingChainBeforeFence(t *testing.T) {
	p, rc, _ := backupFixture(t, "single")
	b := inventoryBackup(t, rc, "orphan", "incremental", "missing", 20, time.Unix(20, 0))
	b.RecoveryIdentity = "source"
	if err := captureFixtureBackupAccess(t.Context(), rc, &b); err != nil {
		t.Fatal(err)
	}
	if err := rc.Store.UpdateBackup(t.Context(), &b); err != nil {
		t.Fatal(err)
	}
	r := &domain.Restore{Namespace: "default", Cluster: "demo", Name: "bad", Source: domain.RestoreSource{Backup: b.Name}}
	if err := rc.Store.CreateRestore(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	_, err := p.RunOperation(t.Context(), rc, &domain.Operation{ID: "bad", Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"bad"}`)})
	if err == nil {
		t.Fatal("missing parent accepted")
	}
	s, _ := load(rc.Cluster)
	for _, m := range s.data() {
		if m.Offline {
			t.Fatal("preflight error fenced target")
		}
	}
	bs, _ := loadBackupState(rc.Cluster)
	if bs.RestoreOperation != nil {
		t.Fatal("preflight error owns recovery")
	}
}

func TestInitialRestoreIndependentRepository(t *testing.T) {
	p, rc, b := backupFixture(t, "single")
	source := &domain.Cluster{Namespace: "default", Name: "source", Spec: rc.Cluster.Spec}
	if err := rc.Store.(*store.Store).CreateCluster(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	base := domain.Backup{Namespace: "default", Cluster: "source", Name: "source-base", Storage: "archive", Provider: "mysql", Type: "full", Phase: domain.PhaseSucceeded, SourceVersion: rc.Cluster.Spec.Version, RecoveryIdentity: "source-snapshot", MySQL: &domain.MySQLBackup{Label: "base", ChainRootLabel: "base", Type: "full", Repository: "prefix/mysql/default/source-sourceuid", ClusterUID: "sourceuid", ServerVersion: rc.Cluster.Spec.Version, ToLSN: 10, EndTime: time.Unix(10, 0), ObjectPaths: []string{"prefix/mysql/default/source-sourceuid/base/base"}}}
	if err := captureFixtureBackupAccess(t.Context(), rc, &base); err != nil {
		t.Fatal(err)
	}
	if err := rc.Store.CreateBackup(t.Context(), &base); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.DataSource = &domain.DataSource{Namespace: "default", Cluster: "source", Backup: base.Name}
	rc.Cluster.Spec.Backup.PITR.Enabled = true
	var request myspec.RestoreRequest
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		switch r.URL.Path {
		case "/v1/mysql/offline-gtid":
			st := b.native[name]
			json.NewEncoder(w).Encode(myspec.OfflineGTIDResponse{Stopped: st.MemberState == "OFFLINE", ServerUUID: st.ServerUUID, GTIDExecuted: st.GTIDExecuted, DataIdentity: st.DataIdentity})
			return true
		case "/v1/mysql/restore":
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			json.NewEncoder(w).Encode(myspec.RestoreStatus{Receipt: request.Receipt, State: "running"})
			return true
		case "/v1/mysql/restore-status":
			json.NewEncoder(w).Encode(myspec.RestoreStatus{Receipt: request.Receipt, State: "done", ServerUUID: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"})
			return true
		}
		return false
	}
	s, _ := load(rc.Cluster)
	op := &domain.Operation{ID: "initial-restore-" + s.UID, Kind: "initial-restore"}
	for range 50 {
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
		bs, _ := loadBackupState(rc.Cluster)
		s, _ = load(rc.Cluster)
		cfg, err := p.MemberConfig(t.Context(), rc, s.data()[0].Name)
		if err != nil {
			t.Fatal(err)
		}
		var sp myspec.Spec
		if err = json.Unmarshal(cfg.EngineSpec, &sp); err != nil {
			t.Fatal(err)
		}
		if !bs.CloneComplete && sp.Backup.Binlog.Enabled {
			t.Fatal("clone archived before independent repository activation")
		}
		b.apply(t, p, rc)
		if result.Done {
			if request.Restore.Source.Path != base.MySQL.Repository || !sp.Backup.Binlog.Enabled || sp.Backup.Repository.Path == base.MySQL.Repository || !bs.CloneComplete || bs.Generation != 1 {
				t.Fatal("clone repository isolation failed", request, sp.Backup, bs)
			}
			return
		}
	}
	t.Fatal("initial restore did not finish")
}
