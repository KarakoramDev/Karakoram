// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestPXCRetirementRejectsUnusableRecordedSeedWithValidatedConfigs(t *testing.T) {
	for _, offlineHold := range []bool{false, true} {
		name := "native-unavailable"
		if offlineHold {
			name = "offline-hold-despite-native-online"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, b := setupPXC(t)
			converge(t, p, rc, b)
			oldName := rc.Cluster.Name + "m1"
			payload, _ := json.Marshal(map[string]string{"member": oldName})
			op := &domain.Operation{ID: "seed-health-replacement", Kind: domain.OperationReplaceMember, Payload: payload}
			var s *state
			joined := false
			for range 120 {
				result, err := p.RunOperation(t.Context(), rc, op)
				if err != nil {
					t.Fatal(err)
				}
				op.Checkpoint = result.Checkpoint
				if p.SyncDuringOperation(op) {
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
				}
				b.apply(t, p, rc)
				s, err = load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				if s.Replacement != nil && s.Replacement.Stage == "retire" && s.find(s.Replacement.New).Stage == "joined" {
					joined = true
					break
				}
			}
			if !joined {
				t.Fatal("fixture did not join the real replacement")
			}
			newName := s.Replacement.New
			old := s.find(oldName)
			old.Removing, old.Offline, old.Retirement = true, true, "leave"
			s.Replacement.Recovery = "complete"
			recorded := s.find(rc.Cluster.Name + "m0")
			recorded.Offline = offlineHold
			s.Formation.Seed = recorded.Name
			s.find(newName).GaleraSSTDonor = oldName
			oldNative := b.native[oldName]
			oldNative.MemberState = "OFFLINE"
			b.native[oldName] = oldNative
			seedNative := b.native[recorded.Name]
			seedNative.MemberState = "OFFLINE"
			if offlineHold {
				seedNative.MemberState = "ONLINE"
			}
			b.native[recorded.Name] = seedNative
			publishRevision(s)
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			seen := validateRetirementAgentReloads(t, rc, b)
			p = New(Options{Provider: "pxc"})
			// The unavailable member can legitimately defer deletion. It must
			// not remain the selected seed or stale donor in rendered configs.
			if _, err := p.Sync(freshMySQLReads(t.Context()), rc); err != nil && !errors.Is(err, engine.ErrUnavailable) {
				t.Fatal(err)
			}
			current, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			want := rc.Cluster.Name + "m2"
			if current.Formation.Seed != want || current.find(newName).GaleraSSTDonor != want {
				t.Fatal("retirement reused an Offline/unavailable recorded seed instead of the healthy admitted peer", current.Formation.Seed, current.find(newName).GaleraSSTDonor)
			}
			if current.find(want).Offline || b.native[want].MemberState != "ONLINE" || pxcJoinRoutingFenced(current, current.find(want)) {
				t.Fatal("replacement seed did not have real admitted native ONLINE evidence")
			}
			applyAgentValidatedRetirementConfig(t, p, rc, b)
			if seen.reloads == 0 {
				t.Fatal("seed migration never passed rendered configs through actual agent validation")
			}
		})
	}
}

func TestPXCRoutingHeldDiagnosticMemberCanBeActuallyReplaced(t *testing.T) {
	testPXCFailedIrreversibleReplacementJoinOwner(t, true, func(p *Core, rc *engine.Context, b *fakeBackend, s *state) {
		held := s.find(rc.Cluster.Name + "m3")
		if held == nil || !pxcJoinRoutingFenced(s, held) || activeGTIDIncident(s) == nil {
			t.Fatal("exit must begin with the actual routing-held member and original live latch")
		}
		beforeIDs := map[string]string{}
		for name, service := range b.services {
			beforeIDs[name] = service.ID
		}
		nextIndex := s.NextIndex
		payload, _ := json.Marshal(map[string]string{"member": held.Name})
		op := &domain.Operation{ID: "replace-routing-held-diagnostic-member", Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationReplaceMember, Phase: domain.PhaseRunning, Actor: "api", Payload: payload}
		if err := rc.Store.EnqueueOperation(t.Context(), op); err != nil {
			t.Fatal(err)
		}
		finished := false
		for range 240 {
			p = New(Options{Provider: "pxc"})
			result, err := p.RunOperation(t.Context(), rc, op)
			if err != nil {
				t.Fatal("real replacement rejected its routing-held target", err)
			}
			op.Checkpoint, op.Step, op.Message = result.Checkpoint, result.Step, result.Message
			if result.Done {
				op.Phase = domain.PhaseSucceeded
			}
			if err := rc.Store.(*store.Store).UpdateOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			if !result.Done && p.SyncDuringOperation(op) {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			}
			applyAgentValidatedRetirementConfig(t, p, rc, b)
			if result.Done {
				finished = true
				break
			}
		}
		if !finished {
			t.Fatal("routing-held member replacement never completed actual retirement")
		}
		for range 100 {
			current, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			if !pxcFormationMappingPending(current) {
				break
			}
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			applyAgentValidatedRetirementConfig(t, p, rc, b)
		}
		current, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if current.Replacement != nil || current.LastReplacement != op.ID || current.find(held.Name) != nil || b.services[held.Name] != nil || b.services[held.Volume] != nil || current.NextIndex != nextIndex+1 || len(current.data()) != 3 || pxcFormationMappingPending(current) {
			t.Fatal("replacement did not finish exact held-member deletion and its new admission")
		}
		for name, id := range beforeIDs {
			if name == held.Name || name == held.Volume {
				continue
			}
			if service := b.services[name]; service == nil || service.ID != id {
				t.Fatal("held-member replacement changed another retained resource", name)
			}
		}
		if activeGTIDIncident(current) == nil {
			t.Fatal("managed replacement acknowledged history without any full backup")
		}
		assertPXCHistoryClientsServing(t, p, rc, current)
	})
}

func TestPXCDiagnosticJoinRestoreCompletesBeforeAnyFullAcknowledgement(t *testing.T) {
	testPXCFailedIrreversibleReplacementJoinOwner(t, true, func(p *Core, rc *engine.Context, b *fakeBackend, s *state) {
		if activeGTIDIncident(s) == nil || !pxcJoinRoutingFenced(s, s.find(rc.Cluster.Name+"m3")) {
			t.Fatal("restore must begin with the original incident active and copied member unrouted")
		}
		incidentAt := activeGTIDIncident(s).DetectedAt
		rc.Clock = nil
		pxcHistoryStorage(t, rc, b)
		base := inventoryBackup(t, rc, "trusted-before-history-ack", "full", "", 10, time.Now().UTC())
		restore := &domain.Restore{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "restore-before-history-ack", Phase: domain.PhasePending, Source: domain.RestoreSource{Backup: base.Name}}
		if err := rc.Store.CreateRestore(t.Context(), restore); err != nil {
			t.Fatal(err)
		}
		op := &domain.Operation{ID: "complete-restore-before-history-ack", Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning, Actor: "api", Payload: json.RawMessage(`{"restore":"restore-before-history-ack"}`)}
		if err := rc.Store.EnqueueOperation(t.Context(), op); err != nil {
			t.Fatal(err)
		}
		var request myspec.RestoreRequest
		requested, prepared := false, map[string]bool{}
		proxyStops, proxyStarts := 0, 0
		prior := b.action
		b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
			if strings.Contains(r.URL.Path, "/processes/proxysql/stop") {
				proxyStops++
			}
			if strings.Contains(r.URL.Path, "/processes/proxysql/start") {
				proxyStarts++
			}
			switch r.URL.Path {
			case "/v1/mysql/" + myspec.ActionRestore:
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				current, err := load(rc.Cluster)
				backup, backupErr := loadBackupState(rc.Cluster)
				if err != nil || backupErr != nil || backup.RestoreOperation == nil || backup.RestoreOperation.ID != op.ID {
					t.Error("physical dispatch lacked its explicit durable restore owner", err, backupErr)
					return true
				}
				for _, member := range current.data() {
					if !member.Offline || b.native[member.Name].MemberState == "ONLINE" {
						t.Error("restore dispatched before the verified native all-member stop", member.Name)
					}
				}
				requested = true
				_ = json.NewEncoder(w).Encode(myspec.RestoreStatus{Receipt: request.Receipt, State: "running"})
				return true
			case "/v1/mysql/" + myspec.ActionRestoreStatus:
				if !requested {
					t.Error("physical completion was polled before dispatch")
				}
				native := b.native[name]
				native.ServerUUID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
				native.GTIDExecuted, native.PhysicalIdentity = base.MySQL.GTIDSet, base.MySQL.PhysicalIdentity
				native.DataIdentity = myspec.DataIdentity{Initialized: true, ServerUUID: native.ServerUUID}
				native.MemberState, native.MemberRole = "OFFLINE", ""
				b.native[name] = native
				users := make([]string, 0, len(request.Restore.RecoveryUsers))
				for _, user := range request.Restore.RecoveryUsers {
					users = append(users, user.Name)
				}
				_ = json.NewEncoder(w).Encode(myspec.RestoreStatus{Receipt: request.Receipt, State: "done", Stage: "done", ServerUUID: native.ServerUUID, RecoveredUsers: users})
				return true
			case "/v1/mysql/" + myspec.ActionGaleraPrepareJoin:
				current, err := load(rc.Cluster)
				if err != nil {
					t.Error(err)
					return true
				}
				follower := current.find(name)
				if follower == nil || name == current.Formation.Seed || !follower.Offline || current.RestoreGTIDFence != op.ID {
					t.Error("restored follower SST lacked native stopped-member authority", name)
				}
				native := b.native[name]
				native.GTIDExecuted, native.PhysicalIdentity = b.native[current.Formation.Seed].GTIDExecuted, base.MySQL.PhysicalIdentity
				b.native[name] = native
				prepared[name] = true
				_ = json.NewEncoder(w).Encode(struct{}{})
				return true
			}
			return prior != nil && prior(w, r, name)
		}
		for range 240 {
			p = New(Options{Provider: "pxc"})
			result, err := p.RunOperation(t.Context(), rc, op)
			if err != nil {
				t.Fatal("diagnostic join owner prevented complete explicit restore", err)
			}
			op.Checkpoint, op.Step, op.Message = result.Checkpoint, result.Step, result.Message
			if result.Done {
				op.Phase = domain.PhaseSucceeded
			}
			if err := rc.Store.(*store.Store).UpdateOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			applyAgentValidatedRetirementConfig(t, p, rc, b)
			current, err := load(rc.Cluster)
			if err != nil || activeGTIDIncident(current) == nil || !activeGTIDIncident(current).DetectedAt.Equal(incidentAt) {
				t.Fatal("restore cleared or replaced the original history latch before any full acknowledgement", err)
			}
			if !result.Done {
				continue
			}
			var cp backupCheckpoint
			if err := json.Unmarshal(result.Checkpoint, &cp); err != nil {
				t.Fatal(err)
			}
			backup, err := loadBackupState(rc.Cluster)
			if err != nil || !requested || len(prepared) != 2 || cp.GTIDCertification == nil || !cp.GTIDCertification.Certified || backup.RestoreOperation != nil || !backup.CloneComplete || proxyStops == 0 || proxyStarts == 0 {
				t.Fatal("restore did not finish physical recovery, certification, activation and owned proxy release", err)
			}
			assertPXCHistoryClientsServing(t, p, rc, current)
			assertPXCHistoryLatchCondition(t, p, rc)
			saved, err := rc.Store.GetRestore(t.Context(), rc.Cluster.Ref(), restore.Name)
			if err != nil || saved.Phase != domain.PhaseSucceeded {
				t.Fatal("complete restore was not durably inventoried", saved, err)
			}
			return
		}
		t.Fatal("explicit restore from the diagnostic copied-history state never activated")
	})
}

func TestPXCDiagnosticJoinDeleteCompletesFullTeardown(t *testing.T) {
	testPXCFailedIrreversibleReplacementJoinOwner(t, true, func(p *Core, rc *engine.Context, b *fakeBackend, s *state) {
		if activeGTIDIncident(s) == nil {
			t.Fatal("delete must begin before the original incident is acknowledged")
		}
		names := make([]string, 0, len(s.Members))
		for _, member := range s.Members {
			names = append(names, member.Name)
		}
		for range 100 {
			p = New(Options{Provider: "pxc"})
			done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
			if err != nil {
				t.Fatal("diagnostic join owner blocked full explicit teardown", err)
			}
			if !done {
				continue
			}
			if len(b.services) != 0 {
				t.Fatal("full deletion left owned runtime or volume resources", b.services)
			}
			for _, name := range names {
				if token, err := rc.Secrets.GetAgentToken(t.Context(), rc.Cluster.Ref(), name); err == nil || !missing(err) || token != nil {
					t.Fatal("full teardown retained a member's agent credential", name, err)
				}
			}
			return
		}
		t.Fatal("explicit deletion stopped before complete runtime/volume/credential teardown")
	})
}
