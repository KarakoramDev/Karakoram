// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type pxcHistoryClientActions struct {
	proxyStops  int
	proxyStarts int
	probes      int
}

// Start with the real first-Ready certification, then introduce a node-local
// executed SID without changing its runtime, volume, epoch, or native health.
func latchedServingPXC(t *testing.T) (*Core, *engine.Context, *fakeBackend, string, *pxcHistoryClientActions) {
	t.Helper()
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	certification := s.Formation.GTIDCertification
	if certification == nil || certification.Checkpoint.GTIDCertification == nil || !certification.Checkpoint.GTIDCertification.Certified || pxcFormationMappingPending(s) {
		t.Fatal("fixture did not pass actual first-Ready mapping certification")
	}
	owner := certification.Owner
	actions := &pxcHistoryClientActions{}
	prior := b.action
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if strings.Contains(r.URL.Path, "/processes/proxysql/stop") {
			actions.proxyStops++
		}
		if strings.Contains(r.URL.Path, "/processes/proxysql/start") {
			actions.proxyStarts++
		}
		if r.URL.Path == "/v1/mysql/"+myspec.ActionGaleraHistoryProbe {
			actions.probes++
		}
		return prior != nil && prior(w, r, name)
	}
	bad := s.data()[2].Name
	native := b.native[bad]
	if native.GTIDExecuted != "" {
		native.GTIDExecuted += ","
	}
	native.GTIDExecuted += native.ServerUUID + ":9000001"
	b.native[bad] = native
	for range 4 {
		p = New(Options{Provider: "pxc"})
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		b.apply(t, p, rc)
		current, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if current.Formation.GTIDCertification.Owner != owner || pxcFormationMappingPending(current) {
			t.Fatal("history-only growth replaced a serving cluster's mapping owner")
		}
		assertPXCHistoryClientsServing(t, p, rc, current)
	}
	s, err = load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if activeGTIDIncident(s) == nil || actions.proxyStops != 0 || actions.probes != 0 {
		t.Fatal("local history divergence was not diagnostic-only", actions)
	}
	assertPXCHistoryLatchCondition(t, p, rc)
	return p, rc, b, bad, actions
}

func assertPXCHistoryClientsServing(t *testing.T, p *Core, rc *engine.Context, s *state) {
	t.Helper()
	if s.RestoreGTIDFence != "" {
		t.Fatal("history latch installed a whole-cluster SQL fence", s.RestoreGTIDFence)
	}
	for _, m := range s.Members {
		config, err := p.MemberConfig(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		var spec myspec.Spec
		if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		if m.Role == "proxy" && !slices.Contains(config.DesiredProcesses, "proxysql") {
			t.Fatal("history latch removed the serving proxy process", m.Name)
		}
		if m.Role == "data" && (m.GaleraJoinFence || spec.Galera == nil || spec.Galera.RestoreFence) {
			t.Fatal("history latch fenced an already admitted writer", m.Name)
		}
	}
}

func assertPXCHistoryLatchCondition(t *testing.T, p *Core, rc *engine.Context) {
	t.Helper()
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	for _, condition := range status.Conditions {
		if condition.Type == "GTIDHistorySafe" && condition.Status == "False" {
			if status.Phase != domain.PhaseDegraded {
				t.Fatal("latched serving cluster did not report Degraded", status.Phase)
			}
			return
		}
	}
	t.Fatal("serving-client recovery silently cleared the GTID history condition")
}

func assertPXCHistoryExitAdmission(t *testing.T, p *Core, rc *engine.Context) {
	t.Helper()
	if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || !allowed {
		t.Fatal("history latch prevented claiming an existing recovery operation", allowed, err)
	}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := maintenanceAllowed(t.Context(), rc, s); err != nil {
		t.Fatal("history latch prevented existing native maintenance", err)
	}
	if activeGTIDIncident(s) == nil {
		t.Fatal("admission acknowledged the incident without a full backup")
	}
}

func pxcHistoryStorage(t *testing.T, rc *engine.Context, b *fakeBackend) {
	t.Helper()
	storage := &domain.BackupStorage{Namespace: "default", Name: "archive", Type: "s3", EndpointURL: "https://objects.example", Bucket: "backups", Region: "region", Prefix: "prefix", ForcePathStyle: true, VerifyTLS: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
	if err := rc.Store.(*store.Store).CreateBackupStorage(t.Context(), storage); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Backup.Storage = storage.Name
	for name, native := range b.native {
		native.PhysicalIdentity = "physical-" + name
		b.native[name] = native
	}
}

// Exercise a persisted user operation and actual native backup receipt, rather
// than calling the GTID acknowledgement helper directly.
func queuedPXCHistoryFull(t *testing.T, rc *engine.Context, b *fakeBackend, name string, beforeClaim ...func()) {
	t.Helper()
	item := &domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: name, Storage: "archive", Type: "full", Phase: domain.PhasePending}
	if err := rc.Store.CreateBackup(t.Context(), item); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]string{"backup": name})
	if err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "full-" + name, Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationBackup, Phase: domain.PhaseRunning, Actor: "api", Payload: payload}
	if len(beforeClaim) > 0 {
		op.Phase = domain.PhasePending
	}
	st := rc.Store.(*store.Store)
	if err := st.EnqueueOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	var request myspec.BackupRequest
	prior := b.action
	b.action = func(w http.ResponseWriter, r *http.Request, member string) bool {
		switch r.URL.Path {
		case "/v1/mysql/backup":
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			_ = json.NewEncoder(w).Encode(myspec.BackupResponse{Receipt: request.Receipt, State: "running"})
			return true
		case "/v1/mysql/backup-info":
			_ = json.NewEncoder(w).Encode(myspec.BackupResponse{Receipt: request.Receipt, State: "done", Result: &myspec.BackupResult{ManifestDigest: fixtureManifestDigest, ManifestSize: 1, BackupType: "full-backuped", ServerVersion: b.native[member].Version, PhysicalIdentity: request.PhysicalIdentity, ToolVersion: "8.4.0-7", ToLSN: 10, EndTime: time.Now().UTC(), ObjectPrefix: request.ObjectPrefix, GTIDSet: b.native[member].GTIDExecuted}})
			return true
		}
		return prior != nil && prior(w, r, member)
	}
	defer func() { b.action = prior }()
	for _, before := range beforeClaim {
		before()
	}
	if len(beforeClaim) > 0 {
		claimed, err := st.ClaimOperation(t.Context(), rc.Cluster.Ref())
		if err != nil || claimed.ID != op.ID {
			t.Fatalf("queued full was not claimable: %+v %v", claimed, err)
		}
		op = claimed
	}
	for range 20 {
		p := New(Options{Provider: "pxc"})
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
		op.Step, op.Message = result.Step, result.Message
		if result.Done {
			op.Phase = domain.PhaseSucceeded
		}
		if err := st.UpdateOperation(t.Context(), op); err != nil {
			t.Fatal(err)
		}
		b.apply(t, p, rc)
		if !result.Done {
			continue
		}
		saved, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), name)
		if err != nil || saved.Phase != domain.PhaseSucceeded || saved.StartedAt == nil {
			t.Fatal("queued full did not inventory its actual native receipt", saved, err)
		}
		return
	}
	t.Fatal("queued full backup did not finish")
}

func TestPXCHistoryLatchAllowsManagedReplacementThenQueuedFull(t *testing.T) {
	p, rc, b, bad, actions := latchedServingPXC(t)
	assertPXCHistoryExitAdmission(t, p, rc)
	s, _ := load(rc.Cluster)
	before := gtidMemberIdentities(s, nil)
	incident := *activeGTIDIncident(s)
	payload, _ := json.Marshal(map[string]string{"member": bad})
	op := &domain.Operation{ID: "replace-errant-member", Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationReplaceMember, Phase: domain.PhaseRunning, Actor: "api", Payload: payload}
	if err := rc.Store.(*store.Store).EnqueueOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	runDay2Operation(t, rc, b, op)
	op.Phase = domain.PhaseSucceeded
	if err := rc.Store.(*store.Store).UpdateOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	// The admitted empty replacement owns only its own join certificate.
	// Finish that real lifecycle before the subsequently queued full backup.
	for range 60 {
		s, _ = load(rc.Cluster)
		if !pxcFormationMappingPending(s) {
			break
		}
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		b.apply(t, p, rc)
	}
	s, _ = load(rc.Cluster)
	if s.find(bad) != nil || b.services[bad] != nil || b.services[bad+"v"] != nil {
		t.Fatal("managed replacement did not retire the errant member's exact resources")
	}
	for _, member := range s.data() {
		if original, existed := before[member.Name]; existed && gtidMemberIdentities(s, nil)[member.Name] != original {
			t.Fatal("replacement changed an unrelated admitted incarnation", member.Name)
		}
	}
	if activeGTIDIncident(s) == nil || !activeGTIDIncident(s).DetectedAt.Equal(incident.DetectedAt) {
		t.Fatal("replacement silently acknowledged the history boundary")
	}
	if actions.proxyStops != 0 {
		t.Fatal("history recovery stopped existing proxy clients", actions.proxyStops)
	}
	assertPXCHistoryClientsServing(t, p, rc, s)
	assertPXCHistoryLatchCondition(t, p, rc)
	pxcHistoryStorage(t, rc, b)
	queuedPXCHistoryFull(t, rc, b, "after-managed-replacement")
	s, _ = load(rc.Cluster)
	if activeGTIDIncident(s) != nil || len(s.GTIDIncidents) == 0 {
		t.Fatal("fresh queued full did not acknowledge while preserving the incident ledger")
	}
	if actions.proxyStops != 0 {
		t.Fatal("full acknowledgement stopped existing clients")
	}
}

func TestPXCHistoryLatchAllowsQueuedFullAcknowledgement(t *testing.T) {
	p, rc, b, _, actions := latchedServingPXC(t)
	assertPXCHistoryExitAdmission(t, p, rc)
	pxcHistoryStorage(t, rc, b)
	queuedPXCHistoryFull(t, rc, b, "acknowledge-local-history")
	s, _ := load(rc.Cluster)
	if activeGTIDIncident(s) != nil || len(s.GTIDIncidents) == 0 {
		t.Fatal("queued full could not acknowledge a serving cluster's frozen history deltas")
	}
	assertPXCHistoryClientsServing(t, p, rc, s)
	if actions.proxyStops != 0 || actions.probes != 0 {
		t.Fatal("full acknowledgement re-entered whole-cluster certification", actions)
	}
}

func TestPXCHistoryFullUnblocksHealthWaitingUpdate(t *testing.T) {
	p, rc, b, _, _ := latchedServingPXC(t)
	pxcHistoryStorage(t, rc, b)
	rc.Cluster.Generation++
	update := &domain.Operation{ID: "health-waiting-update", Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationUpdate, Phase: domain.PhaseRunning, TargetGeneration: rc.Cluster.Generation}
	if err := rc.Store.(*store.Store).EnqueueOperation(t.Context(), update); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		b.apply(t, p, rc)
	}
	if rc.Cluster.Status.Phase != domain.PhaseDegraded || rc.Cluster.Status.ObservedGeneration >= rc.Cluster.Generation {
		t.Fatal("history latch did not preserve the update health wait", rc.Cluster.Status)
	}
	if !p.IncidentBackupReady(t.Context(), rc.Cluster, rc.Secrets, "full") {
		t.Fatal("health-waiting update blocked the explicit incident full")
	}
	queuedPXCHistoryFull(t, rc, b, "pending-update-acknowledgement")
	s, err := load(rc.Cluster)
	if err != nil || activeGTIDIncident(s) != nil || len(s.GTIDIncidents) == 0 {
		t.Fatal("full failed to acknowledge while retaining incident evidence", err)
	}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if rc.Cluster.Status.Phase != domain.PhaseReady || rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
		t.Fatal("acknowledgement did not release update convergence", rc.Cluster.Status)
	}
}

func TestPXCHistoryLatchAllowsExplicitRestoreAdmission(t *testing.T) {
	p, rc, b, _, actions := latchedServingPXC(t)
	assertPXCHistoryExitAdmission(t, p, rc)
	pxcHistoryStorage(t, rc, b)
	base := inventoryBackup(t, rc, "trusted-history-source", "full", "", 10, time.Now().UTC())
	restore := &domain.Restore{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "recover-history", Phase: domain.PhasePending, Source: domain.RestoreSource{Backup: base.Name}}
	if err := rc.Store.CreateRestore(t.Context(), restore); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "restore-history", Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning, Actor: "api", Payload: json.RawMessage(`{"restore":"recover-history"}`)}
	if err := rc.Store.(*store.Store).EnqueueOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	result, err := p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal("history latch refused explicit restore before native admission", err)
	}
	var cp backupCheckpoint
	if err := json.Unmarshal(result.Checkpoint, &cp); err != nil {
		t.Fatal(err)
	}
	if cp.Step != "preflight" || cp.Source == nil || cp.Request == nil || cp.StopBoundary != restoreStopUntouched {
		t.Fatal("restore did not resolve a real trusted source before native stop", cp.Step)
	}
	s, _ := load(rc.Cluster)
	assertPXCHistoryClientsServing(t, p, rc, s)
	if activeGTIDIncident(s) == nil || actions.proxyStops != 0 {
		t.Fatal("history-only admission cleared the latch or stopped clients before explicit restore fencing")
	}
	incidentAt := activeGTIDIncident(s).DetectedAt
	var request myspec.RestoreRequest
	requested, preparedFollowers := false, map[string]bool{}
	prior := b.action
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		switch r.URL.Path {
		case "/v1/mysql/" + myspec.ActionRestore:
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			current, err := load(rc.Cluster)
			backup, backupErr := loadBackupState(rc.Cluster)
			if err != nil || backupErr != nil || backup.RestoreOperation == nil || backup.RestoreOperation.ID != op.ID {
				t.Error("physical dispatch lacked the explicit durable restore owner", err, backupErr)
				return true
			}
			// Physical installation precedes the restored-startup SQL fence.
			// Its safety boundary is the already verified all-member stop.
			for _, member := range current.data() {
				if !member.Offline || b.native[member.Name].MemberState == "ONLINE" {
					t.Error("physical restore dispatched before stopping data members", member.Name)
				}
			}
			requested = true
			_ = json.NewEncoder(w).Encode(myspec.RestoreStatus{Receipt: request.Receipt, State: "running"})
			return true
		case "/v1/mysql/" + myspec.ActionRestoreStatus:
			if !requested {
				t.Error("restore completion polled before physical dispatch")
			}
			native := b.native[name]
			native.ServerUUID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
			native.GTIDExecuted = base.MySQL.GTIDSet
			native.PhysicalIdentity = base.MySQL.PhysicalIdentity
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
				t.Error("restore follower preparation lacked explicit stopped-follower ownership", name)
			}
			// Model the authorized SST replacing the follower's old local
			// history, including the errant SID that caused this incident.
			native := b.native[name]
			native.GTIDExecuted = b.native[current.Formation.Seed].GTIDExecuted
			native.PhysicalIdentity = base.MySQL.PhysicalIdentity
			b.native[name] = native
			preparedFollowers[name] = true
			_ = json.NewEncoder(w).Encode(struct{}{})
			return true
		}
		return prior != nil && prior(w, r, name)
	}
	op.Checkpoint = result.Checkpoint
	for range 180 {
		p = New(Options{Provider: "pxc"})
		result, err = p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal("explicit latched restore failed before activation", err)
		}
		op.Checkpoint, op.Step, op.Message = result.Checkpoint, result.Step, result.Message
		if result.Done {
			op.Phase = domain.PhaseSucceeded
		}
		if err := rc.Store.(*store.Store).UpdateOperation(t.Context(), op); err != nil {
			t.Fatal(err)
		}
		b.apply(t, p, rc)
		if !result.Done {
			continue
		}
		if err := json.Unmarshal(result.Checkpoint, &cp); err != nil {
			t.Fatal(err)
		}
		s, err = load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		backup, err := loadBackupState(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if !requested || len(preparedFollowers) != 2 || cp.GTIDCertification == nil || !cp.GTIDCertification.Certified || backup.RestoreOperation != nil || !backup.CloneComplete {
			t.Fatal("restore did not finish physical recovery, native certification and activation", cp.Step)
		}
		if s.RestoreGTIDBlocked != "" || actions.proxyStops == 0 || actions.proxyStarts == 0 {
			t.Fatal("explicit restore did not exercise and release its native client controls", actions)
		}
		assertPXCHistoryClientsServing(t, p, rc, s)
		if activeGTIDIncident(s) == nil || !activeGTIDIncident(s).DetectedAt.Equal(incidentAt) {
			t.Fatal("restore activation acknowledged historical continuity without a fresh full")
		}
		assertPXCHistoryLatchCondition(t, p, rc)
		saved, err := rc.Store.GetRestore(t.Context(), rc.Cluster.Ref(), restore.Name)
		if err != nil || saved.Phase != domain.PhaseSucceeded {
			t.Fatal("restore activation was not durably inventoried", saved, err)
		}
		return
	}
	t.Fatal("explicit latched restore never reached activation", op.Step)
}

func TestPXCHistoryLatchAllowsExplicitDelete(t *testing.T) {
	p, rc, b, _, actions := latchedServingPXC(t)
	assertPXCHistoryExitAdmission(t, p, rc)
	for range 60 {
		p = New(Options{Provider: "pxc"})
		done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
		if err != nil {
			t.Fatal("history latch refused explicit delete", err)
		}
		if done {
			if len(b.services) != 0 {
				t.Fatal("explicit delete left owned resources", b.services)
			}
			return
		}
	}
	t.Fatal("explicit delete could not escape the history latch", actions)
}

func TestPXCHistoricalServingCertificateNeverReentersAfterEpochChange(t *testing.T) {
	p, rc, b, _, actions := latchedServingPXC(t)
	s, _ := load(rc.Cluster)
	owner := s.Formation.GTIDCertification.Owner
	s.Formation.FormationEpoch++
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		b.apply(t, p, rc)
		s, _ = load(rc.Cluster)
		assertPXCHistoryClientsServing(t, p, rc, s)
		condition, _ := pxcFormationMappingCondition(s)
		if condition.Status != "Unknown" || s.Formation.GTIDCertification.Owner != owner || pxcFormationMappingPending(s) {
			t.Fatal("historical epoch mismatch claimed a fresh mapping or re-entered certification", condition)
		}
	}
	assertPXCHistoryExitAdmission(t, p, rc)
	if actions.probes != 0 || actions.proxyStops != 0 {
		t.Fatal("serving epoch change created a whole-cluster probe/fence owner", actions)
	}
}

func TestPXCCut45HistoryOnlyOwnerUpgradeReleasesClientsWithoutAcknowledgement(t *testing.T) {
	for _, scenario := range []string{"pending-unprobed", "blocked-history", "cleanup-complete", "older-epoch", "legacy-previous", "mixed-mapping", "release-retry"} {
		t.Run(scenario, func(t *testing.T) {
			p, rc, b, bad, actions := latchedServingPXC(t)
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			previous := s.Formation.GTIDCertification
			incidents, err := json.Marshal(s.GTIDIncidents)
			if err != nil {
				t.Fatal(err)
			}
			identities := gtidMemberIdentities(s, nil)
			safeThrough := s.LastGTIDSafeAt
			proof := &pxcRestoreCertification{Phase: "fence", Negotiated: true, FencesOwned: true}
			if scenario != "pending-unprobed" {
				// Preserve an actual native probe round from the certified fixture,
				// rather than manufacturing a successful mapping from a GTID set.
				priorEvidence := previous.Checkpoint.GTIDCertification.Evidence
				if len(priorEvidence) == 0 {
					t.Fatal("first-Ready fixture lacks its persisted native mapping evidence")
				}
				raw, err := json.Marshal(priorEvidence[len(priorEvidence)-1:])
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(raw, &proof.Evidence); err != nil {
					t.Fatal(err)
				}
				proof.Phase, proof.CleanupPending = "cleanup", true
			}
			if scenario == "mixed-mapping" {
				proof.Evidence[0].Mapping[bad] = "eeeeeeee-dddd-cccc-bbbb-aaaaaaaaaaaa"
			}
			if scenario == "legacy-previous" {
				previous.Legacy = true
				previous.Checkpoint.GTIDCertification.Legacy = true
				previous.Checkpoint.GTIDCertification.Certified = false
			}
			owner := &pxcFormationCertification{
				Owner: "cut45-history-only", Sequence: previous.Sequence + 1,
				Epoch: s.Formation.FormationEpoch, Group: s.Formation.GroupName,
				PreviousMapping: previous, Checkpoint: backupCheckpoint{Step: "certify", GTIDCertification: proof},
			}
			if scenario != "pending-unprobed" {
				owner.Blocked = "cut45 whole-cluster mapping certification was blocked by unrelated local history"
				owner.Checkpoint.Step = "certify-cleanup"
			}
			if scenario == "older-epoch" {
				s.Formation.FormationEpoch++
				owner.Epoch = s.Formation.FormationEpoch
			}
			s.RestoreGTIDFence = owner.Owner
			if scenario == "cleanup-complete" {
				s.RestoreGTIDFence = ""
				proof.FencesOwned, proof.CleanupPending, proof.CleanupComplete = false, false, true
				proof.Phase, owner.Checkpoint.Step = "blocked", "certify-blocked"
			}
			s.Formation.GTIDCertification = owner
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			b.apply(t, p, rc)
			if scenario == "release-retry" {
				prior := b.action
				failRelease := true
				b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
					if failRelease && strings.Contains(r.URL.Path, "/processes/proxysql/start") {
						http.Error(w, "proxy release response unavailable", http.StatusServiceUnavailable)
						return true
					}
					return prior != nil && prior(w, r, name)
				}
				if _, err := p.Sync(t.Context(), rc); err == nil {
					t.Fatal("upgrade retired its owner despite a failed proxy release")
				}
				current, err := load(rc.Cluster)
				if err != nil || current.Formation.GTIDCertification.Owner != owner.Owner || activeGTIDIncident(current) == nil {
					t.Fatal("failed release lost its durable owner or history evidence", err)
				}
				failRelease = false
			}
			for range 6 {
				p = New(Options{Provider: "pxc"})
				if _, err := p.Sync(t.Context(), rc); err != nil && !errors.Is(err, engine.ErrUnavailable) {
					t.Fatal(err)
				}
				b.apply(t, p, rc)
			}
			s, err = load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			restored := s.Formation.GTIDCertification
			if restored == nil || restored.Owner != previous.Owner || restored.PreviousMapping != nil || pxcFormationMappingPending(s) {
				t.Fatal("upgrade retained an unauthorized serving-cluster certification owner", restored)
			}
			if restored.Sequence < owner.Sequence {
				t.Fatal("upgrade reused a retired owner's native probe receipt sequence", restored.Sequence, owner.Sequence)
			}
			if !reflect.DeepEqual(gtidMemberIdentities(s, nil), identities) || !s.LastGTIDSafeAt.Equal(safeThrough) {
				t.Fatal("upgrade changed admitted data identities or the historical safe watermark")
			}
			after, err := json.Marshal(s.GTIDIncidents)
			if err != nil || string(after) != string(incidents) {
				t.Fatal("releasing cut45 controls mutated or acknowledged the sealed incident ledger", err)
			}
			if restored.RetiredCertification == nil || !reflect.DeepEqual(restored.RetiredCertification.Evidence, proof.Evidence) {
				t.Fatal("retiring cut45 ownership discarded its mapping probe evidence")
			}
			if actions.proxyStarts == 0 || actions.proxyStops != 0 || actions.probes != 0 {
				t.Fatal("upgrade failed to release proxies or re-entered whole-cluster certification", actions)
			}
			assertPXCHistoryClientsServing(t, p, rc, s)
			assertPXCHistoryLatchCondition(t, p, rc)
			assertPXCHistoryExitAdmission(t, p, rc)
			if scenario == "mixed-mapping" {
				start := activeGTIDIncident(s).DetectedAt.Add(time.Second)
				full := &domain.Backup{Name: "must-not-ack-mixed", Type: "full", Phase: domain.PhaseSucceeded, StartedAt: &start, MySQL: &domain.MySQLBackup{EndTime: start.Add(time.Second)}}
				if err := establishGTIDBaseline(t.Context(), rc, s, full); err == nil {
					t.Fatal("full acknowledgement silently blessed observed mixed native SID mappings")
				}
				if activeGTIDIncident(s) == nil || !pxcUnrepairedMixedMapping(s) {
					t.Fatal("failed mixed-mapping acknowledgement lost the safety evidence")
				}
			}
		})
	}
}

func TestPXCFormationPendingDistinguishesEmptyAdmissionFromHistoricalChanges(t *testing.T) {
	for _, tc := range []struct {
		name    string
		change  func(*state)
		pending bool
	}{
		{name: "already admitted", change: func(*state) {}},
		{name: "history grows", change: func(s *state) {
			s.GTIDIncidents = append(s.GTIDIncidents, gtidIncident{DetectedAt: time.Now().UTC(), Audited: true})
		}},
		{name: "native epoch changes", change: func(s *state) {
			s.Formation.FormationEpoch++
		}},
		{name: "admitted incarnation changes", change: func(s *state) {
			s.find("m0").ServiceID = "replacement-runtime"
		}},
		{name: "initialized recovery follower", change: func(s *state) {
			s.find("m0").Stage = "deployed"
		}},
		{name: "new empty planned member", change: func(s *state) {
			s.Members = append(s.Members, &member{Name: "m3", Role: "data", Stage: "planned"})
		}, pending: true},
		{name: "uncertified birth", change: func(s *state) {
			s.Formation.GTIDCertification.Checkpoint.GTIDCertification.Certified = false
		}, pending: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &state{
				Topology: "cluster",
				Formation: formation{State: "formed", FormationEpoch: 1, GTIDCertification: &pxcFormationCertification{
					Owner: "original-admission", Epoch: 1,
					Members:    map[string]gtidMemberIdentity{"m0": {ServiceID: "original-runtime"}},
					Checkpoint: backupCheckpoint{GTIDCertification: &pxcRestoreCertification{Certified: true}},
				}},
				Members: []*member{{Name: "m0", Role: "data", Stage: "joined", Initialized: true, ServiceID: "original-runtime"}},
			}
			tc.change(s)
			if pending := pxcFormationMappingPending(s); pending != tc.pending {
				t.Fatalf("pending=%t want=%t: only a real new empty admission may reopen this gate", pending, tc.pending)
			}
		})
	}
}

func TestPXCBlockedBirthAdmissionAllowsRestoreOnlyAfterOwnedCleanup(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	birth := s.Formation.GTIDCertification
	birth.PreviousMapping = nil
	birth.JoinOnly = false
	birth.Blocked = "birth mapping probe observed incompatible replicated SIDs; preserve data and restore from trusted history"
	proof := birth.Checkpoint.GTIDCertification
	proof.Certified, proof.FencesOwned = false, false
	proof.Phase, birth.Checkpoint.Step = "blocked", "certify-blocked"
	for _, cleanupComplete := range []bool{false, true} {
		proof.CleanupPending, proof.CleanupComplete = !cleanupComplete, cleanupComplete
		if err := save(rc.Cluster, s); err != nil {
			t.Fatal(err)
		}
		if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || allowed {
			t.Fatal("blocked birth admitted arbitrary ordinary maintenance", allowed, err)
		}
		kind, err := p.RecoveryOperationKind(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		if cleanupComplete && kind != domain.OperationRestore {
			t.Fatal("terminal non-JoinOnly birth failure stranded the trusted-restore exit", kind)
		}
		if !cleanupComplete && kind != "" {
			t.Fatal("blocked birth admitted recovery before its owned cleanup completed", kind)
		}
	}
	pxcHistoryStorage(t, rc, b)
	source := inventoryBackup(t, rc, "trusted-birth-recovery", "full", "", 10, time.Now().UTC())
	restore := &domain.Restore{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "recover-blocked-birth", Phase: domain.PhasePending, Source: domain.RestoreSource{Backup: source.Name}}
	if err := rc.Store.CreateRestore(t.Context(), restore); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "restore-blocked-birth", Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning, Actor: "api", Payload: json.RawMessage(`{"restore":"recover-blocked-birth"}`)}
	if err := rc.Store.(*store.Store).EnqueueOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	result, err := p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal("restore-only recovery claim could not enter real trusted-source preflight", err)
	}
	var checkpoint backupCheckpoint
	if err := json.Unmarshal(result.Checkpoint, &checkpoint); err != nil {
		t.Fatal(err)
	}
	if checkpoint.Step != "preflight" || checkpoint.Source == nil || checkpoint.Request == nil || checkpoint.StopBoundary != restoreStopUntouched {
		t.Fatal("blocked birth restore did not resolve a trusted source before native stop", checkpoint.Step)
	}
	current, err := load(rc.Cluster)
	if err != nil || current.Formation.GTIDCertification.PreviousMapping != nil || current.Formation.GTIDCertification.JoinOnly {
		t.Fatal("regression did not retain a true blocked birth owner", err)
	}
}

func TestPXCServingCurrentMixedMappingCannotBeAcknowledgedByQueuedFull(t *testing.T) {
	const direct = "11111111-2222-3333-4444-555555555555"
	const inverse = "eeeeeeee-dddd-cccc-bbbb-aaaaaaaaaaaa"
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	original := s.Formation.GTIDCertification
	if original == nil || original.Checkpoint.GTIDCertification == nil || !original.Checkpoint.GTIDCertification.Certified || original.RetiredCertification != nil {
		t.Fatal("fixture must begin with actual successful, consistent native certification")
	}
	condition, _ := pxcFormationMappingCondition(s)
	if condition.Status != "True" {
		t.Fatal("fixture did not start with certified mapping", condition)
	}
	events := &gtidEvents{}
	rc.Events = events
	proxyStops, probes := 0, 0
	prior := b.action
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if strings.Contains(r.URL.Path, "/processes/proxysql/stop") {
			proxyStops++
		}
		if r.URL.Path == "/v1/mysql/"+myspec.ActionGaleraHistoryProbe {
			probes++
		}
		return prior != nil && prior(w, r, name)
	}
	bad := s.data()[2].Name
	status := b.native[bad]
	if !strings.Contains(status.GTIDExecuted, direct+":") {
		t.Fatal("certified fixture lacks its actual direct-namespace probe history")
	}
	// Change the live agent observation, not persisted certificate evidence.
	// Peers retain direct current-state GTIDs; this member now executes those
	// same positions under the inverse current-state namespace.
	status.GTIDExecuted = strings.ReplaceAll(status.GTIDExecuted, direct, inverse)
	b.native[bad] = status
	for range 4 {
		p = New(Options{Provider: "pxc"})
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		b.apply(t, p, rc)
		s, err = load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		assertPXCHistoryClientsServing(t, p, rc, s)
		if activeGTIDIncident(s) == nil || s.Formation.GTIDCertification.Owner != original.Owner {
			t.Fatal("live mixed mapping did not latch or replaced serving admission authority")
		}
	}
	pxcHistoryStorage(t, rc, b)
	queuedPXCHistoryFull(t, rc, b, "reject-current-mixed")
	queued, err := rc.Store.GetOperation(t.Context(), "full-reject-current-mixed")
	if err != nil || queued.Step != "done-baseline-pending" || !strings.Contains(queued.Message, "baseline") {
		t.Fatal("successful native backup concealed rejected mixed-mapping acknowledgement", queued, err)
	}
	s, err = load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if activeGTIDIncident(s) == nil {
		t.Fatal("fresh full silently acknowledged current mixed namespaces using an old consistent certificate")
	}
	assertPXCHistoryClientsServing(t, p, rc, s)
	assertPXCHistoryLatchCondition(t, p, rc)
	observed, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	foundMapping := false
	for _, condition := range observed.Conditions {
		if condition.Type == "GTIDMappingCertified" {
			foundMapping = true
			if condition.Status == "True" {
				t.Fatal("historical first-Ready proof claimed current mixed mapping certified", condition)
			}
		}
	}
	if !foundMapping {
		t.Fatal("current mapping diagnostic disappeared")
	}
	for _, event := range events.events {
		if event.Reason == "GTIDHistoryBaselineEstablished" {
			t.Fatal("mixed current mapping emitted a successful baseline acknowledgement")
		}
	}
	if proxyStops != 0 || probes != 0 {
		t.Fatal("current mixed-mapping diagnostic stopped clients or re-entered certification", proxyStops, probes)
	}
}
