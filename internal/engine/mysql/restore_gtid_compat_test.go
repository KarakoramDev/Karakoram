// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestPXCLegacyRestoreNegotiatesBeforeClientHolds(t *testing.T) {
	const direct = "d5aeb882-d447-11f1-96e2-f732d65208d9"
	const inverse = "2a51477d-2bb8-ee0e-691d-08cd29adf726"
	const historical = "e419e04e-d43f-11f1-835a-d377c0fbcfd8:1-227"
	for _, scenario := range []string{"old-data", "old-proxy-mixed", "old-data-resume", "old-data-resume-stopped"} {
		t.Run(scenario, func(t *testing.T) {
			p, rc, b := setupPXC(t)
			converge(t, p, rc, b)
			s, _ := load(rc.Cluster)
			old := s.data()[0].Name
			if scenario == "old-proxy-mixed" {
				old = ""
				for _, m := range s.Members {
					if m.Role == "proxy" {
						old = m.Name
						break
					}
				}
				if old == "" {
					t.Fatal("fixture lacks proxy")
				}
			}
			bad := s.data()[2].Name
			holds, probes, releases := 0, 0, 0
			resuming := strings.HasPrefix(scenario, "old-data-resume")
			stopped := scenario == "old-data-resume-stopped"
			prior := b.action
			b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				if r.URL.Path == "/v1/status" {
					pid := 1
					if stopped && name == old {
						pid = 0
					}
					_ = json.NewEncoder(w).Encode(agentapi.Status{Agent: b.agentBuild, MySQLRestoreCertification: name != old, ConfigVersion: b.applied[name], Processes: map[string]agentapi.ProcessStatus{"mysqld": {PID: pid}, "proxysql": {PID: 1}}})
					return true
				}
				if strings.HasPrefix(r.URL.Path, "/v1/processes/proxysql/") {
					if resuming && strings.HasSuffix(r.URL.Path, "/start") {
						releases++
						_ = json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
						return true
					}
					holds++
					http.Error(w, "legacy restore must not acquire proxy holds", http.StatusInternalServerError)
					return true
				}
				if r.URL.Path == "/v1/mysql/galera-history-probe" {
					probes++
					http.Error(w, "old endpoint absent", http.StatusNotFound)
					return true
				}
				if strings.HasPrefix(r.URL.Path, "/v1/processes/mysqld/start") && name == old {
					stopped = false
					_ = json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
					return true
				}
				if r.URL.Path == "/v1/mysql/galera-status" {
					if stopped && name == old {
						http.Error(w, "mysqld stopped", http.StatusServiceUnavailable)
						return true
					}
					set := direct + ":1-366," + historical
					if scenario == "old-proxy-mixed" && name == bad {
						set = inverse + ":1-368," + historical
					}
					uuid := b.native[name].ServerUUID
					if name == old {
						uuid = ""
					}
					_ = json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, ClusterSize: 3, StateUUID: direct, LastCommitted: 377, GTIDExecuted: set, ServerUUID: uuid})
					return true
				}
				return prior != nil && prior(w, r, name)
			}
			op := &domain.Operation{ID: "legacy-restore", Kind: "initial-restore"}
			cp := backupCheckpoint{Step: "reform", Member: s.Formation.Seed, Generation: 1, Request: &myspec.Restore{}}
			if resuming {
				s.RestoreGTIDFence = op.ID
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				cp.Step, cp.GTIDCertification = "certify", &pxcRestoreCertification{Phase: "observe"}
				if stopped {
					cp.Step = "reform"
				}
			}
			done := false
			for range 10 {
				b.apply(t, p, rc)
				result, err := p.runRestore(t.Context(), rc, op, cp)
				if err != nil {
					t.Fatal(err)
				}
				cp = backupCheckpoint{}
				if err := json.Unmarshal(result.Checkpoint, &cp); err != nil {
					t.Fatal(err)
				}
				s, _ = load(rc.Cluster)
				if s.RestoreGTIDFence != "" || holds != 0 || probes != 0 {
					t.Fatal("unsupported agents acquired a certification fence or probe")
				}
				if result.Done {
					done = true
					break
				}
			}
			if resuming && releases == 0 {
				t.Fatal("legacy migration did not release preexisting certification holds")
			}
			if !done || !legacyPXCRestore(cp) || cp.GTIDCertification.Certified {
				t.Fatalf("legacy restore did not complete compatibly: %+v", cp)
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			unavailable := false
			for _, condition := range status.Conditions {
				unavailable = unavailable || condition.Reason == "RestoreCertificationUnavailable" && strings.Contains(condition.Message, "Re-create the cluster")
			}
			if !unavailable {
				t.Fatal("outdated-agent certification condition missing")
			}
			s, _ = load(rc.Cluster)
			if scenario == "old-proxy-mixed" {
				if activeGTIDIncident(s) == nil {
					t.Fatal("legacy compatibility bypassed real mixed-accounting divergence")
				}
				start := activeGTIDIncident(s).DetectedAt.Add(time.Second)
				automatic := &domain.Backup{Name: "post-restore-full", Trigger: "restore", Type: "full", Phase: domain.PhaseSucceeded, StartedAt: &start, MySQL: &domain.MySQLBackup{EndTime: start.Add(time.Second)}}
				if err := establishGTIDBaseline(t.Context(), rc, s, automatic); err == nil {
					t.Fatal("uncertified automatic full silently acknowledged real divergence")
				}
				if activeGTIDIncident(s) == nil {
					t.Fatal("legacy automatic full cleared latch")
				}
			}
		})
	}
}

func TestPXCRestoreCapabilityTransportFailureAcquiresNoHolds(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	failed := s.data()[0].Name
	mutations := 0
	prior := b.action
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path == "/v1/status" && name == failed {
			http.Error(w, "agent unavailable", http.StatusServiceUnavailable)
			return true
		}
		if strings.HasPrefix(r.URL.Path, "/v1/processes/") {
			mutations++
		}
		return prior != nil && prior(w, r, name)
	}
	_, err := p.runRestore(t.Context(), rc, &domain.Operation{ID: "capability-unavailable", Kind: "initial-restore"}, backupCheckpoint{Step: "reform", Request: &myspec.Restore{}})
	current, _ := load(rc.Cluster)
	if err == nil || mutations != 0 || current.RestoreGTIDFence != "" || current.RestoreGTIDUnavailable != "" {
		t.Fatal("transport failure was mistaken for legacy support or acquired holds")
	}
}

type unavailableHistoryReads struct{ engine.SecretStore }

func (s unavailableHistoryReads) GetClusterSecret(ctx context.Context, ref domain.ClusterRef, key string) ([]byte, error) {
	if key == "mysql/gtid/history" {
		return nil, engine.ErrUnavailable
	}
	return s.SecretStore.GetClusterSecret(ctx, ref, key)
}

func TestPXCRestoreBlockedRecoveryRetriesTerminalCleanup(t *testing.T) {
	for _, checkpoint := range []string{"certify-cleanup", "certify-blocked"} {
		t.Run(checkpoint, func(t *testing.T) {
			p, rc, b := setupPXC(t)
			converge(t, p, rc, b)
			s, _ := load(rc.Cluster)
			op := &domain.Operation{ID: "blocked-initial-restore", Kind: "initial-restore"}
			s.RestoreGTIDFence, s.RestoreGTIDBlocked = op.ID, "PXC restore certification blocked; preserve trusted history"
			s.GTIDIncidents = []gtidIncident{{DetectedAt: time.Now(), Audited: true}}
			if err := persistGTIDHistory(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			initialBackup, _ := loadBackupState(rc.Cluster)
			cp := backupCheckpoint{Step: checkpoint, Generation: initialBackup.Generation + 1, Request: &myspec.Restore{}, GTIDCertification: &pxcRestoreCertification{Phase: "blocked", Negotiated: true}}
			if checkpoint == "certify-cleanup" {
				cp.GTIDCertification.CleanupPending = true
				cp.GTIDCertification.Phase = "cleanup"
			}
			if checkpoint == "certify-blocked" {
				cp.Request = nil
			} // Cleanup needs ownership, not source metadata.
			if err := persistRestore(rc, op, cp); err != nil {
				t.Fatal(err)
			}
			held := map[string]bool{}
			first := ""
			for _, m := range s.Members {
				if m.Role == "proxy" {
					held[m.Name] = true
					if first == "" {
						first = m.Name
					}
				}
			}
			if first == "" {
				t.Fatal("fixture lacks proxy")
			}
			unreachable, starts := true, 0
			prior := b.action
			b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				if r.URL.Path == "/v1/reload" && name == first && unreachable {
					http.Error(w, "proxy unreachable", http.StatusServiceUnavailable)
					return true
				}
				if strings.HasPrefix(r.URL.Path, "/v1/processes/proxysql/start") {
					held[name] = false
					starts++
					_ = json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
					return true
				}
				return prior != nil && prior(w, r, name)
			}
			if _, err := p.runRestore(t.Context(), rc, op, cp); err == nil {
				t.Fatal("unreachable release did not retain cleanup failure")
			}
			bs, _ := loadBackupState(rc.Cluster)
			cp = backupCheckpoint{}
			if err := json.Unmarshal(bs.RestoreOperation.Checkpoint, &cp); err != nil {
				t.Fatal(err)
			}
			if cp.Step != "certify-cleanup" || !cp.GTIDCertification.CleanupPending || cp.GTIDCertification.CleanupComplete || !held[first] {
				t.Fatal("terminal cleanup pending was not durable")
			}
			// No CancelOperation call: this is the initial-restore Sync recovery path.
			// Even an unavailable GTID journal must not gate release of client holds.
			unreachable = false
			originalSecrets := rc.Secrets
			rc.Secrets = unavailableHistoryReads{SecretStore: originalSecrets}
			p = New(Options{Provider: "pxc"})
			_, syncErr := p.Sync(t.Context(), rc)
			if syncErr == nil {
				t.Fatal("blocked restore unexpectedly activated")
			}
			rc.Secrets = originalSecrets
			bs, _ = loadBackupState(rc.Cluster)
			cp = backupCheckpoint{}
			if err := json.Unmarshal(bs.RestoreOperation.Checkpoint, &cp); err != nil {
				t.Fatal(err)
			}
			if cp.Step != "certify-blocked" || cp.GTIDCertification.CleanupPending || !cp.GTIDCertification.CleanupComplete || bs.Generation != initialBackup.Generation {
				t.Fatalf("recovered cleanup did not settle blocked: checkpoint=%+v certification=%+v generation=%d starts=%d held=%v syncError=%v", cp, cp.GTIDCertification, bs.Generation, starts, held, syncErr)
			}
			for name, stopped := range held {
				if stopped {
					t.Fatalf("reachable proxy remains held after Sync recovery: %s", name)
				}
			}
			s, _ = load(rc.Cluster)
			if err := loadGTIDJournal(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			if s.RestoreGTIDFence != "" || s.RestoreGTIDBlocked == "" || activeGTIDIncident(s) == nil {
				t.Fatal("cleanup erased discontinuity evidence or retained native fence")
			}
			released := starts
			stale := backupCheckpoint{Step: "certify-cleanup", GTIDCertification: &pxcRestoreCertification{CleanupPending: true}}
			if _, err := p.runRestore(t.Context(), rc, op, stale); err == nil {
				t.Fatal("stale caller checkpoint activated blocked restore")
			}
			if starts != released {
				t.Fatal("JSON snapshot reload retained stale caller cleanup flags")
			}
			if _, err := p.Sync(t.Context(), rc); err == nil {
				t.Fatal("settled blocked restore unexpectedly activated")
			}
			if starts != released {
				t.Fatal("settled cleanup kept restarting proxies")
			}
		})
	}
}

func TestPXCRestoreNegotiationFailurePreservesLegacyRecovery(t *testing.T) {
	for _, step := range []string{"preflight-poll", "poll"} {
		t.Run(step, func(t *testing.T) {
			p, rc, b := setupPXC(t)
			converge(t, p, rc, b)
			s, _ := load(rc.Cluster)
			cleanupAt := time.Now()
			op := &domain.Operation{ID: "negotiated-before-native-failure", Kind: domain.OperationRestore, FailureCleanupStartedAt: &cleanupAt}
			cp := backupCheckpoint{
				Step: step, Member: s.Formation.Seed, Generation: 1,
				Source:  &restoreSource{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Backup: "frozen-full"},
				Request: &myspec.Restore{BackupID: "frozen-full", NewGroupName: "aaaaaaaabbbb4ccc8dddeeeeeeeeeeee", ObjectPaths: []string{"frozen/object"}},
			}
			if step == "poll" {
				for _, m := range s.data() {
					m.Offline = true // Original restore-owned mysqld stop holds.
					native := b.native[m.Name]
					native.MemberState = "OFFLINE"
					b.native[m.Name] = native
				}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
			}
			if err := negotiatePXCRestoreCertification(t.Context(), rc, s, op, &cp); err != nil {
				t.Fatal(err)
			}
			if !cp.GTIDCertification.Negotiated || cp.GTIDCertification.Legacy || cp.GTIDCertification.FencesOwned || s.RestoreGTIDFence != "" {
				t.Fatal("successful negotiation incorrectly acquired client controls")
			}
			if step == "poll" {
				// Older negotiation checkpoints used this phase before any
				// certification controls were acquired. Phase alone owns nothing.
				cp.GTIDCertification.Phase = "fence"
				if err := persistRestore(rc, op, cp); err != nil {
					t.Fatal(err)
				}
			}
			before, _ := loadBackupState(rc.Cluster)
			frozen := before.RestoreOperation.Checkpoint
			rejected, mutations, polls := true, 0, 0
			prior := b.action
			b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				if strings.HasPrefix(r.URL.Path, "/v1/processes/") {
					mutations++
				}
				if r.URL.Path == "/v1/mysql/"+myspec.ActionRestoreStatus {
					polls++
					status := myspec.RestoreStatus{State: "running"}
					if rejected {
						status.State, status.Error = "failed", "native request rejected"
					}
					_ = json.NewEncoder(w).Encode(status)
					return true
				}
				return prior != nil && prior(w, r, name)
			}
			if _, err := p.runRestore(t.Context(), rc, op, cp); err == nil {
				t.Fatal("native rejection did not fail its checkpoint")
			}
			result, err := p.CancelOperation(t.Context(), rc, op)
			if err != nil || !result.Done {
				t.Fatalf("native failure cleanup did not complete: %+v %v", result, err)
			}
			current, _ := load(rc.Cluster)
			backup, _ := loadBackupState(rc.Cluster)
			if mutations != 0 || current.RestoreGTIDFence != "" || current.RestoreGTIDBlocked != "" {
				t.Fatal("negotiation-only failure acquired controls or invented a certification failure")
			}
			if step == "preflight-poll" {
				if result.Step != "restore-preflight-aborted" || backup.RestoreOperation != nil {
					t.Fatal("preflight rejection retained restore ownership")
				}
				for _, m := range current.data() {
					if m.Offline {
						t.Fatal("preflight rejection fenced serving data")
					}
				}
				if err := persistRestore(rc, &domain.Operation{ID: "fresh-restore", Kind: domain.OperationRestore}, backupCheckpoint{Step: "preflight", Request: &myspec.Restore{}}); err != nil {
					t.Fatalf("preflight abort blocked a fresh restore: %v", err)
				}
			} else {
				if result.Step != "restore-recovery" || backup.RestoreOperation == nil || !bytes.Equal(frozen, backup.RestoreOperation.Checkpoint) {
					t.Fatal("native polling failure replaced its frozen recovery checkpoint")
				}
				for _, m := range current.data() {
					if !m.Offline {
						t.Fatal("native polling failure released original destructive restore holds")
					}
				}
				rejected = false
				if busy, err := p.reconcileRestoreRecovery(t.Context(), rc, current); !busy || err != nil {
					t.Fatalf("native restore polling could not resume: busy=%t err=%v", busy, err)
				}
				if polls != 2 || mutations != 0 {
					t.Fatal("resumption abandoned native polling or acquired certification controls")
				}
			}
		})
	}
}
