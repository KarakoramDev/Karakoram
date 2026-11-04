// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestPXCRestoreGTIDCertification(t *testing.T) {
	const direct = "d5aeb882-d447-11f1-96e2-f732d65208d9"
	const inverse = "2a51477d-2bb8-ee0e-691d-08cd29adf726"
	const historical = "e419e04e-d43f-11f1-835a-d377c0fbcfd8:1-227"
	for _, scenario := range []string{"direct", "inverse", "remedy", "persistent", "resume", "cancel", "cancel-unavailable"} {
		t.Run(scenario, func(t *testing.T) {
			p, rc, b := setupPXC(t)
			converge(t, p, rc, b)
			rc.Cluster.Spec.Backup.Storage = ""
			s, _ := load(rc.Cluster)
			seed, bad := s.Formation.Seed, s.data()[2].Name
			proxyHeld, proxyStarts := map[string]bool{}, map[string]int{}
			for _, member := range s.Members {
				if member.Role == "proxy" {
					proxyHeld[member.Name] = false
				}
			}
			if len(proxyHeld) == 0 {
				t.Fatal("fixture lacks a client proxy")
			}
			assertReleased := func() {
				for name, held := range proxyHeld {
					if held || proxyStarts[name] == 0 {
						t.Fatalf("terminal restore left durable proxy hold: %s held=%t starts=%d", name, held, proxyStarts[name])
					}
				}
			}
			histories, mapping := map[string]string{}, map[string]string{}
			for _, m := range s.data() {
				histories[m.Name], mapping[m.Name] = historical, direct
				if scenario == "inverse" {
					mapping[m.Name] = inverse
				}
			}
			if scenario == "remedy" || scenario == "persistent" || scenario == "resume" {
				mapping[bad] = inverse
			}
			sequence, probes, resets := int64(0), 0, 0
			receipts := map[string]myspec.GaleraStatusResponse{}
			prior := b.action
			native := func(name string) myspec.GaleraStatusResponse {
				current, _ := load(rc.Cluster)
				return myspec.GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, ClusterSize: 3, StateUUID: direct, LastCommitted: sequence, GTIDExecuted: histories[name], ServerUUID: b.native[name].ServerUUID, OfflineMode: current.RestoreGTIDFence != ""}
			}
			terminalFailure := false
			b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				if _, proxy := proxyHeld[name]; !proxy && terminalFailure && r.URL.Path == "/v1/reload" {
					http.Error(w, "data agent unavailable during terminal cleanup", http.StatusServiceUnavailable)
					return true
				}
				if _, proxy := proxyHeld[name]; proxy {
					if strings.HasPrefix(r.URL.Path, "/v1/processes/proxysql/") {
						if strings.HasSuffix(r.URL.Path, "/stop") {
							current, _ := load(rc.Cluster)
							if current.RestoreGTIDFence == "" {
								t.Fatal("proxy hold lacked durable restore authority")
							}
							proxyHeld[name] = true
						} else if strings.HasSuffix(r.URL.Path, "/start") {
							current, _ := load(rc.Cluster)
							if current.RestoreGTIDFence != "" {
								t.Fatal("proxy released before native client fence")
							}
							proxyHeld[name] = false
							proxyStarts[name]++
						}
						_ = json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
						return true
					}
					if r.URL.Path == "/v1/status" {
						pid := 1
						if proxyHeld[name] {
							pid = 0
						}
						_ = json.NewEncoder(w).Encode(agentapi.Status{MySQLRestoreCertification: true, ConfigVersion: b.applied[name], Processes: map[string]agentapi.ProcessStatus{"proxysql": {PID: pid}}})
						return true
					}
				}
				switch r.URL.Path {
				case "/v1/mysql/galera-status":
					_ = json.NewEncoder(w).Encode(native(name))
					return true
				case "/v1/mysql/galera-history-probe":
					var req myspec.GaleraHistoryProbeRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Fatal(err)
					}
					current, _ := load(rc.Cluster)
					if current.RestoreGTIDFence == "" || name != seed {
						t.Fatal("probe was not fenced on restored seed")
					}
					for proxy, held := range proxyHeld {
						if !held {
							t.Fatalf("probe admitted privileged proxy clients through %s", proxy)
						}
					}
					if saved, ok := receipts[req.Receipt]; ok {
						_ = json.NewEncoder(w).Encode(saved)
						return true
					}
					sequence++
					probes++
					for member := range histories {
						histories[member] += fmt.Sprintf(",%s:%d", mapping[member], sequence)
					}
					out := native(name)
					receipts[req.Receipt] = out
					_ = json.NewEncoder(w).Encode(out)
					return true
				case "/v1/mysql/galera-prepare-join":
					var req myspec.GaleraBootstrapRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Fatal(err)
					}
					current, _ := load(rc.Cluster)
					target := current.find(name)
					if name == seed || !target.Offline || target.GaleraSSTDonor != seed || req.FormationEpoch != current.Formation.FormationEpoch || current.RestoreGTIDFence == "" {
						t.Fatal("SST remedy lacked fenced seed authority")
					}
					resets++
					histories[name] = histories[seed]
					if scenario != "persistent" {
						mapping[name] = mapping[seed]
					}
					_ = json.NewEncoder(w).Encode(struct{}{})
					return true
				}
				return prior != nil && prior(w, r, name)
			}
			op := &domain.Operation{ID: "restore-certification", Kind: "initial-restore"}
			cp := backupCheckpoint{Step: "certify", Member: seed, Generation: 1, Request: &myspec.Restore{}}
			blocked, resumed := false, false
			for i := range 60 {
				b.apply(t, p, rc)
				result, err := p.runRestore(t.Context(), rc, op, cp)
				if err != nil {
					if scenario != "persistent" {
						t.Fatalf("phase %s: %v", cp.Step, err)
					}
					blocked = true
					break
				}
				if err := json.Unmarshal(result.Checkpoint, &cp); err != nil {
					t.Fatal(err)
				}
				s, _ = load(rc.Cluster)
				if cp.GTIDCertification != nil && cp.GTIDCertification.Phase == "probe" && !cp.GTIDCertification.FencesOwned {
					t.Fatal("certification began without durable client-fence ownership")
				}
				if strings.HasPrefix(scenario, "cancel") && cp.GTIDCertification != nil && cp.GTIDCertification.Phase == "probe" {
					if s.RestoreGTIDFence == "" {
						t.Fatal("certification did not fence clients")
					}
					terminalFailure = scenario == "cancel-unavailable"
					_, cleanupErr := p.CancelOperation(t.Context(), rc, op)
					if terminalFailure && cleanupErr == nil || !terminalFailure && cleanupErr != nil {
						t.Fatalf("unexpected terminal cleanup result: %v", cleanupErr)
					}
					s, _ = load(rc.Cluster)
					if s.RestoreGTIDFence != "" || s.RestoreGTIDBlocked == "" {
						t.Fatal("cancel did not release client fence and block destructive retry")
					}
					assertReleased()
					if terminalFailure {
						terminalFailure = false
						if _, err := p.CancelOperation(t.Context(), rc, op); err != nil {
							t.Fatal(err)
						}
						assertReleased()
					}
					return
				}
				if scenario == "resume" && !resumed && cp.GTIDCertification != nil && cp.GTIDCertification.Phase == "reset" {
					// Lose the caller's checkpoint and provider instance. Recovery must load
					// the durable stop/reset intent rather than count another attempt.
					p = New(Options{Provider: "pxc"})
					cp = backupCheckpoint{}
					resumed = true
				}
				if result.Done {
					break
				}
				if i == 59 {
					t.Fatalf("certification stalled at %+v", cp.GTIDCertification)
				}
			}
			s, _ = load(rc.Cluster)
			assertReleased()
			if scenario == "persistent" {
				if !blocked || resets != 2 || s.RestoreGTIDFence != "" || s.RestoreGTIDBlocked == "" {
					t.Fatalf("persistent mapping did not fail closed: blocked=%t resets=%d state=%+v", blocked, resets, s)
				}
				status, err := p.Status(t.Context(), rc)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, condition := range status.Conditions {
					found = found || condition.Type == "RestoreGTIDCertified" && condition.Status == "False"
				}
				if !found {
					t.Fatal("blocked certification condition missing")
				}
			} else {
				if !cp.GTIDCertification.Certified || s.RestoreGTIDFence != "" || s.RestoreGTIDBlocked != "" {
					t.Fatalf("successful certification did not release clients: %+v", cp.GTIDCertification)
				}
				if scenario == "remedy" || scenario == "resume" {
					if resets != 1 || probes != 2 || cp.GTIDCertification.Attempts[bad] != 1 || activeGTIDIncident(s) == nil {
						t.Fatalf("SST remedy lost evidence or duplicated attempt: resets=%d probes=%d cert=%+v", resets, probes, cp.GTIDCertification)
					}
				} else if resets != 0 || probes != 1 {
					t.Fatalf("consistent mapping unnecessarily remediated: resets=%d probes=%d", resets, probes)
				}
			}
		})
	}
}

func TestPXCRestoreFenceQuiescesManagedActors(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	storage := &domain.BackupStorage{Namespace: "default", Name: "archive", Type: "s3", EndpointURL: "https://objects.example", Bucket: "backups", Region: "region", Prefix: "prefix", VerifyTLS: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
	if err := rc.Store.(*store.Store).CreateBackupStorage(t.Context(), storage); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Backup.Storage, rc.Cluster.Spec.Backup.PITR.Enabled = storage.Name, true
	s, _ := load(rc.Cluster)
	s.RestoreGTIDFence = "restore-owner"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	for _, step := range []string{"reform", "certify", "activate"} {
		cp := backupCheckpoint{Step: step, Generation: 3}
		bs, _ := loadBackupState(rc.Cluster)
		bs.Generation = 3 // Even matching activation generation cannot bypass fence.
		bs.RestoreOperation = &domain.Operation{ID: "restore-owner", Checkpoint: backupResult(cp, false).Checkpoint}
		if err := saveBackupState(rc.Cluster, bs); err != nil {
			t.Fatal(err)
		}
		if err := accessFence(t.Context(), rc); err == nil {
			t.Fatalf("%s admitted account writes during restore", step)
		}
		for _, member := range s.Members {
			config, err := p.MemberConfig(t.Context(), rc, member.Name)
			if err != nil {
				t.Fatal(err)
			}
			var spec myspec.Spec
			if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
				t.Fatal(err)
			}
			if member.Role == "data" && (spec.Backup == nil || spec.Backup.Binlog.Enabled || spec.Galera == nil || !spec.Galera.RestoreFence) {
				t.Fatalf("%s allowed privileged archival or omitted native fence", step)
			}
			if member.Role == "proxy" && len(config.DesiredProcesses) != 0 {
				t.Fatalf("%s allowed privileged proxy clients while native fence is active", step)
			}
		}
	}
	// Native proxy admin intentionally remains privileged; it must be stopped,
	// not silently demoted as part of restore.
	users, err := p.proxyUsers(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if len(users) == 0 || users[0].Username != "admin" {
		t.Fatal("managed proxy administrator was silently removed")
	}
}

func TestPXCRestoreProxyReleaseRetriesBeforeCertification(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	held := map[string]bool{}
	first := ""
	for _, m := range s.Members {
		if m.Role == "proxy" {
			held[m.Name] = false
			if first == "" {
				first = m.Name
			}
		}
	}
	if first == "" {
		t.Fatal("fixture lacks a proxy")
	}
	failRelease := true
	prior := b.action
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if _, proxy := held[name]; proxy {
			if strings.HasPrefix(r.URL.Path, "/v1/processes/proxysql/") {
				if strings.HasSuffix(r.URL.Path, "/stop") {
					held[name] = true
				}
				if strings.HasSuffix(r.URL.Path, "/start") {
					if failRelease && name == first {
						http.Error(w, "proxy agent unavailable", http.StatusServiceUnavailable)
						return true
					}
					held[name] = false
				}
				_ = json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
				return true
			}
			if r.URL.Path == "/v1/status" {
				pid := 1
				if held[name] {
					pid = 0
				}
				_ = json.NewEncoder(w).Encode(agentapi.Status{MySQLRestoreCertification: true, ConfigVersion: b.applied[name], Processes: map[string]agentapi.ProcessStatus{"proxysql": {PID: pid}}})
				return true
			}
		}
		return prior != nil && prior(w, r, name)
	}
	op := &domain.Operation{ID: "restore-before-certification", Kind: "initial-restore"}
	cp := backupCheckpoint{Step: "reform", Request: &myspec.Restore{}}
	if err := persistRestore(rc, op, cp); err != nil {
		t.Fatal(err)
	}
	if done, err := fencePXCRestoreClients(t.Context(), rc, s, op.ID); err != nil || !done {
		t.Fatalf("cannot establish durable proxy hold: %v", err)
	}
	if _, err := p.CancelOperation(t.Context(), rc, op); err == nil {
		t.Fatal("unavailable proxy did not retain a cleanup error")
	}
	current, _ := load(rc.Cluster)
	if current.RestoreGTIDFence != "" || !held[first] {
		t.Fatal("fixture did not model partial release after clearing desired fence")
	}
	failRelease = false
	p = New(Options{Provider: "pxc"})
	if _, err := p.CancelOperation(t.Context(), rc, op); err != nil {
		t.Fatal(err)
	}
	for name, stopped := range held {
		if stopped {
			t.Fatalf("CP reconstruction lost pre-certification proxy release ownership: %s", name)
		}
	}
}
