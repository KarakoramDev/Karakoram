// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
)

func prepareReviewRestore(t *testing.T, f *snapshotFixture) *domain.Operation {
	t.Helper()
	snapshotRun(t, f, &domain.Operation{Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"snap"}`)})
	f.store.restore = &domain.Restore{Name: "restore", Source: domain.RestoreSource{Backup: "snap"}, Phase: domain.PhasePending}
	op := &domain.Operation{Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"restore"}`)}
	for range 2 {
		result, _, err := f.p.runBackupOperation(t.Context(), f.rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
	}
	return op
}

func TestRestoreExpiredTokenBeforeUnappliedImport(t *testing.T) {
	f := newSnapshotFixture(t)
	op := prepareReviewRestore(t, f)
	if len(f.restorePolicies) != 1 || f.imports != 0 {
		t.Fatal("prepare must arm a marker before importing")
	}
	// Model expiry during an arbitrarily long pause at the persisted post step.
	// Nothing has imported; a 403 must recover access, not report success.
	f.agents.tokenValid = false
	f.p = New(ProviderOptions{})
	snapshotRun(t, f, op)
	if f.imports != 1 || f.store.restore.Phase != domain.PhaseSucceeded {
		t.Fatalf("expired token skipped import: imports=%d phase=%s", f.imports, f.store.restore.Phase)
	}
}

func TestRestoreLostAppliedPostResponseUsesSnapshotMarker(t *testing.T) {
	for _, invalidateToken := range []bool{false, true} {
		t.Run(map[bool]string{false: "token-survives", true: "token-invalid"}[invalidateToken], func(t *testing.T) {
			f := newSnapshotFixture(t)
			op := prepareReviewRestore(t, f)
			f.loseImportReply = true
			if _, _, err := f.p.runBackupOperation(t.Context(), f.rc, op); err == nil {
				t.Fatal("expected lost HTTP acknowledgement")
			}
			if f.imports != 1 || len(f.restorePolicies) != 0 {
				t.Fatal("snapshot did not actually replace barrier policy state")
			}
			f.agents.tokenValid = !invalidateToken
			// A post-apply change must not be erased by importing a second time.
			f.restorePolicies["post-apply-sentinel"] = true
			f.p = New(ProviderOptions{})
			snapshotRun(t, f, op)
			if f.imports != 1 || !f.restorePolicies["post-apply-sentinel"] || f.store.restore.Phase != domain.PhaseSucceeded {
				t.Fatalf("applied snapshot replayed: imports=%d policies=%v", f.imports, f.restorePolicies)
			}
		})
	}
}

func TestRestorePolicyInventoryFailureNeverProvesImport(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusNotFound, http.StatusServiceUnavailable} {
		f := newSnapshotFixture(t)
		op := prepareReviewRestore(t, f)
		f.policyListStatus = status
		result, _, err := f.p.runBackupOperation(t.Context(), f.rc, op)
		if err == nil || result.Step != "post" || f.imports != 0 {
			t.Fatalf("inventory status %d treated as evidence: result=%+v imports=%d err=%v", status, result, f.imports, err)
		}
	}
}

func TestRestorePrepareRecoversSealedFollowersBeforeForeignKeySwap(t *testing.T) {
	f := newSnapshotFixture(t)
	snapshotRun(t, f, &domain.Operation{Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"snap"}`)})
	s := state(t, f.rc)
	members := ordered(s)
	original, err := readUnseal(t.Context(), f.rc)
	if err != nil {
		t.Fatal(err)
	}
	source := domain.ClusterRef{Namespace: f.rc.Cluster.Ref().Namespace, Name: "deleted"}
	f.store.backup.Cluster = source.Name
	supplied := RestoreUnsealShares{Shares: []string{"foreign-one", "foreign-two", "foreign-three"}, Threshold: 3}
	raw, _ := json.Marshal(supplied)
	if err = putSecret(t.Context(), f.rc, RestoreUnsealSecret("restore"), raw); err != nil {
		t.Fatal(err)
	}
	for _, m := range members[1:] {
		f.agents.members[m.Name].sealed = true
		f.agents.members[m.Name].active = false
	}
	for i := 1; i < len(members); i++ {
		step, err := f.p.snapshotRestoreStep(t.Context(), f.rc, s, f.store.backup, source, "restore", "prepare")
		if err != nil || step != "prepare" {
			t.Fatalf("recovery pass %d: step=%s err=%v", i, step, err)
		}
		material, err := readUnseal(t.Context(), f.rc)
		if err != nil || material.Shares[0] != original.Shares[0] {
			t.Fatal("foreign material replaced current shares before pre-restore recovery")
		}
		unsealed := 0
		for _, m := range members[1:] {
			if !f.agents.members[m.Name].sealed {
				unsealed++
			}
		}
		if unsealed != i {
			t.Fatalf("expected serialized recovery: got %d recovered, want %d", unsealed, i)
		}
	}
	step, err := f.p.snapshotRestoreStep(t.Context(), f.rc, s, f.store.backup, source, "restore", "prepare")
	if err != nil || step != "post" {
		t.Fatalf("prepare did not advance after recovery: %s %v", step, err)
	}
	material, err := readUnseal(t.Context(), f.rc)
	if err != nil || material.Shares[0] != supplied.Shares[0] {
		t.Fatal("source material was not installed after recovering target members")
	}
}

func TestRestoreForeignExpiredTokenBeforeUnappliedImport(t *testing.T) {
	f := newSnapshotFixture(t)
	snapshotRun(t, f, &domain.Operation{Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"snap"}`)})
	source := domain.ClusterRef{Namespace: f.rc.Cluster.Ref().Namespace, Name: "deleted"}
	f.store.backup.Cluster = source.Name
	supplied := RestoreUnsealShares{Shares: []string{"foreign-one", "foreign-two", "foreign-three"}, Threshold: 3}
	f.sourceShares = supplied.Shares
	raw, _ := json.Marshal(supplied)
	if err := putSecret(t.Context(), f.rc, RestoreUnsealSecret("restore"), raw); err != nil {
		t.Fatal(err)
	}
	f.store.restore = &domain.Restore{Name: "restore", SourceNamespace: source.Namespace, Source: domain.RestoreSource{Backup: "snap", SourceCluster: source.Name}}
	op := &domain.Operation{Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"restore"}`)}
	for range 2 {
		result, _, err := f.p.runBackupOperation(t.Context(), f.rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
	}
	// SecretUnseal now holds source keys, but the live barrier still belongs
	// to the target. Generate-root must try retained target material after a
	// native wrong-key rejection; neither rejection nor expiry proves import.
	f.agents.tokenValid = false
	f.p = New(ProviderOptions{})
	snapshotRun(t, f, op)
	if f.imports != 1 || !f.force || f.store.restore.Phase != domain.PhaseSucceeded {
		t.Fatalf("foreign import skipped: imports=%d force=%v phase=%s", f.imports, f.force, f.store.restore.Phase)
	}
}

func TestRestoreLostMarkerWriteResponseDoesNotArmImport(t *testing.T) {
	f := newSnapshotFixture(t)
	snapshotRun(t, f, &domain.Operation{Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"snap"}`)})
	f.store.restore = &domain.Restore{Name: "restore", Source: domain.RestoreSource{Backup: "snap"}}
	op := &domain.Operation{Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"restore"}`)}
	result, _, err := f.p.runBackupOperation(t.Context(), f.rc, op)
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = result.Checkpoint
	f.loseMarkerReply = true
	if _, _, err = f.p.runBackupOperation(t.Context(), f.rc, op); err == nil {
		t.Fatal("expected lost marker write acknowledgement")
	}
	raw, err := secret(t.Context(), f.rc, RestoreUnsealSecret("restore")+"/marker")
	if err != nil {
		t.Fatal(err)
	}
	var marker snapshotRestoreMarker
	if err = json.Unmarshal(raw, &marker); err != nil || marker.Armed || f.imports != 0 || !f.restorePolicies[marker.Policy] {
		t.Fatalf("unacknowledged marker was armed: marker=%+v imports=%d err=%v", marker, f.imports, err)
	}
	// Losing the remote marker as well still cannot imply snapshot import:
	// the unarmed identity must be re-created and acknowledged first.
	delete(f.restorePolicies, marker.Policy)
	f.p = New(ProviderOptions{})
	snapshotRun(t, f, op)
	if f.imports != 1 || f.store.restore.Phase != domain.PhaseSucceeded {
		t.Fatalf("unarmed marker falsely proved restore: imports=%d", f.imports)
	}
}

// Unlike the broad engine fake, this handler uses the LIVE barrier threshold,
// not SecretUnseal (which already contains source material before import).
// Structurally valid foreign shares are accepted until the native threshold is
// reached; they do not immediately return 400 on the first submission.
// After force-import, seal-status deliberately keeps advertising target T=3
// while the restored barrier accepts the source threshold (2). Recovery must
// submit those source shares rather than reject them using stale seal metadata.
type restoreThresholdNative struct {
	f                   *snapshotFixture
	required            int
	sourceRequired      int
	rootKeys            []string
	unsealKeys          map[string][]string
	rootCancels         int
	unsealResets        int
	foreignBeforeImport int
	failRootCancelAt    int
	failUnsealReset     bool
	failUpdateStatus    int
	unsealUpdates       int
	failUpdateTransport bool
	requireRestart      bool
	cachedThreshold     map[string]int
	restarts            map[string]int
	loseReloadReply     bool
}

func installRestoreThresholdNative(t *testing.T, f *snapshotFixture) *restoreThresholdNative {
	t.Helper()
	n := &restoreThresholdNative{f: f, required: 3, sourceRequired: 2, unsealKeys: map[string][]string{}}
	for _, m := range ordered(state(t, f.rc)) {
		name := m.Name
		server := f.agents.nativeServers[name]
		next := server.Config.Handler
		server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/v1/sys/seal-status", "/v1/sys/unseal",
				"/v1/sys/generate-root/attempt", "/v1/sys/generate-root/update":
				f.agents.requestMu.Lock()
				defer f.agents.requestMu.Unlock()
				n.serve(name, w, r)
			default:
				if r.Method == http.MethodPost && r.URL.Path == "/v1/sys/storage/raft/snapshot-force" {
					defer func() {
						f.agents.requestMu.Lock()
						defer f.agents.requestMu.Unlock()
						if f.imports > 0 {
							n.required = n.sourceRequired
							clear(n.unsealKeys)
						}
					}()
				}
				next.ServeHTTP(w, r)
			}
		})
	}
	return n
}

func (n *restoreThresholdNative) serve(name string, w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/v1/sys/generate-root/") && (r.TLS == nil || len(r.TLS.VerifiedChains) == 0) {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	a := n.f.agents
	b := a.member(name)
	required := n.required
	if n.requireRestart {
		required = n.cachedThreshold[name]
	}
	send := func(v any) { _ = json.NewEncoder(w).Encode(v) }
	seal := func() {
		send(map[string]any{"initialized": true, "sealed": b.sealed, "t": 3, "n": len(a.shares), "progress": len(n.unsealKeys[name]), "version": b.version})
	}
	valid := func(keys []string) bool {
		for _, key := range keys {
			found := false
			for _, share := range a.shares {
				found = found || key == share
			}
			if !found {
				return false
			}
		}
		return true
	}
	submit := func(keys *[]string, key string) {
		if n.f.imports == 0 && strings.HasPrefix(key, "foreign-") {
			n.foreignBeforeImport++
		}
		for _, existing := range *keys {
			if existing == key {
				return
			}
		}
		*keys = append(*keys, key)
	}
	switch r.URL.Path {
	case "/v1/sys/seal-status":
		seal()
	case "/v1/sys/unseal":
		var req struct {
			Key   string `json:"key"`
			Reset bool   `json:"reset"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Reset {
			n.unsealResets++
			if n.failUnsealReset {
				http.Error(w, "reset unavailable", http.StatusServiceUnavailable)
				return
			}
			delete(n.unsealKeys, name)
			b.progress = 0
			seal()
			return
		}
		n.unsealUpdates++
		if n.failUpdateTransport {
			panic(http.ErrAbortHandler)
		}
		if n.failUpdateStatus != 0 {
			http.Error(w, "submission unavailable", n.failUpdateStatus)
			return
		}
		keys := n.unsealKeys[name]
		submit(&keys, req.Key)
		n.unsealKeys[name] = keys
		b.progress = len(keys)
		if len(keys) >= required {
			if !valid(keys) {
				http.Error(w, "invalid barrier key", http.StatusBadRequest)
				return
			}
			b.sealed = false
			b.active = !a.leaderExists()
			b.voter, b.healthy = true, true
			b.unseals += len(keys)
			b.progress = 0
			delete(n.unsealKeys, name)
		}
		seal()
	case "/v1/sys/generate-root/attempt":
		if r.Method == http.MethodDelete {
			n.rootCancels++
			if n.rootCancels == n.failRootCancelAt {
				http.Error(w, "cancel unavailable", http.StatusServiceUnavailable)
				return
			}
			n.rootKeys = nil
			w.WriteHeader(http.StatusNoContent)
			return
		}
		a.generated++
		a.rootOTP = "01234567890123456789012345678901"
		send(map[string]any{"nonce": "root-nonce", "otp": a.rootOTP, "required": n.required})
	case "/v1/sys/generate-root/update":
		var req struct {
			Key   string `json:"key"`
			Nonce string `json:"nonce"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Nonce != "root-nonce" {
			http.Error(w, "invalid nonce", http.StatusBadRequest)
			return
		}
		if n.failUpdateTransport {
			panic(http.ErrAbortHandler)
		}
		if n.failUpdateStatus != 0 {
			http.Error(w, "submission unavailable", n.failUpdateStatus)
			return
		}
		submit(&n.rootKeys, req.Key)
		if len(n.rootKeys) < n.required {
			send(map[string]any{"complete": false})
			return
		}
		if !valid(n.rootKeys) {
			http.Error(w, "invalid barrier key", http.StatusBadRequest)
			return
		}
		a.root = strings.Repeat("r", len(a.rootOTP))
		encoded := []byte(a.root)
		for i := range encoded {
			encoded[i] ^= a.rootOTP[i]
		}
		n.rootKeys = nil
		send(map[string]any{"complete": true, "encoded_token": base64.RawStdEncoding.EncodeToString(encoded)})
	}
}

func prepareThresholdReviewRestore(t *testing.T, f *snapshotFixture, sourceThreshold int) *domain.Operation {
	t.Helper()
	snapshotRun(t, f, &domain.Operation{Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"snap"}`)})
	source := domain.ClusterRef{Namespace: f.rc.Cluster.Ref().Namespace, Name: "deleted"}
	f.store.backup.Cluster = source.Name
	supplied := RestoreUnsealShares{Shares: []string{"foreign-one", "foreign-two", "foreign-three"}, Threshold: sourceThreshold}
	f.sourceShares = supplied.Shares
	raw, _ := json.Marshal(supplied)
	if err := putSecret(t.Context(), f.rc, RestoreUnsealSecret("restore"), raw); err != nil {
		t.Fatal(err)
	}
	f.store.restore = &domain.Restore{Name: "restore", SourceNamespace: source.Namespace, Source: domain.RestoreSource{Backup: "snap", SourceCluster: source.Name}}
	op := &domain.Operation{Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"restore"}`)}
	for range 2 {
		result, _, err := f.p.runBackupOperation(t.Context(), f.rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
	}
	if f.imports != 0 || len(f.restorePolicies) != 1 {
		t.Fatal("expected armed, unapplied foreign restore")
	}
	return op
}

func TestRestoreForeignDifferingThresholdPreImportRecovery(t *testing.T) {
	for _, scenario := range []string{"expired-token", "sealed-member", "expired-token-and-sealed-member"} {
		t.Run(scenario, func(t *testing.T) {
			f := newSnapshotFixture(t)
			n := installRestoreThresholdNative(t, f)
			op := prepareThresholdReviewRestore(t, f, 2) // source 2-of-3; target 3-of-5
			if strings.Contains(scenario, "expired-token") {
				f.agents.tokenValid = false
				n.rootKeys = []string{"foreign-partial"}
			}
			if strings.Contains(scenario, "sealed-member") {
				m := ordered(state(t, f.rc))[1]
				f.agents.members[m.Name].sealed = true
				f.agents.members[m.Name].active = false
				n.unsealKeys[m.Name] = []string{"foreign-partial"}
			}
			f.p = New(ProviderOptions{})
			snapshotRun(t, f, op)
			if f.imports != 1 || !f.force || f.store.restore.Phase != domain.PhaseSucceeded {
				t.Fatalf("foreign restore failed: imports=%d force=%v phase=%s", f.imports, f.force, f.store.restore.Phase)
			}
			expectedForeign := 0
			if strings.Contains(scenario, "sealed-member") {
				expectedForeign = 2 // Incomplete source attempt, reset before target shares.
			}
			if n.foreignBeforeImport != expectedForeign {
				t.Fatalf("submitted %d source shares before import, want %d", n.foreignBeforeImport, expectedForeign)
			}
			if strings.Contains(scenario, "sealed-member") && n.unsealResets < 2 {
				t.Fatal("existing and incomplete foreign unseal attempts were not both reset")
			}
			if strings.Contains(scenario, "expired-token") && n.rootCancels < 3 {
				t.Fatal("insufficient generate-root candidate was not cancelled before target recovery")
			}
		})
	}
}

func TestRestoreForeignRecoveryFailsClosedOnNativeErrors(t *testing.T) {
	for _, scenario := range []string{"root-cancel", "unseal-reset", "root-transport", "unseal-transport", "root-forbidden", "unseal-forbidden"} {
		t.Run(scenario, func(t *testing.T) {
			f := newSnapshotFixture(t)
			n := installRestoreThresholdNative(t, f)
			threshold := 3
			if scenario == "root-cancel" || scenario == "unseal-reset" {
				threshold = 2
			}
			op := prepareThresholdReviewRestore(t, f, threshold)
			root := strings.HasPrefix(scenario, "root-")
			if root {
				f.agents.tokenValid = false
			} else {
				m := ordered(state(t, f.rc))[1]
				f.agents.members[m.Name].sealed = true
				f.agents.members[m.Name].active = false
				n.unsealKeys[m.Name] = []string{"foreign-partial"}
			}
			switch scenario {
			case "root-cancel":
				n.failRootCancelAt = n.rootCancels + 2 // cancellation after insufficient attempt
			case "unseal-reset":
				n.failUnsealReset = true
			case "root-forbidden", "unseal-forbidden":
				n.failUpdateStatus = http.StatusForbidden
			default:
				n.failUpdateTransport = true
			}
			generated := f.agents.generated
			result, _, err := f.p.runBackupOperation(t.Context(), f.rc, op)
			if err == nil || result.Step != "post" || f.imports != 0 || len(f.restorePolicies) != 1 {
				t.Fatalf("native failure bypassed marker/import gate: result=%+v imports=%d err=%v", result, f.imports, err)
			}
			if root && f.agents.generated != generated+1 {
				t.Fatal("native failure incorrectly tried retained target root material")
			}
			if !root {
				expectedUpdates := 1
				if scenario == "unseal-reset" {
					expectedUpdates = 0
				}
				if n.unsealUpdates != expectedUpdates {
					t.Fatal("native failure retried unseal with retained target material")
				}
				for _, keys := range n.unsealKeys {
					for _, key := range keys {
						if !strings.HasPrefix(key, "foreign-") {
							t.Fatal("native failure incorrectly tried retained target unseal material")
						}
					}
				}
			}
		})
	}
}

func TestRestoreForeignRejectedSharesResetBeforeRetainedMaterial(t *testing.T) {
	f := newSnapshotFixture(t)
	n := installRestoreThresholdNative(t, f)
	n.sourceRequired = 3
	op := prepareThresholdReviewRestore(t, f, 3)
	f.agents.tokenValid = false
	m := ordered(state(t, f.rc))[1]
	f.agents.members[m.Name].sealed = true
	f.agents.members[m.Name].active = false
	snapshotRun(t, f, op)
	if f.imports != 1 || f.store.restore.Phase != domain.PhaseSucceeded || n.unsealResets == 0 {
		t.Fatalf("rejected source attempt poisoned target recovery: imports=%d phase=%s resets=%d", f.imports, f.store.restore.Phase, n.unsealResets)
	}
}

// Model the native process's cached seal configuration, which remains in use
// after a failed foreign-Shamir key upgrade until that process is restarted.
func (n *restoreThresholdNative) requireSealConfigRestart(t *testing.T) {
	t.Helper()
	n.requireRestart = true
	n.cachedThreshold = map[string]int{}
	n.restarts = map[string]int{}
	for _, m := range ordered(state(t, n.f.rc)) {
		name := m.Name
		n.cachedThreshold[name] = 3
		server := n.f.agents.servers[name]
		next := server.Config.Handler
		server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/v1/status", "/v1/openbao/status", "/v1/reload":
				n.f.agents.requestMu.Lock()
				defer n.f.agents.requestMu.Unlock()
				b := n.f.agents.members[name]
				switch r.URL.Path {
				case "/v1/status":
					_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: b.config, Processes: map[string]agentapi.ProcessStatus{"openbao": {State: "running"}}})
				case "/v1/openbao/status":
					_ = json.NewEncoder(w).Encode(baospec.StatusResponse{NodeID: name, Initialized: true, Sealed: b.sealed, Active: b.active, ProcessGeneration: b.generation})
				case "/v1/reload":
					cfg, err := n.f.p.MemberConfig(r.Context(), n.f.rc, name)
					if err != nil {
						http.Error(w, "config unavailable", http.StatusServiceUnavailable)
						return
					}
					var spec baospec.MemberSpec
					if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					if spec.ProcessGeneration != b.generation {
						if !b.sealed {
							t.Errorf("restore restarted an unsealed member %s", name)
						}
						for other := range n.restarts {
							if other != name && n.f.agents.members[other].sealed {
								t.Errorf("restore restarted %s before recovering %s", name, other)
							}
						}
						n.restarts[name]++
						b.generation = spec.ProcessGeneration
						n.cachedThreshold[name] = n.required
						delete(n.unsealKeys, name)
					}
					b.config = cfg.ConfigVersion
					if n.loseReloadReply {
						n.loseReloadReply = false
						panic(http.ErrAbortHandler)
					}
					_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
				}
			default:
				next.ServeHTTP(w, r)
			}
		})
	}
}

func TestRestoreForeignSealRefreshSurvivesCheckpointAndReloadReplyLoss(t *testing.T) {
	for _, lostImportReply := range []bool{false, true} {
		t.Run(map[bool]string{false: "acknowledged-import", true: "lost-applied-import-reply"}[lostImportReply], func(t *testing.T) {
			f := newSnapshotFixture(t)
			n := installRestoreThresholdNative(t, f)
			op := prepareThresholdReviewRestore(t, f, 2)
			n.requireSealConfigRestart(t)
			n.loseReloadReply = true
			f.loseImportReply = lostImportReply
			original := state(t, f.rc)
			initialGeneration := map[string]int64{}
			for _, m := range ordered(original) {
				initialGeneration[m.Name] = m.ProcessGeneration
			}
			lostReload := false
			lostImport := false
			lostConfigState := false
			for range 80 {
				// Lose controller-local objects at every durable boundary.
				f.p = New(ProviderOptions{})
				beforeInternal := append(json.RawMessage(nil), f.rc.Cluster.Internal...)
				result, _, err := f.p.runBackupOperation(t.Context(), f.rc, op)
				if err != nil {
					if lostImportReply && !lostImport && f.imports == 1 && len(n.restarts) == 0 {
						lostImport = true
						continue // Still at post: recovery must inspect marker after access returns.
					}
					if !lostReload && !n.loseReloadReply && len(n.restarts) > 0 {
						lostReload = true
						continue // Exact persisted generation must prevent a second restart.
					}
					t.Fatal(err)
				}
				if !lostConfigState && len(n.restarts) == 0 {
					for _, m := range ordered(state(t, f.rc)) {
						if m.ProcessGeneration > initialGeneration[m.Name] {
							// Crash after the durable secret plan but before cluster
							// config/checkpoint publication. Reuse the exact plan.
							f.rc.Cluster.Internal = beforeInternal
							lostConfigState = true
							break
						}
					}
					if lostConfigState {
						continue
					}
				}
				op.Checkpoint = result.Checkpoint
				if result.Done {
					break
				}
			}
			if !lostConfigState || !lostReload || (lostImportReply && !lostImport) {
				t.Fatal("expected lost config/checkpoints/acknowledgements were not exercised")
			}
			if f.imports != 1 || f.store.restore.Phase != domain.PhaseSucceeded {
				t.Fatalf("recovery replayed or skipped import: imports=%d phase=%s", f.imports, f.store.restore.Phase)
			}
			final := state(t, f.rc)
			for _, m := range ordered(final) {
				if n.restarts[m.Name] != 1 || m.ProcessGeneration != initialGeneration[m.Name]+1 || f.agents.members[m.Name].sealed {
					t.Fatalf("member %s refresh not idempotent: restarts=%d generation=%d sealed=%v", m.Name, n.restarts[m.Name], m.ProcessGeneration, f.agents.members[m.Name].sealed)
				}
			}
			if _, err := secret(t.Context(), f.rc, RestoreUnsealSecret("restore")+"/seal-refresh"); !missing(err) {
				t.Fatalf("completed restore retained process refresh staging: %v", err)
			}
		})
	}
}

func TestRestoreSealRefreshDoesNotRestartRecoveredMember(t *testing.T) {
	f := newSnapshotFixture(t)
	n := installRestoreThresholdNative(t, f)
	n.requireSealConfigRestart(t)
	s := state(t, f.rc)
	m := ordered(s)[1]
	initialGeneration := m.ProcessGeneration
	initialConfig := m.ConfigVersion
	f.agents.members[m.Name].sealed = true
	if busy, err := f.p.refreshSnapshotSeal(t.Context(), f.rc, s, "restore", m, true); !busy || err != nil {
		t.Fatalf("persist refresh: busy=%v err=%v", busy, err)
	}
	// Another operator recovers it before the new config is published.
	f.agents.members[m.Name].sealed = false
	if busy, err := f.p.refreshSnapshotSeal(t.Context(), f.rc, s, "restore", m, false); !busy || err != nil {
		t.Fatalf("cancel recovered refresh: busy=%v err=%v", busy, err)
	}
	if m.ProcessGeneration != initialGeneration || m.ConfigVersion != initialConfig || len(n.restarts) != 0 {
		t.Fatal("already recovered member was disrupted")
	}
	if busy, err := f.p.refreshSnapshotSeal(t.Context(), f.rc, s, "restore", m, false); busy || err != nil {
		t.Fatalf("completed refresh did not remain idempotent: busy=%v err=%v", busy, err)
	}
}
