// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// This same test fails on the detached v0.4.2 release, in both phases, with
// "Galera local peer and port required" from the real agent schema validator.
func TestPXCRetiringMemberConfigRemainsAgentValid(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	for _, phase := range []string{"drain", "leave"} {
		t.Run(phase, func(t *testing.T) {
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			retiring := s.data()[len(s.data())-1]
			retiring.Removing, retiring.Offline, retiring.Retirement = true, phase == "leave", phase
			publishRevision(s)
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			config, err := p.MemberConfig(t.Context(), rc, retiring.Name)
			if err != nil {
				t.Fatal(err)
			}
			var spec myspec.Spec
			if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
				t.Fatal(err)
			}
			if err := spec.Validate(); err != nil {
				t.Fatalf("retiring member %s phase=%s received an agent-rejected config: %v", retiring.Name, phase, err)
			}
		})
	}
}

type retirementConfigValidation struct {
	reloads  int
	removing map[string]int
	stopped  map[string]int
}

// Apply the exported CP rendering and full exported agent schema validator on
// every reload. Validation is not replaced by a test-specific peer-name rule.
func validateRetirementAgentReloads(t *testing.T, rc *engine.Context, b *fakeBackend) *retirementConfigValidation {
	t.Helper()
	seen := &retirementConfigValidation{removing: map[string]int{}, stopped: map[string]int{}}
	prior := b.action
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path == "/v1/reload" {
			p := New(Options{Provider: rc.Cluster.Spec.Provider})
			config, err := p.MemberConfig(r.Context(), rc, name)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return true
			}
			var spec myspec.Spec
			if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return true
			}
			if err := spec.Validate(); err != nil {
				t.Errorf("agent rejected actual MemberConfig for %s: %v", name, err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return true
			}
			current, err := load(rc.Cluster)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return true
			}
			seen.reloads++
			if member := current.find(name); member != nil && member.Removing {
				seen.removing[name]++
				if member.Offline {
					seen.stopped[name]++
				}
			}
			// Local schema identity is retained only in its own Galera image;
			// survivors' join lists and client routing still exclude retirement.
			for _, retiring := range current.data() {
				if !retiring.Removing {
					continue
				}
				if spec.Galera != nil && retiring.Name != name {
					for _, peer := range spec.Galera.Peers {
						if peer.Name == retiring.Name {
							t.Errorf("survivor %s still lists retiring Galera peer %s", name, retiring.Name)
						}
					}
				}
				if spec.Proxy != nil {
					for _, peer := range spec.Proxy.Backends {
						if peer.Name == retiring.Name {
							t.Errorf("proxy still routes to retiring member %s", retiring.Name)
						}
					}
				}
			}
		}
		return prior != nil && prior(w, r, name)
	}
	return seen
}

// A real agent validates a polled config before acknowledging its version.
// fakeBackend.apply alone would acknowledge an invalid spec and mask D095.
func applyAgentValidatedRetirementConfig(t *testing.T, p *Core, rc *engine.Context, b *fakeBackend) {
	t.Helper()
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range s.Members {
		if !b.deployed[member.Name] || b.services[member.Name] == nil {
			continue
		}
		agent, err := rc.Agents.Dial(t.Context(), rc.Cluster.Ref(), member.Name)
		if err != nil {
			t.Fatal(err)
		}
		if err := agent.Reload(t.Context()); err != nil {
			t.Fatalf("member %s stage=%s removing=%t offline=%t failed actual agent config validation: %v", member.Name, member.Retirement, member.Removing, member.Offline, err)
		}
	}
	b.apply(t, p, rc)
}

func TestPXCReplacementCompletesWithAgentValidatedRetirementConfigs(t *testing.T) {
	for _, actor := range []string{"api", "system"} {
		t.Run(actor, func(t *testing.T) {
			p, rc, b := setupPXC(t)
			converge(t, p, rc, b)
			old := b.leader
			seen := validateRetirementAgentReloads(t, rc, b)
			payload, err := json.Marshal(map[string]string{"member": old})
			if err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{ID: "replace-validated-retirement", Kind: domain.OperationReplaceMember, Actor: actor, Payload: payload}
			finished := false
			for range 240 {
				p = New(Options{Provider: "pxc"})
				result, err := p.RunOperation(t.Context(), rc, op)
				if err != nil {
					t.Fatal(err)
				}
				op.Checkpoint = result.Checkpoint
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
				t.Fatal("managed PXC replacement did not finish with real agent spec validation")
			}
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			if len(s.data()) != 3 || s.find(old) != nil || b.services[old] != nil || b.services[old+"v"] != nil {
				t.Fatal("replacement returned before old runtime/volume retirement completed")
			}
			if seen.reloads == 0 || seen.removing[old] == 0 || seen.stopped[old] == 0 {
				t.Fatal("regression did not validate the actual removing and stopped retiring agent images", seen)
			}
		})
	}
}

func TestScaleInCompletesWithAgentValidatedRetirementConfigs(t *testing.T) {
	for _, provider := range []string{"pxc", "mysql"} {
		t.Run(provider, func(t *testing.T) {
			var p *Core
			var rc *engine.Context
			var b *fakeBackend
			if provider == "pxc" {
				p, rc, b = setupPXC(t)
			} else {
				p, rc, b = setup(t, "group")
			}
			converge(t, p, rc, b)
			rc.Cluster.Generation++
			rc.Cluster.Spec.Components.Engine.Replicas = 5
			day2Ticks(t, rc, b, func(s *state) bool {
				if len(s.data()) != 5 || provider == "pxc" && pxcFormationMappingPending(s) {
					return false
				}
				for _, member := range s.data() {
					if member.Stage != "joined" {
						return false
					}
				}
				return true
			})
			seen := validateRetirementAgentReloads(t, rc, b)
			rc.Cluster.Generation++
			rc.Cluster.Spec.Components.Engine.Replicas = 3
			finished := false
			for range 160 {
				p = New(Options{Provider: provider})
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				applyAgentValidatedRetirementConfig(t, p, rc, b)
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				if len(s.data()) == 3 {
					finished = true
					break
				}
			}
			if !finished {
				t.Fatal("scale-in did not complete through real agent config validation")
			}
			for _, name := range []string{rc.Cluster.Name + "m4", rc.Cluster.Name + "m3"} {
				if b.services[name] != nil || b.services[name+"v"] != nil || seen.removing[name] == 0 || seen.stopped[name] == 0 {
					t.Fatal("scale-in skipped validated stopped-member retirement or left exact resources", name, seen)
				}
			}
		})
	}
}

func TestPXCInFlightLeaveUpgradeCompletesWithExistingReleasedAgents(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	rc.Cluster.Generation++
	rc.Cluster.Spec.Components.Engine.Replicas = 5
	day2Ticks(t, rc, b, func(s *state) bool {
		if len(s.data()) != 5 || pxcFormationMappingPending(s) {
			return false
		}
		for _, member := range s.data() {
			if member.Stage != "joined" {
				return false
			}
		}
		return true
	})
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	originalIDs := map[string]string{}
	for name, service := range b.services {
		originalIDs[name] = service.ID
	}
	nextIndex := s.NextIndex
	old := s.find(rc.Cluster.Name + "m4")
	old.Removing, old.Offline, old.Retirement = true, true, "leave"
	publishRevision(s)
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	native := b.native[old.Name]
	native.MemberState = "OFFLINE"
	b.native[old.Name] = native
	// Reproduce the durable released-CP checkpoint: mysqld stopped, but its
	// agent could never acknowledge the newer invalid peer-less image.
	if b.applied[old.Name] >= old.ConfigVersion {
		t.Fatal("upgrade fixture did not preserve an unacknowledged leave config")
	}
	prior := b.action
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path == "/v1/status" {
			pid := 0
			if b.native[name].MemberState == "ONLINE" {
				pid = 1
			}
			_ = json.NewEncoder(w).Encode(agentapi.Status{Agent: agentapi.AgentBuild{Version: "v0.4.2", Commit: "8a7c2cad"}, ConfigVersion: b.applied[name], Processes: map[string]agentapi.ProcessStatus{"mysqld": {PID: pid}}})
			return true
		}
		return prior != nil && prior(w, r, name)
	}
	seen := validateRetirementAgentReloads(t, rc, b)
	p = New(Options{Provider: "pxc"})
	// Control-plane-only cutover must deliver a valid stopped-member image
	// before the old agent can acknowledge leave and permit exact-ID deletion.
	applyAgentValidatedRetirementConfig(t, p, rc, b)
	rc.Cluster.Generation++
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	for range 160 {
		p = New(Options{Provider: "pxc"})
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		applyAgentValidatedRetirementConfig(t, p, rc, b)
		current, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if len(current.data()) != 3 {
			continue
		}
		if current.NextIndex != nextIndex || seen.stopped[old.Name] == 0 {
			t.Fatal("CP-only upgrade recreated members or skipped the existing stopped agent", seen)
		}
		for _, name := range []string{rc.Cluster.Name + "m4", rc.Cluster.Name + "m3"} {
			if b.services[name] != nil || b.services[name+"v"] != nil {
				t.Fatal("CP-only recovery stranded the retiring exact resources", name)
			}
		}
		for name, service := range b.services {
			if service.ID != originalIDs[name] {
				t.Fatal("CP-only recovery replaced an existing runtime or volume", name)
			}
		}
		return
	}
	t.Fatal("persisted released leave checkpoint did not recover with unchanged old agents")
}

func TestPXCFailedReplacementResumesValidatedRetirementWithoutRunningOperation(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	oldName := rc.Cluster.Name + "m1"
	payload, err := json.Marshal(map[string]string{"member": oldName})
	if err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "released-replacement-retries-exhausted", Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationReplaceMember, Phase: domain.PhaseRunning, Actor: "api", Payload: payload}
	var s *state
	joined := false
	for range 120 {
		p = New(Options{Provider: "pxc"})
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
		if s.Replacement != nil && s.Replacement.Stage == "retire" {
			next := s.find(s.Replacement.New)
			if next != nil && next.Stage == "joined" {
				joined = true
				break
			}
		}
	}
	if !joined {
		t.Fatal("fixture did not complete the real replacement's new-member join")
	}
	cursor := s.Replacement
	if cursor.New != rc.Cluster.Name+"m3" || cursor.Old != oldName {
		t.Fatal("fixture does not match the released old-m1/new-m3 replacement", cursor)
	}
	old := s.find(oldName)
	old.Removing, old.Offline, old.Retirement = true, true, "leave"
	cursor.Stage, cursor.Recovery = "retire", "complete"
	// v0.4.2 had no formation-certification owner or member SQL fences.
	// Preserve that released checkpoint instead of inventing a new admission.
	s.Formation.GTIDCertification = nil
	s.RestoreGTIDFence = ""
	for _, member := range s.data() {
		member.GaleraJoinFence, member.GaleraSSTDonor = false, ""
	}
	publishRevision(s)
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	native := b.native[oldName]
	native.MemberState = "OFFLINE"
	b.native[oldName] = native
	if b.applied[oldName] >= old.ConfigVersion {
		t.Fatal("fixture lost the stale stopped agent's unacknowledged revision")
	}
	stamp := now(rc)
	op.Phase, op.Failures, op.CompletedAt = domain.PhaseFailed, 20, &stamp
	op.Step, op.Checkpoint = "retire", replacementResult(cursor).Checkpoint
	op.LastError = "agent reload retries exhausted: Galera local peer and port required"
	if err := rc.Store.EnqueueOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	queued, err := rc.Store.GetOperation(t.Context(), op.ID)
	if err != nil || queued.Phase != domain.PhaseFailed || queued.Failures != 20 {
		t.Fatal("replacement failure was not durably terminal", queued, err)
	}
	beforeIDs := map[string]string{}
	for name, service := range b.services {
		beforeIDs[name] = service.ID
	}
	nextIndex, newName := s.NextIndex, cursor.New
	prior := b.action
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path == "/v1/status" {
			pid := 0
			if b.native[name].MemberState == "ONLINE" {
				pid = 1
			}
			_ = json.NewEncoder(w).Encode(agentapi.Status{Agent: agentapi.AgentBuild{Version: "v0.4.2", Commit: "8a7c2cad"}, ConfigVersion: b.applied[name], Processes: map[string]agentapi.ProcessStatus{"mysqld": {PID: pid}}})
			return true
		}
		return prior != nil && prior(w, r, name)
	}
	seen := validateRetirementAgentReloads(t, rc, b)
	if rc.Operation != nil {
		t.Fatal("terminal-recovery fixture unexpectedly has a Running operation")
	}
	for range 160 {
		// Only normal reconciliation resumes the durable recovery cursor.
		// The failed operation is never retried, reclaimed, or run again.
		p = New(Options{Provider: "pxc"})
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		applyAgentValidatedRetirementConfig(t, p, rc, b)
		current, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if current.Replacement != nil {
			continue
		}
		if current.LastReplacement != op.ID || current.NextIndex != nextIndex || len(current.data()) != 3 || current.find(newName) == nil {
			t.Fatal("background recovery lost its completion receipt or recreated replacement identities")
		}
		if current.find(oldName) != nil || b.services[oldName] != nil || b.services[oldName+"v"] != nil || seen.stopped[oldName] == 0 {
			t.Fatal("terminal queue failure stranded the validated stopped member's exact resources", seen)
		}
		for name, service := range b.services {
			if beforeIDs[name] != service.ID {
				t.Fatal("background retirement changed an existing survivor/new-member runtime or volume", name)
			}
		}
		queued, err := rc.Store.GetOperation(t.Context(), op.ID)
		if err != nil || queued.Phase != domain.PhaseFailed || queued.Failures != 20 || rc.Operation != nil {
			t.Fatal("background cleanup required or rewrote a Running queue operation", queued, err)
		}
		return
	}
	t.Fatal("normal Sync did not finish the failed replacement's durable complete-recovery cursor")
}

func TestPXCFailedIrreversibleReplacementRetainsRealJoinOwnerPastTimeout(t *testing.T) {
	for _, copiedLocalSID := range []bool{false, true} {
		name := "without-copied-local-SID"
		if copiedLocalSID {
			name = "with-copied-local-SID"
		}
		t.Run(name, func(t *testing.T) {
			testPXCFailedIrreversibleReplacementJoinOwner(t, copiedLocalSID, nil)
		})
	}
}

func testPXCFailedIrreversibleReplacementJoinOwner(t *testing.T, copiedLocalSID bool, afterRetirement func(*Core, *engine.Context, *fakeBackend, *state)) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	at := now(rc)
	rc.Clock = trafficRenewalClock{at: at}
	oldName := rc.Cluster.Name + "m1"
	native := b.native[oldName]
	oldLocalSID := native.ServerUUID
	if present, err := myspec.GTIDContains(native.GTIDExecuted, oldLocalSID+":2"); err != nil || present {
		t.Fatal("fixture requires an unused member-local SID interval :2", err)
	}
	if native.GTIDExecuted != "" {
		native.GTIDExecuted += ","
	}
	native.GTIDExecuted += native.ServerUUID + ":2"
	b.native[oldName] = native
	for range 3 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		b.apply(t, p, rc)
	}
	s, err := load(rc.Cluster)
	if err != nil || activeGTIDIncident(s) == nil {
		t.Fatal("fixture did not durably observe the old member's real local GTID divergence", err)
	}
	incidentAt := activeGTIDIncident(s).DetectedAt
	seen := validateRetirementAgentReloads(t, rc, b)
	payload, err := json.Marshal(map[string]string{"member": oldName})
	if err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "failed-replacement-real-join-owner", Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationReplaceMember, Phase: domain.PhaseRunning, Actor: "api", Payload: payload}
	joined := false
	for range 120 {
		p = New(Options{Provider: "pxc"})
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
		applyAgentValidatedRetirementConfig(t, p, rc, b)
		s, err = load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if cursor := s.Replacement; cursor != nil && cursor.Stage == "retire" {
			if next := s.find(cursor.New); next != nil && next.Stage == "joined" {
				joined = true
				break
			}
		}
	}
	if !joined {
		t.Fatal("actual replacement did not reach a native-joined, unproven new member")
	}
	cursor := s.Replacement
	newName, nextIndex := cursor.New, s.NextIndex
	if copiedLocalSID {
		copy := b.native[newName]
		if copy.GTIDExecuted != "" {
			copy.GTIDExecuted += ","
		}
		copy.GTIDExecuted += oldLocalSID + ":2"
		b.native[newName] = copy
	}
	owner := s.Formation.GTIDCertification
	if owner == nil || !owner.JoinOnly || !owner.EmptyMembers[newName] || !owner.FencedMembers[newName] ||
		owner.Checkpoint.GTIDCertification == nil || !owner.Checkpoint.GTIDCertification.Negotiated ||
		!owner.Checkpoint.GTIDCertification.FencesOwned || owner.Checkpoint.GTIDCertification.Certified ||
		!s.find(newName).GaleraJoinFence || !pxcJoinRoutingFenced(s, s.find(newName)) {
		t.Fatal("fixture lost the actual negotiated empty-join admission and its native/client controls")
	}
	ownerID := owner.Owner
	originalEvidenceCount := len(owner.Checkpoint.GTIDCertification.Evidence)
	originalEvidence, err := json.Marshal(owner.Checkpoint.GTIDCertification.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	beforeIDs := map[string]string{}
	for name, service := range b.services {
		beforeIDs[name] = service.ID
	}
	rc.Clock = trafficRenewalClock{at: at.Add(2 * pxcFormationAdmissionTimeout)}
	if err := formationMappingOwner(owner).block(freshMySQLReads(t.Context()), rc, s, &owner.Checkpoint, "PXC join admission made no native Synced/certification progress within five minutes"); err == nil {
		t.Fatal("fixture did not produce the real terminal blocked join-owner checkpoint")
	}
	if owner.Blocked == "" || !owner.Checkpoint.GTIDCertification.CleanupComplete || !pxcJoinRoutingFenced(s, s.find(newName)) {
		t.Fatal("real owner cleanup lost its blocked checkpoint or unproven-member route fence")
	}
	// Capture the live mixture, including its stale stopped seed/donor
	// reference even though terminal desired-fence cleanup normally clears it.
	s.Formation.Seed = oldName
	s.find(newName).GaleraJoinFence, s.find(newName).GaleraSSTDonor = true, oldName
	old := s.find(oldName)
	old.Removing, old.Offline, old.Retirement = true, true, "leave"
	cursor.Stage, cursor.Recovery = "retire", "complete"
	// Unlike the released-checkpoint regression above, retain the real
	// BLOCKED formation owner, its routing fence, admission and GTID evidence.
	publishRevision(s)
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	native = b.native[oldName]
	native.MemberState = "OFFLINE"
	b.native[oldName] = native
	stamp := now(rc)
	op.Phase, op.Failures, op.CompletedAt = domain.PhaseFailed, 20, &stamp
	op.Step, op.Checkpoint = "retire", replacementResult(cursor).Checkpoint
	if err := rc.Store.EnqueueOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	if b.applied[oldName] >= old.ConfigVersion || s.find(newName).GaleraSSTDonor != oldName {
		t.Fatal("fixture lost the stale stopped-agent revision or retiring SST donor")
	}
	if owner.Blocked == "" || now(rc).Sub(owner.LastProgressAt) <= pxcFormationAdmissionTimeout {
		t.Fatal("fixture requires both an expired progress deadline and an already blocked join owner")
	}
	for range 180 {
		p = New(Options{Provider: "pxc"})
		if rc.Operation != nil {
			t.Fatal("failed-operation recovery unexpectedly acquired a Running operation")
		}
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal("expired join admission starved irreversible old-member retirement", err)
		}
		applyAgentValidatedRetirementConfig(t, p, rc, b)
		s, err = load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		current := s.Formation.GTIDCertification
		if current == nil || current.Owner != ownerID || current.Checkpoint.GTIDCertification == nil {
			t.Fatal("background cleanup discarded or replaced the actual join-certification owner")
		}
		if s.Formation.Seed == oldName || s.Formation.Seed == newName || s.find(newName).GaleraSSTDonor == oldName {
			t.Fatal("irreversible retirement kept a stopped seed/donor or promoted the unproven joiner")
		}
		if activeGTIDIncident(s) == nil || !activeGTIDIncident(s).DetectedAt.Equal(incidentAt) {
			t.Fatal("background retirement acknowledged or erased the real GTID discontinuity")
		}
		if !current.Checkpoint.GTIDCertification.Certified {
			next := s.find(newName)
			if next == nil || !pxcJoinRoutingFenced(s, next) || !current.FencedMembers[newName] {
				t.Fatal("unproven new member became routable while completing irreversible retirement")
			}
			for _, member := range s.Members {
				if member.Role != "proxy" {
					continue
				}
				config, err := p.MemberConfig(t.Context(), rc, member.Name)
				if err != nil {
					t.Fatal(err)
				}
				var spec myspec.Spec
				if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
					t.Fatal(err)
				}
				for _, backend := range spec.Proxy.Backends {
					if backend.Name == newName {
						t.Fatal("proxy routed the native-joined member before mapping certification")
					}
				}
			}
		}
		if s.Replacement != nil || pxcFormationMappingPending(s) {
			continue
		}
		proof := current.Checkpoint.GTIDCertification
		if len(proof.Evidence) == 0 {
			t.Fatal("normal operation admission reopened without an actual native mapping probe")
		}
		if copiedLocalSID {
			if proof.Certified || current.Blocked == "" || !pxcJoinRoutingFenced(s, s.find(newName)) {
				t.Fatal("copied frozen local history was silently certified or routed")
			}
			contains, err := myspec.GTIDContains(b.native[newName].GTIDExecuted, oldLocalSID+":2")
			if err != nil || !contains {
				t.Fatal("background certification erased the copied local history", err)
			}
		} else if !proof.Certified || current.Blocked != "" {
			t.Fatal("consistent final membership did not finish its native mapping certification")
		}
		mapping := proof.Evidence[len(proof.Evidence)-1].Mapping
		if len(mapping) != len(s.data()) {
			t.Fatal("final-membership probe did not cover every retained member")
		}
		var sharedSID string
		for _, sid := range mapping {
			if sid == "" || sharedSID != "" && sid != sharedSID {
				t.Fatal("diagnostic recovery bypassed a genuinely mixed native mapping")
			}
			sharedSID = sid
		}
		if len(current.Checkpoint.GTIDCertification.Evidence) < originalEvidenceCount {
			t.Fatal("resuming certification erased previously collected native evidence")
		}
		if originalEvidenceCount > 0 {
			retained, err := json.Marshal(current.Checkpoint.GTIDCertification.Evidence[:originalEvidenceCount])
			if err != nil || string(retained) != string(originalEvidence) {
				t.Fatal("resuming certification rewrote previously collected native evidence", err)
			}
		}
		if s.LastReplacement != op.ID || s.NextIndex != nextIndex || s.find(oldName) != nil ||
			b.services[oldName] != nil || b.services[oldName+"v"] != nil || s.find(newName) == nil || seen.stopped[oldName] == 0 {
			t.Fatal("failed-operation recovery did not finish exact retirement and preserve its replacement")
		}
		for name, service := range b.services {
			if beforeIDs[name] != service.ID {
				t.Fatal("irreversible recovery recreated a survivor or the admitted replacement", name)
			}
		}
		queued, err := rc.Store.GetOperation(t.Context(), op.ID)
		if err != nil || queued.Phase != domain.PhaseFailed || queued.Failures != 20 {
			t.Fatal("native recovery rewrote or reclaimed the terminal queue record", queued, err)
		}
		assertPXCHistoryExitAdmission(t, p, rc)
		if err := p.ValidateBackup(t.Context(), rc, "full"); err != nil {
			t.Fatal("completed replacement still blocked explicit full acknowledgement", err)
		}
		assertPXCHistoryClientsServing(t, p, rc, s)
		if afterRetirement != nil {
			// Additional recovery exits start with the original incident still
			// active, before this fixture's full-backup acknowledgement.
			afterRetirement(p, rc, b, s)
			return
		}
		proofBeforeFull, err := json.Marshal(current)
		if err != nil {
			t.Fatal(err)
		}
		// The native backup fixture uses real wall time for its end receipt.
		// Admission timeout has already been exercised; return to wall time
		// before requiring the new full to start after the actual incident.
		rc.Clock = nil
		pxcHistoryStorage(t, rc, b)
		fullName := "after-failed-irreversible-replacement"
		queuedPXCHistoryFull(t, rc, b, fullName)
		s, err = load(rc.Cluster)
		if err != nil || activeGTIDIncident(s) != nil || len(s.GTIDIncidents) == 0 {
			t.Fatal("queued full could not acknowledge the final membership's frozen history delta", err)
		}
		proofAfterFull, err := json.Marshal(s.Formation.GTIDCertification)
		if err != nil || string(proofAfterFull) != string(proofBeforeFull) {
			t.Fatal("full acknowledgement altered certification, block, admission or probe evidence", err)
		}
		if copiedLocalSID && (!pxcJoinRoutingFenced(s, s.find(newName)) || s.Formation.GTIDCertification.Checkpoint.GTIDCertification.Certified || s.Formation.GTIDCertification.Blocked == "") {
			t.Fatal("full acknowledgement released the uncertified member's routing fence")
		}
		if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || !allowed {
			t.Fatal("history acknowledgement stranded the normal restore-operation exit", allowed, err)
		}
		restore := &domain.Restore{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "after-failed-replacement", Phase: domain.PhasePending, Source: domain.RestoreSource{Backup: fullName}}
		if err := rc.Store.CreateRestore(t.Context(), restore); err != nil {
			t.Fatal(err)
		}
		restoreOp := &domain.Operation{ID: "restore-after-failed-replacement", Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning, Actor: "api", Payload: json.RawMessage(`{"restore":"after-failed-replacement"}`)}
		if err := rc.Store.EnqueueOperation(t.Context(), restoreOp); err != nil {
			t.Fatal(err)
		}
		result, err := p.RunOperation(t.Context(), rc, restoreOp)
		if err != nil {
			t.Fatal("diagnostic join owner blocked a real trusted-source restore", err)
		}
		var restoreCheckpoint backupCheckpoint
		if err := json.Unmarshal(result.Checkpoint, &restoreCheckpoint); err != nil {
			t.Fatal(err)
		}
		if restoreCheckpoint.Step != "preflight" || restoreCheckpoint.Source == nil || restoreCheckpoint.Request == nil || restoreCheckpoint.StopBoundary != restoreStopUntouched {
			t.Fatal("restore exit did not enter its real source-resolved preflight")
		}
		// Deletion is independent of the queue and its diagnostic owner.
		if _, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{}); err != nil {
			t.Fatal("completed irreversible recovery still blocked explicit deletion", err)
		}
		deleting, err := load(rc.Cluster)
		if err != nil || deleting.Pending == nil || deleting.Pending.Action != "delete-runtime" || len(deleting.GTIDIncidents) == 0 {
			t.Fatal("explicit deletion did not advance while preserving historical evidence", err)
		}
		return
	}
	t.Fatal("failed irreversible replacement never completed retirement, certification and recovery admission")
}
