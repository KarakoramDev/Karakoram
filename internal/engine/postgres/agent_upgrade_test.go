// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

type rolloutBackend struct {
	*fakeBackend
	rc        *engine.Context
	names     []string
	holdBuild bool
}

func (b *rolloutBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	b.names = append(b.names, name)
	if name == "demop0" && b.rc.Cluster.Spec.Topology.Type == "ha" && (b.leader == "" || b.leader == name) {
		return infra.Op{}, engine.Invalid("primary was not switched away")
	}
	op, err := b.fakeBackend.Deploy(ctx, name, d)
	if err == nil && !b.holdBuild {
		s, _ := load(b.rc.Cluster)
		b.genericOverrides[name] = agentapi.Status{ConfigVersion: s.find(name).ConfigVersion, Agent: agentapi.AgentBuild{Build: engine.CurrentAgentBuild().Build}}
	}
	return op, err
}
func rolloutOperation(include bool) *domain.Operation {
	raw, _ := json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, TargetVersion: engine.CurrentAgentBuild().Version, IncludeCurrent: include})
	return &domain.Operation{ID: "agents", Kind: domain.OperationUpgradeAgents, Payload: raw}
}
func TestAgentRolloutOrderSkipAndCrashResume(t *testing.T) {
	for _, include := range []bool{false, true} {
		t.Run(map[bool]string{false: "skip", true: "include"}[include], func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			s := converge(t, p, rc)
			b.genericOverrides = map[string]agentapi.Status{}
			for _, m := range s.Members {
				b.genericOverrides[m.Name] = agentapi.Status{ConfigVersion: m.ConfigVersion, Agent: agentapi.AgentBuild{Build: "old"}}
			}
			current := b.genericOverrides["demop1"]
			current.Agent.Build = engine.CurrentAgentBuild().Build
			b.genericOverrides["demop1"] = current
			wrapped := &rolloutBackend{fakeBackend: b, rc: rc}
			rc.Backend = wrapped
			before, _ := json.Marshal(s.Members)
			op := rolloutOperation(include)
			done := false
			for range 80 {
				p = New(p.options)
				r, e := p.RunOperation(t.Context(), rc, op)
				if e != nil {
					t.Fatal(e)
				}
				if r.Checkpoint != nil {
					op.Checkpoint = r.Checkpoint
				}
				if r.Done {
					done = true
					break
				}
			}
			if !done {
				t.Fatal("rollout did not finish")
			}
			want := []string{"demop2", "demopx", "demop0"}
			if include {
				want = []string{"demop1", "demop2", "demopx", "demop0"}
			}
			if !reflect.DeepEqual(wrapped.names, want) {
				t.Fatalf("deployment order %v, want %v", wrapped.names, want)
			}
			after, _ := load(rc.Cluster)
			afterRaw, _ := json.Marshal(after.Members)
			if string(before) != string(afterRaw) {
				t.Fatal("agent rollout changed applied member state")
			}
		})
	}
}
func TestAgentRolloutBuildFenceAndCancellation(t *testing.T) {
	p, rc, b := setup(t, "single")
	s := converge(t, p, rc)
	b.actions = nil
	bad := rolloutOperation(true)
	bad.Payload = json.RawMessage(`{"targetBuild":"different"}`)
	if _, e := p.RunOperation(t.Context(), rc, bad); e == nil || !strings.Contains(e.Error(), "differs from running") || !strings.Contains(e.Error(), "re-run upgrade-agents") {
		t.Fatal("missing build fence", e)
	}
	if len(b.actions) != 0 {
		t.Fatal("mismatch disrupted a member")
	}
	b.genericOverrides = map[string]agentapi.Status{}
	for _, m := range s.Members {
		b.genericOverrides[m.Name] = agentapi.Status{ConfigVersion: m.ConfigVersion, Agent: agentapi.AgentBuild{Build: "old"}}
	}
	wrapped := &rolloutBackend{fakeBackend: b, rc: rc, holdBuild: true}
	rc.Backend = wrapped
	op := rolloutOperation(true)
	for range 2 {
		r, e := p.RunOperation(t.Context(), rc, op)
		if e != nil {
			t.Fatal(e)
		}
		op.Checkpoint = r.Checkpoint
	}
	r, e := p.CancelOperation(t.Context(), rc, op)
	if e != nil || r.Done {
		t.Fatal("cancel abandoned in-flight agent", r, e)
	}
	op.Checkpoint = r.Checkpoint
	first := wrapped.names[0]
	st := b.genericOverrides[first]
	st.Agent.Build = engine.CurrentAgentBuild().Build
	b.genericOverrides[first] = st
	for range 4 {
		r, e = p.CancelOperation(t.Context(), rc, op)
		if e != nil {
			t.Fatal(e)
		}
		if r.Checkpoint != nil {
			op.Checkpoint = r.Checkpoint
		}
		if r.Done {
			break
		}
	}
	if !r.Done || len(wrapped.names) != 1 {
		t.Fatal("cancel did not stop between members", wrapped.names, r)
	}
}
func closeRolloutWindow(rc *engine.Context) {
	// PKI issuance uses wall time; freeze a valid instant, then place the next
	// window two hours away rather than making fresh certificates future-dated.
	at := time.Now().UTC()
	rc.Clock = importReplayClock{at: at}
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 " + at.Add(2*time.Hour).Format("15") + " * * *", Duration: "1h"}}}
}
func TestMaintenanceBlocksNewVersionAndScaleInButNotDeployCompletion(t *testing.T) {
	for _, change := range []string{"version", "resources", "scale", "profiles"} {
		t.Run(change, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			s := converge(t, p, rc)
			closeRolloutWindow(rc)
			b.actions = nil
			switch change {
			case "version":
				rc.Cluster.Spec.Version = "17.8-1"
			case "resources":
				rc.Cluster.Spec.Components.Engine.Resources.MaxCPU++
			case "scale":
				rc.Cluster.Spec.Components.Engine.Replicas = 1
			case "profiles":
				rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"profiles":["postgis"]}`)
			}
			if _, e := p.Sync(t.Context(), rc); e != nil {
				t.Fatal(e)
			}
			if len(b.actions) != 0 {
				t.Fatal("closed window disrupted cluster", b.actions)
			}
			current, _ := load(rc.Cluster)
			for _, m := range current.Members {
				if m.Role == "data" && m.Version != s.find(m.Name).Version {
					t.Fatal("version advanced outside window")
				}
				if m.Role == "data" && change == "profiles" && string(m.Options) != string(s.find(m.Name).Options) {
					t.Fatal("profile preparation advanced outside window")
				}
			}
		})
	}
	p, rc, b := setup(t, "single")
	s := converge(t, p, rc)
	closeRolloutWindow(rc)
	s.data()[0].Stage = "deploying"
	s.data()[0].DeployOp = &infra.Op{}
	if e := save(rc.Cluster, s); e != nil {
		t.Fatal(e)
	}
	b.actions = nil
	if _, e := p.Sync(t.Context(), rc); e != nil {
		t.Fatal(e)
	}
	s, _ = load(rc.Cluster)
	if s.data()[0].Stage != "joined" {
		t.Fatal("window stopped in-flight deployment")
	}
	delete(b.services, s.data()[0].Name)
	b.deployed[s.data()[0].Name] = false
	for range 12 {
		if _, e := p.Sync(t.Context(), rc); e != nil {
			t.Fatal(e)
		}
	}
	if !b.deployed[s.data()[0].Name] {
		t.Fatal("healing was gated")
	}
}
func TestMaintenanceLeafRenewalEmergencyAndTransportExemption(t *testing.T) {
	for _, days := range []int{29, 7} {
		t.Run(time.Duration(days).String(), func(t *testing.T) {
			p, rc, _ := setup(t, "single")
			s := converge(t, p, rc)
			crt, _ := secret(t.Context(), rc, "tls/ca.crt")
			key, _ := secret(t.Context(), rc, "tls/ca.key")
			ca, e := pki.LoadCA(crt, key)
			if e != nil {
				t.Fatal(e)
			}
			leaf, leafKey, e := ca.Issue(pki.CertRequest{CommonName: "demop0", DNSNames: []string{"demop0", "localhost"}, Validity: time.Duration(days) * 24 * time.Hour, Server: true, Client: true})
			if e != nil {
				t.Fatal(e)
			}
			for _, kind := range []string{"servers", "agents"} {
				for suffix, value := range map[string][]byte{".crt": leaf, ".key": leafKey} {
					if e := putSecret(t.Context(), rc, "tls/"+kind+"/demop0"+suffix, value); e != nil {
						t.Fatal(e)
					}
				}
			}
			closeRolloutWindow(rc)
			block, _ := pem.Decode(leaf)
			certificate, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				t.Fatal(err)
			}
			at := certificate.NotAfter.Add(-time.Duration(days)*24*time.Hour + time.Minute)
			rc.Clock = importReplayClock{at: at}
			rc.Cluster.Spec.Maintenance.Windows[0].Schedule = "0 " + at.Add(2*time.Hour).Format("15") + " * * *"
			if e := ensureCertificates(t.Context(), rc, s, false); e != nil {
				t.Fatal(e)
			}
			if s.find("demop0").TLSPending != (days <= 7) {
				t.Fatal("incorrect seven-day leaf rule")
			}
			transport, _ := secret(t.Context(), rc, "tls/agents/demop0.crt")
			if string(transport) == string(leaf) {
				t.Fatal("transport renewal waited for maintenance")
			}
		})
	}
}

func TestAgentRolloutHandoffBlockerAndReplicationWait(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	b.genericOverrides = map[string]agentapi.Status{}
	for _, m := range s.Members {
		b.genericOverrides[m.Name] = agentapi.Status{ConfigVersion: m.ConfigVersion, Agent: agentapi.AgentBuild{Build: "old"}}
	}
	wrapped := &rolloutBackend{fakeBackend: b, rc: rc}
	rc.Backend = wrapped
	writer, err := live(t.Context(), rc, "demop0")
	if err != nil {
		t.Fatal(err)
	}
	writer.EtcdHealthy = false
	b.statusOverrides = map[string]pgspec.StatusResponse{"demop0": writer}
	cp := agentUpgradeCheckpoint{Target: engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: true}, Order: []string{"demop0"}}
	raw, _ := json.Marshal(cp)
	op := rolloutOperation(true)
	op.Checkpoint = raw
	r, err := p.RunOperation(t.Context(), rc, op)
	if err != nil || len(wrapped.names) != 0 || b.leader != "" {
		t.Fatal("local-etcd blocker did not fence handoff", r, err)
	}
	delete(b.statusOverrides, "demop0")
	for range 2 {
		r, err = p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = r.Checkpoint
	}
	if len(wrapped.names) != 1 {
		t.Fatal("primary did not deploy after handoff", wrapped.names)
	}
	replica, err := live(t.Context(), rc, "demop0")
	if err != nil {
		t.Fatal(err)
	}
	replica.LagBytes = 1
	b.statusOverrides["demop0"] = replica
	closeRolloutWindow(rc)
	r, err = p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(r.Checkpoint, &cp); err != nil {
		t.Fatal(err)
	}
	if cp.Index != 0 || !cp.Waiting {
		t.Fatal("lagging member completed rollout")
	}
}

func TestMaintenanceDirectionalRestartAndMajorStart(t *testing.T) {
	p, rc, b := setup(t, "ha")
	converge(t, p, rc)
	closeRolloutWindow(rc)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"parameters":{"max_connections":"200"}}`)
	b.actions = nil
	for range 8 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	for _, action := range b.actions {
		if strings.Contains(action, "restart") {
			t.Fatal("directional restart crossed closed window", b.actions)
		}
	}
	s, _ := load(rc.Cluster)
	if s.Rolling == nil {
		t.Fatal("directional restart was not planned")
	}
	s.Rolling = nil
	rc.Cluster.Spec.Version = "18.1"
	handled, err := p.reconcileUpgrade(t.Context(), rc, s)
	if err != nil || !handled || s.Upgrade != nil {
		t.Fatal("offline major upgrade started outside window", handled, err)
	}
}

func TestMaintenancePendingRestartRecoveryExemption(t *testing.T) {
	for _, desired := range []bool{false, true} {
		t.Run(map[bool]string{false: "healing", true: "desired-config"}[desired], func(t *testing.T) {
			p, rc, b := setup(t, "single")
			s := converge(t, p, rc)
			m := s.data()[0]
			m.Stage = "deployed"
			m.ConfigRestartPending = desired
			b.statusOverrides = map[string]pgspec.StatusResponse{m.Name: {Running: true, Role: "standalone", PendingRestart: true}}
			closeRolloutWindow(rc)
			b.actions = nil
			if _, err := readyDesired(t.Context(), rc, m); err != nil {
				t.Fatal(err)
			}
			if desired && len(b.actions) != 0 {
				t.Fatal("desired configuration restarted outside window", b.actions)
			}
			if !desired && len(b.actions) == 0 {
				t.Fatal("healing restart was gated")
			}
		})
	}
}

func TestAgentRolloutWindowClosesBetweenMembers(t *testing.T) {
	for _, next := range []string{"demop2", "demop0"} {
		t.Run(next, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			s := converge(t, p, rc)
			b.genericOverrides = map[string]agentapi.Status{}
			for _, m := range s.Members {
				b.genericOverrides[m.Name] = agentapi.Status{ConfigVersion: m.ConfigVersion, Agent: agentapi.AgentBuild{Build: "old"}}
			}
			wrapped := &rolloutBackend{fakeBackend: b, rc: rc, holdBuild: true}
			rc.Backend = wrapped
			cp := agentUpgradeCheckpoint{Target: engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build}, Order: []string{"demop1", next}}
			op := rolloutOperation(false)
			op.Checkpoint, _ = json.Marshal(cp)
			step := func() engine.OpResult {
				t.Helper()
				p = New(p.options)
				r, err := p.RunOperation(t.Context(), rc, op)
				if err != nil {
					t.Fatal(err)
				}
				if r.Checkpoint != nil {
					op.Checkpoint = r.Checkpoint
				}
				return r
			}
			step() // Deploy member one while open.
			if len(wrapped.names) != 1 {
				t.Fatal("first member did not deploy", wrapped.names)
			}
			closeRolloutWindow(rc)
			step() // The old build must still fence completion while closed.
			if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
				t.Fatal(err)
			}
			if !cp.Waiting || cp.Index != 0 {
				t.Fatal("closed window bypassed recovery build fence", cp)
			}
			st := b.genericOverrides["demop1"]
			st.Agent.Build = engine.CurrentAgentBuild().Build
			b.genericOverrides["demop1"] = st
			step()
			if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
				t.Fatal(err)
			}
			if cp.Waiting || cp.Index != 1 {
				t.Fatal("closed window stranded recovering member", cp)
			}
			r := step()
			if len(wrapped.names) != 1 || b.leader != "" || r.Message != "Waiting for maintenance window" || r.RequeueAfter <= 0 {
				t.Fatal("next member disrupted outside window", wrapped.names, b.leader, r)
			}
			// The override must survive operation persistence and provider restart,
			// without relying on the caller having populated rc.Operation.
			op.IgnoreMaintenanceWindow = true
			raw, err := json.Marshal(op)
			if err != nil {
				t.Fatal(err)
			}
			op = &domain.Operation{}
			if err := json.Unmarshal(raw, op); err != nil {
				t.Fatal(err)
			}
			step()
			if next == "demop0" {
				if b.leader == "" {
					t.Fatal("persisted override did not permit handoff")
				}
				step()
			}
			if len(wrapped.names) != 2 || wrapped.names[1] != next {
				t.Fatal("persisted override did not permit redeployment", wrapped.names)
			}
			if rc.Operation != nil {
				t.Fatal("rollout leaked its operation context")
			}
		})
	}
}

func TestAgentRolloutWindowClosesAfterHandoff(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	b.genericOverrides = map[string]agentapi.Status{}
	for _, m := range s.Members {
		b.genericOverrides[m.Name] = agentapi.Status{ConfigVersion: m.ConfigVersion, Agent: agentapi.AgentBuild{Build: "old"}}
	}
	wrapped := &rolloutBackend{fakeBackend: b, rc: rc}
	rc.Backend = wrapped
	cp := agentUpgradeCheckpoint{Target: engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build}, Order: []string{"demop0"}}
	op := rolloutOperation(false)
	op.Checkpoint, _ = json.Marshal(cp)
	r, err := p.RunOperation(t.Context(), rc, op)
	if err != nil || b.leader == "" || len(wrapped.names) != 0 {
		t.Fatal("handoff did not finish before deployment", r, err)
	}
	op.Checkpoint = r.Checkpoint
	closeRolloutWindow(rc)
	r, err = p.RunOperation(t.Context(), rc, op)
	if err != nil || len(wrapped.names) != 0 || r.Message != "Waiting for maintenance window" {
		t.Fatal("demoted but running member redeployed outside window", r, err, wrapped.names)
	}
}

func TestDistributedAgentRolloutCoversEveryGroupSerially(t *testing.T) {
	p, rc, b, _ := setupDistributed(t, 3)
	s := converge(t, p, rc)
	b.genericOverrides = map[string]agentapi.Status{}
	for _, m := range s.Members {
		b.genericOverrides[m.Name] = agentapi.Status{ConfigVersion: m.ConfigVersion, Agent: agentapi.AgentBuild{Build: "old"}}
	}
	wrapped := &rolloutBackend{fakeBackend: b, rc: rc, holdBuild: true}
	rc.Backend = wrapped
	op := rolloutOperation(true)
	step := func() (engine.OpResult, agentUpgradeCheckpoint) {
		t.Helper()
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		if result.Checkpoint != nil {
			op.Checkpoint = result.Checkpoint
		}
		var checkpoint agentUpgradeCheckpoint
		if err := json.Unmarshal(op.Checkpoint, &checkpoint); err != nil {
			t.Fatal(err)
		}
		return result, checkpoint
	}
	_, checkpoint := step()
	if len(checkpoint.Order) != len(s.Members) {
		t.Fatalf("frozen order omitted group members: %v", checkpoint.Order)
	}
	seen := map[string]bool{}
	for _, name := range checkpoint.Order {
		if seen[name] || s.find(name) == nil {
			t.Fatalf("invalid frozen order: %v", checkpoint.Order)
		}
		seen[name] = true
	}
	_, checkpoint = step()
	if !checkpoint.Waiting || len(wrapped.names) != 1 {
		t.Fatal("first member was not journaled", checkpoint, wrapped.names)
	}
	for range 3 {
		step()
	}
	if len(wrapped.names) != 1 {
		t.Fatal("another member deployed before the current build was ready", wrapped.names)
	}
	first := s.find(wrapped.names[0])
	b.genericOverrides[first.Name] = agentapi.Status{ConfigVersion: first.ConfigVersion, Agent: agentapi.AgentBuild{Build: engine.CurrentAgentBuild().Build}}
	wrapped.holdBuild = false
	for range 100 {
		result, checkpoint := step()
		if checkpoint.Waiting {
			member := s.find(checkpoint.Order[checkpoint.Index])
			if member.Role == "data" {
				want := "group-0"
				if member.Group == 1 {
					want = "group-1"
				}
				if checkpoint.SystemID != want {
					t.Fatalf("WAL barrier crossed groups: member=%s checkpoint=%+v", member.Name, checkpoint)
				}
			}
		}
		if result.Done {
			if len(wrapped.names) != len(s.Members) {
				t.Fatalf("rollout omitted members: %v", wrapped.names)
			}
			return
		}
	}
	t.Fatal("distributed rollout did not finish", wrapped.names)
}

func TestDistributedRestartAdmitsEachNewMemberInsideWindow(t *testing.T) {
	p, rc, b, _ := setupDistributed(t, 3)
	s := converge(t, p, rc)
	closeRolloutWindow(rc)
	order := []string{s.group(1).data()[1].Name, s.group(0).data()[1].Name}
	op := &domain.Operation{ID: "distributed-restart", Kind: domain.OperationRestart, Checkpoint: mustJSON(map[string]any{"order": order})}
	b.actions = nil
	result, err := p.RunOperation(t.Context(), rc, op)
	if err != nil || len(b.actions) != 0 || result.Message != "Waiting for maintenance window" {
		t.Fatal("closed window admitted restart", result, b.actions, err)
	}
	op.Checkpoint = result.Checkpoint
	op.IgnoreMaintenanceWindow = true
	result, err = p.RunOperation(t.Context(), rc, op)
	if err != nil || len(b.actions) != 1 {
		t.Fatal("override did not restart exactly one member", result, b.actions, err)
	}
	op.Checkpoint = result.Checkpoint
	op.IgnoreMaintenanceWindow = false
	result, err = p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = result.Checkpoint
	result, err = p.RunOperation(t.Context(), rc, op)
	if err != nil || len(b.actions) != 1 || result.Message != "Waiting for maintenance window" {
		t.Fatal("next group disrupted after window closure", result, b.actions, err)
	}
}

type extensionSelectionFlavour struct{}

func (extensionSelectionFlavour) Name() string                                     { return "supabase" }
func (extensionSelectionFlavour) PostReady(context.Context, *engine.Context) error { return nil }
func (extensionSelectionFlavour) RelayTargets(*domain.Cluster) []string            { return nil }
func (extensionSelectionFlavour) PostgresOptions(json.RawMessage) (json.RawMessage, error) {
	return nil, nil
}
func (extensionSelectionFlavour) SelectedExtensions(raw json.RawMessage) ([]string, error) {
	var selected struct {
		Extensions []string `json:"extensions"`
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &selected); err != nil {
			return nil, err
		}
	}
	return selected.Extensions, nil
}

func TestExtensionPreloadChangesRequireMaintenanceAdmission(t *testing.T) {
	p, rc, _ := setup(t, "single")
	s := converge(t, p, rc)
	p.options.Flavour = extensionSelectionFlavour{}
	member := s.data()[0]
	for _, test := range []struct {
		before, after string
		disruptive    bool
	}{
		{`{}`, `{"extensions":["pg_cron"]}`, true},
		{`{"extensions":["pg_cron"]}`, `{}`, true},
		{`{}`, `{"extensions":["pgcrypto"]}`, false},
	} {
		member.Options = json.RawMessage(test.before)
		rc.Cluster.Spec.EngineOptions = json.RawMessage(test.after)
		if got := needsDisruption(p, rc, member, desiredComponent(rc, member)); got != test.disruptive {
			t.Fatalf("preloads %s -> %s disruptive=%v, want %v", test.before, test.after, got, test.disruptive)
		}
	}
}

func TestAgentRolloutRefreshesBarrierAfterFailover(t *testing.T) {
	for _, fault := range []string{"lag", "wrong-leader", "old-timeline", "replica-identity", "primary-identity", "second-failover", "more-members", "partition", "stale-leader", "unhealthy-dcs", "duplicate-primary", "forked-upstream"} {
		t.Run(fault, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			s := converge(t, p, rc)
			b.statusOverrides = map[string]pgspec.StatusResponse{}
			for _, m := range s.data() {
				st, err := live(t.Context(), rc, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				st.SystemID, st.Timeline, st.WALPosition, st.Leader = "system", 11, "0/300", "demop0"
				b.statusOverrides[m.Name] = st
			}
			target := engine.AgentUpgradePayload{TargetBuild: "previous-cp-build"}
			b.genericOverrides = map[string]agentapi.Status{
				"demop1": {ConfigVersion: s.find("demop1").ConfigVersion, Agent: agentapi.AgentBuild{Build: target.TargetBuild}},
			}
			cp := agentUpgradeCheckpoint{
				Target: target, Order: []string{"demop1"}, Waiting: true,
				Barrier: "0/200", Timeline: 6, SystemID: "system",
			}
			if fault == "more-members" {
				cp.Order = append(cp.Order, "demop2")
			}
			if fault == "replica-identity" || fault == "primary-identity" {
				name := "demop1"
				if fault == "primary-identity" {
					name = "demop0"
				}
				st := b.statusOverrides[name]
				st.SystemID = "foreign-system"
				b.statusOverrides[name] = st
			}
			proof := installRolloutReplaySQL(t, rc)
			switch fault {
			case "partition":
				b.statusFailures = map[string]int{"demop2": 100}
			case "stale-leader", "unhealthy-dcs":
				writer := b.statusOverrides["demop0"]
				if fault == "stale-leader" {
					writer.Leader = "demop2"
				} else {
					writer.EtcdHealthy = false
				}
				b.statusOverrides["demop0"] = writer
			case "duplicate-primary":
				peer := b.statusOverrides["demop2"]
				peer.Role, peer.Leader = "primary", "demop2"
				b.statusOverrides["demop2"] = peer
			}
			op := rolloutOperation(false)
			op.Checkpoint, _ = json.Marshal(cp)
			step := func() (engine.OpResult, error) {
				t.Helper()
				p = New(p.options) // Resume exclusively from the durable receipt.
				result, err := p.RunOperation(t.Context(), rc, op)
				if result.Checkpoint != nil {
					op.Checkpoint = result.Checkpoint
					if e := json.Unmarshal(result.Checkpoint, &cp); e != nil {
						t.Fatal(e)
					}
				}
				return result, err
			}
			before := len(b.actions)
			result, err := step()
			if fault == "partition" || fault == "stale-leader" || fault == "unhealthy-dcs" || fault == "duplicate-primary" {
				if err == nil || !strings.Contains(err.Error(), "re-run upgrade-agents") || cp.Timeline != 6 || cp.Primary != "" || cp.Index != 0 {
					t.Fatalf("non-authoritative primary refreshed or acknowledged barrier: %+v %v", cp, err)
				}
				return
			}
			if strings.HasSuffix(fault, "identity") {
				if !errors.Is(err, engine.ErrInvalid) || !strings.Contains(err.Error(), "re-run upgrade-agents") {
					t.Fatalf("diverged identity did not fail explicitly: %+v %v", result, err)
				}
				return
			}
			if err != nil || result.Done || cp.Index != 0 || cp.Timeline != 11 || cp.Barrier != "0/300" || cp.Primary != "demop0" {
				t.Fatalf("current-primary barrier not durably refreshed: %+v %v", cp, err)
			}
			if fault == "second-failover" {
				writer := b.statusOverrides["demop0"]
				writer.Timeline, writer.WALPosition = 12, "0/400"
				b.statusOverrides["demop0"] = writer
				if _, err := step(); err != nil || cp.Timeline != 12 || cp.Barrier != "0/400" {
					t.Fatalf("second failover did not refresh barrier: %+v %v", cp, err)
				}
			}
			replica := b.statusOverrides["demop1"]
			switch fault {
			case "lag":
				replica.WALPosition = "0/250"
			case "wrong-leader":
				replica.Leader = "demop2"
			case "old-timeline":
				replica.Timeline = 6
			case "forked-upstream":
				// Local streaming, DCS leader, system ID, timeline and replay
				// all match, but the selected primary has no replay proof.
				proof.Store(false)
			}
			b.statusOverrides["demop1"] = replica
			if fault != "more-members" {
				if _, err := step(); !errors.Is(err, engine.ErrUnavailable) || !strings.Contains(err.Error(), "re-run upgrade-agents") {
					t.Fatalf("unproven catch-up passed barrier or lost recovery guidance: %v", err)
				}
			}
			replica.Timeline, replica.WALPosition, replica.Leader = cp.Timeline, cp.Barrier, cp.Primary
			b.statusOverrides["demop1"] = replica
			proof.Store(true)
			if _, err := step(); err != nil || cp.Index != 1 || cp.Waiting {
				t.Fatalf("caught-up old-target member did not complete: %+v %v", cp, err)
			}
			result, err = step()
			if fault == "more-members" {
				if !errors.Is(err, engine.ErrInvalid) {
					t.Fatalf("new CP was allowed to disrupt remaining frozen-target member: %+v %v", result, err)
				}
			} else if err != nil || !result.Done {
				t.Fatalf("final frozen-target acknowledgement failed after CP change: %+v %v", result, err)
			}
			if len(b.actions) != before {
				t.Fatal("waiting recovery redeployed or mutated a member", b.actions[before:])
			}
		})
	}
}

type pausedProxyRolloutBackend struct {
	*rolloutBackend
	state *infra.OpState
	err   error
}

func (b *pausedProxyRolloutBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if b.state != nil || b.err != nil {
		if b.state != nil {
			return *b.state, b.err
		}
		return infra.OpState{}, b.err
	}
	if op.ID == "paused-proxy" {
		return infra.OpState{Failed: true, Status: "failed", Message: "ProxySQL start-first rollout paused; incumbents were retained. Inspect replacement health before retrying"}, nil
	}
	return b.rolloutBackend.OpStatus(ctx, op)
}

func TestAgentRolloutPausedProxyFailsPermanentlyAndCanRetry(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	b.genericOverrides = map[string]agentapi.Status{}
	for _, m := range s.Members {
		b.genericOverrides[m.Name] = agentapi.Status{ConfigVersion: m.ConfigVersion, Agent: agentapi.AgentBuild{Build: engine.CurrentAgentBuild().Build}}
	}
	wrapped := &pausedProxyRolloutBackend{rolloutBackend: &rolloutBackend{fakeBackend: b, rc: rc}}
	rc.Backend = wrapped
	op := rolloutOperation(true)
	cp := agentUpgradeCheckpoint{Target: engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: true}, Order: []string{"demopx"}, Waiting: true, Deploy: &infra.Op{ID: "paused-proxy"}}
	op.Checkpoint, _ = json.Marshal(cp)
	for _, cancel := range []bool{false, true} {
		var err error
		if cancel {
			_, err = p.CancelOperation(t.Context(), rc, op)
		} else {
			_, err = p.RunOperation(t.Context(), rc, op)
		}
		if !errors.Is(err, engine.ErrInvalid) || !strings.Contains(err.Error(), "incumbents were retained") || !strings.Contains(err.Error(), "manual recovery") {
			t.Fatalf("paused proxy must fail without generic retries: %v", err)
		}
	}
	if len(wrapped.names) != 0 {
		t.Fatal("failed receipt submitted a deployment", wrapped.names)
	}
	retry := rolloutOperation(true)
	for range 80 {
		result, err := p.RunOperation(t.Context(), rc, retry)
		if err != nil {
			t.Fatal(err)
		}
		if result.Done {
			if len(wrapped.names) == 0 {
				t.Fatal("include-current retry skipped deployment")
			}
			return
		}
		retry.Checkpoint = result.Checkpoint
	}
	t.Fatal("include-current retry did not finish")
}

func TestProxyServingUpdateFailurePreservesDiagnostic(t *testing.T) {
	for _, action := range []string{"scale", "resize"} {
		t.Run(action, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			s := converge(t, p, rc)
			m := s.find("demopx")
			rc.Backend = &pausedProxyRolloutBackend{rolloutBackend: &rolloutBackend{fakeBackend: b, rc: rc}}
			s.Pending = &pending{Action: action, Member: m.Name, Replicas: desiredComponent(rc, m).Replicas, Resources: desiredComponent(rc, m).Resources, Op: infra.Op{ID: "paused-proxy"}}
			busy, err := p.observePending(t.Context(), rc, s)
			if !busy || err == nil || errors.Is(err, engine.ErrInvalid) || !strings.Contains(err.Error(), "incumbents were retained") {
				t.Fatalf("lost terminal serving update diagnostic: busy=%t err=%v", busy, err)
			}
			if s.Pending != nil {
				t.Fatal("terminal receipt blocks a corrective request")
			}
		})
	}
}

func TestProxyAmbiguousFailureRemainsRetryable(t *testing.T) {
	for _, mode := range []string{"stalled", "expired", "transport"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			s := converge(t, p, rc)
			state := infra.OpState{Failed: true, Status: "stalled", Message: "outcome unknown"}
			var transport error
			if mode == "expired" {
				state.Status, state.Expired = "expired", true
			}
			if mode == "transport" {
				transport = errors.New("transport unavailable")
			}
			rc.Backend = &pausedProxyRolloutBackend{rolloutBackend: &rolloutBackend{fakeBackend: b, rc: rc}, state: &state, err: transport}
			cp := agentUpgradeCheckpoint{Order: []string{"demopx"}, Waiting: true, Deploy: &infra.Op{ID: "paused-proxy"}}
			raw, _ := json.Marshal(cp)
			_, err := p.RunOperation(t.Context(), rc, &domain.Operation{Kind: domain.OperationUpgradeAgents, Checkpoint: raw})
			if err == nil || errors.Is(err, engine.ErrInvalid) {
				t.Fatalf("ambiguous deployment became permanent: %v", err)
			}
			for _, action := range []string{"scale", "resize"} {
				m := s.find("demopx")
				s.Pending = &pending{Action: action, Member: m.Name, Replicas: desiredComponent(rc, m).Replicas, Resources: desiredComponent(rc, m).Resources, Op: infra.Op{ID: "paused-proxy"}}
				_, err = p.observePending(t.Context(), rc, s)
				if err == nil || errors.Is(err, engine.ErrInvalid) || (mode == "transport" && s.Pending == nil) {
					t.Fatalf("ambiguous %s became permanent or lost transport retry: %v", action, err)
				}
			}
		})
	}
}

type ambiguousProxyRolloutBackend struct {
	*rolloutBackend
	op        *domain.Operation
	receipt   *infra.Op
	requestID string
	lookups   int
	polls     int
	state     infra.OpState
	lookupErr error
	t         *testing.T
}

func (b *ambiguousProxyRolloutBackend) Capabilities() infra.Capabilities {
	c := b.rolloutBackend.Capabilities()
	c.ProxyDeploymentPreservesServing = true
	return c
}

func (b *ambiguousProxyRolloutBackend) ProxyUpdatesPreserveServing(context.Context, string) (bool, error) {
	return true, nil
}

func (b *ambiguousProxyRolloutBackend) Deploy(_ context.Context, name string, d infra.Deployment) (infra.Op, error) {
	// Submission is accepted by the platform, but its reply never reaches the
	// engine. Recovery must work from the checkpoint saved before this call.
	var intent map[string]any
	if err := json.Unmarshal(b.op.Checkpoint, &intent); err != nil || intent["proxyServing"] != true {
		b.t.Errorf("serving rollout policy not persisted before submission: %s (%v)", b.op.Checkpoint, err)
	}
	if d.RequestID != b.op.ID || d.ProxyPreserveServing == nil || !*d.ProxyPreserveServing {
		b.t.Errorf("missing immutable serving request identity: request=%q policy=%v", d.RequestID, d.ProxyPreserveServing)
	}
	b.names = append(b.names, name)
	b.requestID = d.RequestID
	b.receipt = &infra.Op{ID: "accepted-proxy"}
	return infra.Op{}, errors.New("upload reply lost after acceptance")
}

func (b *ambiguousProxyRolloutBackend) LookupProxyDeployment(_ context.Context, name, requestID string) (*infra.Op, error) {
	b.lookups++
	if b.receipt == nil {
		return nil, b.lookupErr
	}
	if name != "demopx" || requestID != b.requestID {
		return nil, errors.New("request identity does not match")
	}
	return b.receipt, b.lookupErr
}

func (b *ambiguousProxyRolloutBackend) OpStatus(_ context.Context, op infra.Op) (infra.OpState, error) {
	b.polls++
	if b.receipt == nil || op.ID != b.receipt.ID {
		return infra.OpState{}, errors.New("polling a different deployment")
	}
	return b.state, nil
}

func TestAgentRolloutAmbiguousProxyCancellation(t *testing.T) {
	for _, flavour := range []string{pgspec.FlavourPostgres, pgspec.FlavourSupabase} {
		for _, terminal := range []string{"completed", "paused", "rolled-back", "lookup-error"} {
			t.Run(flavour+"/"+terminal, func(t *testing.T) {
				p, rc, b := setup(t, "ha")
				s := converge(t, p, rc)
				if flavour == pgspec.FlavourSupabase {
					p.options.Flavour = extensionSelectionFlavour{}
					rc.Cluster.Spec.Provider = flavour
				}
				b.genericOverrides = map[string]agentapi.Status{}
				for _, m := range s.Members {
					b.genericOverrides[m.Name] = agentapi.Status{ConfigVersion: m.ConfigVersion, Agent: agentapi.AgentBuild{Build: engine.CurrentAgentBuild().Build}}
				}
				op := rolloutOperation(true)
				cp := agentUpgradeCheckpoint{Target: engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: true}, Order: []string{"demopx", "demop1"}}
				op.Checkpoint, _ = json.Marshal(cp)
				backend := &ambiguousProxyRolloutBackend{rolloutBackend: &rolloutBackend{fakeBackend: b, rc: rc}, op: op, t: t}
				rc.Backend = backend
				// Allow the engine to persist its intent, then lose the reply.
				var submitErr error
				for range 3 {
					result, err := p.RunOperation(t.Context(), rc, op)
					if result.Checkpoint != nil {
						op.Checkpoint = result.Checkpoint
					}
					if err != nil {
						submitErr = err
						break
					}
				}
				if submitErr == nil || !strings.Contains(submitErr.Error(), "reply lost") || len(backend.names) != 1 {
					t.Fatalf("did not exercise accepted ambiguous submission: %v %v", backend.names, submitErr)
				}
				backend.lookups = 0
				if terminal == "lookup-error" {
					backend.lookupErr = errors.New("receipt unavailable")
				}
				// Recreate the core to simulate restart; no artifact submission is
				// permitted while cancellation recovers the persisted receipt.
				p = New(p.options)
				result, err := p.CancelOperation(t.Context(), rc, op)
				if terminal == "lookup-error" {
					if err == nil || result.Done || backend.lookups != 1 || backend.polls != 0 {
						t.Fatalf("lookup failure settled cancellation: %+v %v", result, err)
					}
					return
				}
				if err != nil || result.Done || backend.lookups != 1 || backend.polls != 1 {
					t.Fatalf("cancel abandoned ambiguous platform work: %+v %v (lookups=%d polls=%d)", result, err, backend.lookups, backend.polls)
				}
				op.Checkpoint = result.Checkpoint
				backend.state = infra.OpState{Done: terminal == "completed", Failed: terminal != "completed", Status: terminal, Message: "platform " + terminal}
				result, err = p.CancelOperation(t.Context(), rc, op)
				if terminal != "completed" {
					if err == nil || !result.Done || !strings.Contains(err.Error(), "manual recovery") || !strings.Contains(err.Error(), terminal) {
						t.Fatalf("failed platform work did not terminate cancellation as failure: %+v %v", result, err)
					}
				} else {
					if err != nil || result.Done {
						t.Fatalf("completed platform work did not settle member readiness: %+v %v", result, err)
					}
					op.Checkpoint = result.Checkpoint
					result, err = p.CancelOperation(t.Context(), rc, op)
					if err != nil || !result.Done {
						t.Fatalf("settled member did not cancel between members: %+v %v", result, err)
					}
				}
				if len(backend.names) != 1 || backend.lookups != 1 || backend.polls != 2 {
					t.Fatalf("cancel resubmitted or lost adopted receipt: deploys=%v lookups=%d polls=%d", backend.names, backend.lookups, backend.polls)
				}
			})
		}
	}
}

type recoveringProxyRolloutBackend struct {
	*ambiguousProxyRolloutBackend
	recovering bool
}

func (b *recoveringProxyRolloutBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	if !b.recovering {
		return b.ambiguousProxyRolloutBackend.Deploy(ctx, name, d)
	}
	// Like Dokploy's Deploy path, a fresh explicit operation can re-arm a
	// definitively failed receipt. Read-only lookup must not prevent that.
	if _, err := b.rolloutBackend.Deploy(ctx, name, d); err != nil {
		return infra.Op{}, err
	}
	b.requestID = d.RequestID
	b.receipt = &infra.Op{ID: "recovered-proxy"}
	b.state = infra.OpState{Done: true}
	return *b.receipt, nil
}

func TestAgentRolloutExplicitRetryRearmsFailedProxyReceipt(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	b.genericOverrides = map[string]agentapi.Status{}
	for _, m := range s.Members {
		b.genericOverrides[m.Name] = agentapi.Status{ConfigVersion: m.ConfigVersion, Agent: agentapi.AgentBuild{Build: engine.CurrentAgentBuild().Build}}
	}
	newOperation := func(id string) *domain.Operation {
		op := rolloutOperation(true)
		op.ID = id
		cp := agentUpgradeCheckpoint{Target: engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: true}, Order: []string{"demopx"}}
		op.Checkpoint, _ = json.Marshal(cp)
		return op
	}
	first := newOperation("first-upgrade")
	backend := &recoveringProxyRolloutBackend{ambiguousProxyRolloutBackend: &ambiguousProxyRolloutBackend{
		rolloutBackend: &rolloutBackend{fakeBackend: b, rc: rc}, op: first, t: t,
	}}
	rc.Backend = backend
	result, err := p.RunOperation(t.Context(), rc, first)
	if err != nil || len(backend.names) != 0 {
		t.Fatalf("first operation did not checkpoint before submission: %+v %v", result, err)
	}
	first.Checkpoint = result.Checkpoint
	if _, err := p.RunOperation(t.Context(), rc, first); err == nil || !strings.Contains(err.Error(), "reply lost") {
		t.Fatalf("first operation did not exercise ambiguous submission: %v", err)
	}
	backend.state = infra.OpState{Failed: true, Status: "paused", Message: "rollout paused"}
	result, err = p.CancelOperation(t.Context(), rc, first)
	if err == nil || !result.Done || !strings.Contains(err.Error(), "manual recovery") {
		t.Fatalf("first operation did not fail after adopting paused receipt: %+v %v", result, err)
	}
	lookups := backend.lookups
	backend.recovering = true
	retry := newOperation("explicit-retry")
	for range 8 {
		result, err = p.RunOperation(t.Context(), rc, retry)
		if err != nil {
			t.Fatalf("explicit retry blocked by retained failed receipt: %v", err)
		}
		if result.Done {
			if len(backend.names) != 2 || backend.requestID != retry.ID || backend.lookups != lookups {
				t.Fatalf("retry did not re-arm through Deploy: deploys=%v request=%q lookups=%d", backend.names, backend.requestID, backend.lookups)
			}
			return
		}
		retry.Checkpoint = result.Checkpoint
	}
	t.Fatal("explicit include-current retry did not complete")
}
