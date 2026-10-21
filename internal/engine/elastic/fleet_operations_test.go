// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"bytes"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func closeFleetWindow(rc *engine.Context) {
	rc.Clock = &coreClock{at: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)}
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{TimeZone: "UTC", Windows: []domain.MaintenanceWindow{{Schedule: "0 1 * * *", Duration: "1h"}}}
}

func fleetAgents(t *testing.T, rc *engine.Context, build *string) {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			opsJSON(w, agentapi.Status{Agent: agentapi.AgentBuild{Build: *build}, ConfigVersion: 1 << 30, Processes: map[string]agentapi.ProcessStatus{"elasticsearch": {State: "running"}, "kibana": {State: "running"}, "logstash": {State: "running"}, "apm-server": {State: "running"}}})
		case "/v1/elasticsearch/readiness":
			opsJSON(w, agentspec.Readiness{Ready: true, ConfigVersion: 1 << 30})
		case "/v1/reload":
			opsJSON(w, map[string]bool{"success": true})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	rc.Agents = coreAgentDialer{server.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
}

func TestAgentUpgradeOrderJournalAndBuildGates(t *testing.T) {
	for _, include := range []bool{false, true} {
		t.Run(map[bool]string{false: "skip-current", true: "include-current"}[include], func(t *testing.T) {
			p, rc, f, es := coreSetup(t, 2)
			rc.Cluster.Spec.Topology.Type = "dedicatedMasters"
			masters := rc.Cluster.Spec.Components.Extra[kindMaster]
			masters.Replicas = 3
			rc.Cluster.Spec.Components.Extra[kindMaster] = masters
			for _, kind := range []string{kindKibana, kindLogstash, kindAPM} {
				c := rc.Cluster.Spec.Components.Extra[kind]
				c.Replicas = 1
				rc.Cluster.Spec.Components.Extra[kind] = c
			}
			es.master = "demom0"
			coreConverge(t, p, rc, f)
			target := engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: include}
			build := target.TargetBuild
			fleetAgents(t, rc, &build)
			rc.Cluster.Status.Members = []domain.MemberStatus{{Name: "demon1", Agent: domain.AgentStatus{Healthy: true, Build: target.TargetBuild}}}
			payload, _ := json.Marshal(target)
			op := &domain.Operation{ID: "fleet", Kind: domain.OperationUpgradeAgents, Payload: payload}
			r, err := p.RunOperation(t.Context(), rc, op)
			if err != nil {
				t.Fatal(err)
			}
			op.Checkpoint = r.Checkpoint
			s, _ := load(rc.Cluster)
			if s.Rolling.AgentTarget == nil || s.Rolling.AgentTarget.TargetBuild != target.TargetBuild {
				t.Fatal("target not frozen")
			}
			before := len(f.calls)
			r, err = p.RunOperation(t.Context(), rc, op)
			if err != nil {
				t.Fatal(err)
			}
			op.Checkpoint = r.Checkpoint
			s, _ = load(rc.Cluster)
			want := []string{"demon0", "demon1", "demom1", "demom2", "demom0", "demokb", "demols", "demoapm"}
			if !include {
				want = slices.Delete(want, 1, 2)
			}
			if !slices.Equal(s.Rolling.Order, want) {
				t.Fatalf("order %v want %v", s.Rolling.Order, want)
			}
			// The native shard gate precedes allocation restriction and deployment.
			es.health = "red"
			r, err = p.RunOperation(t.Context(), rc, op)
			if err != nil {
				t.Fatal(err)
			}
			op.Checkpoint = r.Checkpoint
			s, _ = load(rc.Cluster)
			if s.Rolling.Step != "prepare" || len(f.calls) != before {
				t.Fatal("disrupted past shard gate")
			}
			es.health = "green"
			checked := false
			complete := false
			for range 200 {
				s, _ = load(rc.Cluster)
				if s.Rolling != nil && s.Rolling.Step == "rejoin" && !checked {
					build = "old"
					r, err = p.RunOperation(t.Context(), rc, op)
					if err != nil {
						t.Fatal(err)
					}
					op.Checkpoint = r.Checkpoint
					s, _ = load(rc.Cluster)
					if s.Rolling.Step != "rejoin" {
						t.Fatal("advanced without exact build")
					}
					build = target.TargetBuild
					checked = true
				}
				r, err = p.RunOperation(t.Context(), rc, op)
				if err != nil {
					t.Fatal(err)
				}
				op.Checkpoint = r.Checkpoint
				// Reload the durable state each turn; includeCurrent must not repeat Done.
				// Internal is intentionally omitted from the public cluster JSON.
				internal := slices.Clone(rc.Cluster.Internal)
				rc.Cluster = cloneCluster(t, rc.Cluster)
				rc.Cluster.Internal = internal
				if r.Done {
					complete = true
					break
				}
			}
			if !complete || !checked {
				t.Fatal("rollout did not complete", string(op.Checkpoint))
			}
			var deployed []string
			for _, call := range f.calls[before:] {
				if name, ok := strings.CutPrefix(call, "deploy:"); ok {
					deployed = append(deployed, name)
				}
			}
			if !slices.Equal(deployed, want) {
				t.Fatalf("deployments %v want %v", deployed, want)
			}
			s, _ = load(rc.Cluster)
			for _, m := range s.Members {
				if m.Version != rc.Cluster.Spec.Version {
					t.Fatal("changed engine version")
				}
			}
			if !slices.Contains(p.Describe().Operations, domain.OperationUpgradeAgents) {
				t.Fatal("not advertised")
			}
		})
	}
}

func TestAgentUpgradeFenceAndCancellation(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	coreConverge(t, p, rc, f)
	payload, _ := json.Marshal(engine.AgentUpgradePayload{TargetBuild: "different"})
	op := &domain.Operation{ID: "fleet", Kind: domain.OperationUpgradeAgents, Payload: payload}
	before := f.mutations
	if _, err := p.RunOperation(t.Context(), rc, op); err == nil || !strings.Contains(err.Error(), "re-run") {
		t.Fatal(err)
	}
	if f.mutations != before {
		t.Fatal("disrupted mismatched target")
	}
	payload, _ = json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build})
	op.Payload = payload
	r, err := p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = r.Checkpoint
	if err := p.CanCancelOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	r, err = p.CancelOperation(t.Context(), rc, op)
	if err != nil || !r.Done {
		t.Fatal(r, err)
	}
	s, _ := load(rc.Cluster)
	if s.Rolling != nil {
		t.Fatal("cancel left rolling work")
	}
	op.Checkpoint, _ = json.Marshal(operationState{Waiting: "demon0"})
	if err := p.CanCancelOperation(t.Context(), op); err == nil {
		t.Fatal("accepted in-flight cancellation")
	}
}

func TestRollingMaintenanceStartOnly(t *testing.T) {
	for _, reason := range []string{"upgrade", "config", "resources", "agents", "restart"} {
		t.Run(reason, func(t *testing.T) {
			p, rc, f, _ := coreSetup(t, 1)
			s := coreConverge(t, p, rc, f)
			closeFleetWindow(rc)
			target := engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build}
			build := target.TargetBuild
			fleetAgents(t, rc, &build)
			m := s.Members[0]
			s.Rolling = &rolling{Reason: reason, Version: m.Version, AgentTarget: &target, Order: []string{m.Name}, Current: m.Name, Step: "prepare"}
			before := f.mutations
			if _, _, err := p.rollingStep(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			if s.Rolling.Step != "prepare" || f.mutations != before {
				t.Fatal("closed window started disruption")
			}
			// Allocation was already restricted: closing a window cannot strand it.
			s.Rolling.Step = "allocation"
			if _, _, err := p.rollingStep(t.Context(), rc, s); err != nil || s.Rolling.Step != "recover" {
				t.Fatal("allocation recovery gated", err)
			}
			if _, _, err := p.rollingStep(t.Context(), rc, s); err != nil || s.Rolling.Current != "" {
				t.Fatal("shard recovery gated", err)
			}
			rc.Cluster.MaintenanceOverrideGeneration = rc.Cluster.Generation
			rc.Cluster.Generation = 10
			rc.Cluster.MaintenanceOverrideGeneration = 10
			s.Rolling.Current = m.Name
			s.Rolling.Step = "prepare"
			if _, _, err := p.rollingStep(t.Context(), rc, s); err != nil || s.Rolling.Step != "flush" {
				t.Fatal("override not honored", err)
			}
		})
	}
}

func TestMaintenanceScaleAndCreationExemption(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 2)
	s := coreConverge(t, p, rc, f)
	closeFleetWindow(rc)
	rc.Cluster.Spec.Components.Engine.Replicas = 1
	if changed, err := p.scale(t.Context(), rc, s); err != nil || !changed {
		t.Fatal(changed, err)
	}
	for _, m := range s.Members {
		if m.Removing {
			t.Fatal("scale-in started outside window")
		}
	}
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	if changed, err := p.scale(t.Context(), rc, s); err != nil || !changed || len(s.esNodes()) != 3 {
		t.Fatal("scale-out gated", changed, err)
	}
}

func TestDatabaseLeafMaintenanceThresholds(t *testing.T) {
	for _, days := range []int{8, 7, 1, -1} {
		t.Run(time.Duration(days).String(), func(t *testing.T) {
			p, rc, f, _ := coreSetup(t, 1)
			s := coreConverge(t, p, rc, f)
			closeFleetWindow(rc)
			certPEM, _ := secret(t.Context(), rc, secretCA)
			keyPEM, _ := secret(t.Context(), rc, secretCAKey)
			ca, err := LoadCA(certPEM, keyPEM)
			if err != nil {
				t.Fatal(err)
			}
			m := s.Members[0]
			for _, kind := range []string{"http", "transport"} {
				base := "tls/nodes/" + m.Name + "/" + kind
				old, _ := secret(t.Context(), rc, base+".crt")
				block, _ := pem.Decode(old)
				leaf, err := x509.ParseCertificate(block.Bytes)
				if err != nil {
					t.Fatal(err)
				}
				leaf.NotBefore = now(rc).Add(-24 * time.Hour)
				leaf.NotAfter = now(rc).Add(time.Duration(days) * 24 * time.Hour)
				der, err := x509.CreateCertificate(rand.Reader, leaf, ca.Cert, leaf.PublicKey, ca.Key)
				if err != nil {
					t.Fatal(err)
				}
				bad := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
				if err := putSecret(t.Context(), rc, base+".crt", bad); err != nil {
					t.Fatal(err)
				}
				if err := p.issueLeafs(t.Context(), rc, s, m, false); err != nil {
					t.Fatal(err)
				}
				got, _ := secret(t.Context(), rc, base+".crt")
				if renewed := !bytes.Equal(got, bad); renewed != (days <= 7) {
					t.Fatalf("%s days=%d renewed=%v", kind, days, renewed)
				}
			}
		})
	}
}

func TestAgentUpgradeMasterQuorumGates(t *testing.T) {
	p, rc, f, es := coreSetup(t, 3)
	s := coreConverge(t, p, rc, f)
	target := engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build}
	for _, fault := range []string{"missing-master", "committed-quorum"} {
		t.Run(fault, func(t *testing.T) {
			s.Rolling = &rolling{Reason: "agents", AgentTarget: &target, Order: []string{"demon0"}, Current: "demon0", Step: "prepare"}
			if fault == "missing-master" {
				delete(f.nodes, "demon2")
				defer func() { f.nodes["demon2"] = rc.Cluster.Spec.Version }()
			} else {
				es.voters = []string{"demon0", "demon1", "missing1", "missing2", "missing3"}
			}
			before, writes := f.mutations, len(es.mutations)
			if _, _, err := p.rollingStep(t.Context(), rc, s); err == nil {
				t.Fatal("native master gate bypassed")
			}
			if s.Rolling.Step != "prepare" || f.mutations != before || len(es.mutations) != writes {
				t.Fatal("disruption preceded master/quorum gate")
			}
		})
	}
}

func TestMaintenanceEmergencyRecoveryExemptions(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	closeFleetWindow(rc)
	s := coreConverge(t, p, rc, f)
	m := s.Members[0]
	// New cluster provisioning and stopped runtime healing do not need a window.
	f.services[m.Name].Status = infra.StatusStopped
	if busy, err := p.recoverRuntimes(t.Context(), rc, s); err != nil || !busy {
		t.Fatal("healing gated", busy, err)
	}
	certPEM, _ := secret(t.Context(), rc, secretCA)
	keyPEM, _ := secret(t.Context(), rc, secretCAKey)
	ca, err := LoadCA(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	base := "tls/agents/" + m.Name
	old, _ := secret(t.Context(), rc, base+".crt")
	bad := invalidTransportLeaf(t, ca, old, false)
	if err := putSecret(t.Context(), rc, base+".crt", bad); err != nil {
		t.Fatal(err)
	}
	rc.Agents = nil
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	got, _ := secret(t.Context(), rc, base+".crt")
	if bytes.Equal(got, bad) {
		t.Fatal("agent transport certificate renewal gated")
	}
}
