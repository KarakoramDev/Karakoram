// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type agentRolloutBackend struct {
	*fakeBackend
	rc        *engine.Context
	names     []string
	builds    map[string]string
	runtimes  map[string]infra.RuntimeSpec
	holdBuild bool
	writer    string
}

func (b *agentRolloutBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	s, _ := load(b.rc.Cluster)
	m := s.find(name)
	if m.Role == "data" && clustered(s.Topology) {
		if s.Topology == "group" && b.leader == name {
			return infra.Op{}, fmt.Errorf("writer %s was not handed off", name)
		}
		if s.Topology == "cluster" && name == b.writer && !m.ProxyDraining {
			return infra.Op{}, fmt.Errorf("PXC writer %s was not drained", name)
		}
	}
	b.names = append(b.names, name)
	b.runtimes[name] = d.Runtime
	op, err := b.fakeBackend.Deploy(ctx, name, d)
	if err == nil && !b.holdBuild {
		b.builds[name] = engine.CurrentAgentBuild().Build
	}
	return op, err
}

func setupAgentRollout(t *testing.T, topology string) (*Core, *engine.Context, *agentRolloutBackend) {
	t.Helper()
	var p *Core
	var rc *engine.Context
	var fake *fakeBackend
	if topology == "cluster" {
		p, rc, fake = setupPXC(t)
	} else {
		p, rc, fake = setup(t, topology)
	}
	converge(t, p, rc, fake)
	b := &agentRolloutBackend{fakeBackend: fake, rc: rc, builds: map[string]string{}, runtimes: map[string]infra.RuntimeSpec{}, writer: fake.leader}
	prior := fake.action
	fake.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path == "/v1/status" {
			json.NewEncoder(w).Encode(agentapi.Status{Agent: agentapi.AgentBuild{Build: b.builds[name]}, ConfigVersion: fake.applied[name], MySQLRestoreCertification: true})
			return true
		}
		if topology == "cluster" && r.URL.Path == "/v1/mysql/proxy-status" {
			current, _ := load(rc.Cluster)
			if writer := current.find(fake.leader); writer != nil && writer.ProxyDraining {
				// Simulate ProxySQL's Galera monitor promoting the first
				// remaining eligible backend after configuration withdrawal.
				for _, peer := range current.data() {
					if !peer.ProxyDraining && fake.native[peer.Name].MemberState == "ONLINE" {
						fake.leader = peer.Name
						break
					}
				}
			}
		}
		return prior != nil && prior(w, r, name)
	}
	s, _ := load(rc.Cluster)
	for _, m := range s.Members {
		b.builds[m.Name] = "old"
	}
	rc.Backend = b
	return p, rc, b
}

func mysqlAgentRolloutOperation(include bool) *domain.Operation {
	raw, _ := json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, TargetVersion: engine.CurrentAgentBuild().Version, IncludeCurrent: include})
	return &domain.Operation{ID: "agent-rollout", Kind: domain.OperationUpgradeAgents, Payload: raw}
}

func agentRolloutTick(t *testing.T, rc *engine.Context, b *agentRolloutBackend, op *domain.Operation, cancel bool) engine.OpResult {
	t.Helper()
	p := New(Options{Provider: rc.Cluster.Spec.Provider})
	var result engine.OpResult
	var err error
	if cancel {
		result, err = p.CancelOperation(t.Context(), rc, op)
	} else {
		result, err = p.RunOperation(t.Context(), rc, op)
	}
	if err != nil {
		t.Fatal(err)
	}
	if result.Checkpoint != nil {
		op.Checkpoint = result.Checkpoint
	}
	b.apply(t, p, rc)
	return result
}

func finishAgentRollout(t *testing.T, rc *engine.Context, b *agentRolloutBackend, op *domain.Operation, cancel bool) {
	t.Helper()
	for range 100 {
		if agentRolloutTick(t, rc, b, op, cancel).Done {
			return
		}
	}
	t.Fatalf("agent rollout did not finish: %s", op.Checkpoint)
}

func TestAgentUpgradeGRProxyFirstWriterLastReplay(t *testing.T) {
	for _, include := range []bool{false, true} {
		t.Run(fmt.Sprint(include), func(t *testing.T) {
			_, rc, b := setupAgentRollout(t, "group")
			original, _ := load(rc.Cluster)
			b.builds["demom1"] = engine.CurrentAgentBuild().Build
			before, _ := json.Marshal(original.Members)
			runtimes := map[string]infra.RuntimeSpec{}
			for _, m := range original.Members {
				spec, err := New(Options{}).runtimeSpec(rc, m)
				if err != nil {
					t.Fatal(err)
				}
				runtimes[m.Name] = spec
			}
			op := mysqlAgentRolloutOperation(include)
			finishAgentRollout(t, rc, b, op, false)
			want := []string{"demomx", "demom2", "demom0"}
			if include {
				want = []string{"demomx", "demom1", "demom2", "demom0"}
			}
			if !reflect.DeepEqual(b.names, want) {
				t.Fatalf("deploy order %v, want %v", b.names, want)
			}
			if b.leader == "demom0" {
				t.Fatal("writer was not handed off")
			}
			final, _ := load(rc.Cluster)
			after, _ := json.Marshal(final.Members)
			if string(before) != string(after) {
				t.Fatal("rollout changed member versions, resources, identities or configuration")
			}
			for name, runtime := range b.runtimes {
				if !reflect.DeepEqual(runtime, runtimes[name]) {
					t.Fatalf("runtime changed for %s", name)
				}
			}
			// Replay the terminal journal after losing a completion response.
			if !agentRolloutTick(t, rc, b, op, false).Done || !reflect.DeepEqual(b.names, want) {
				t.Fatal("terminal replay redeployed members")
			}
			var cp agentUpgradeCheckpoint
			json.Unmarshal(op.Checkpoint, &cp)
			if len(cp.Done) != len(cp.Order) {
				t.Fatalf("missing completed-member journal: %+v", cp)
			}
			// The completed journal wins over selection, even an explicit
			// include-current refresh replaying an older iterator.
			cp.Index = 0
			op.Checkpoint, _ = json.Marshal(cp)
			finishAgentRollout(t, rc, b, op, false)
			if !reflect.DeepEqual(b.names, want) {
				t.Fatal("completed journal did not prevent duplicate refresh", b.names)
			}
		})
	}
}

func TestAgentUpgradeSingleNoHandoff(t *testing.T) {
	_, rc, b := setupAgentRollout(t, "single")
	prior := b.action
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path == "/v1/mysql/switchover" {
			t.Error("single topology attempted handoff")
			return false
		}
		return prior(w, r, name)
	}
	op := mysqlAgentRolloutOperation(false)
	finishAgentRollout(t, rc, b, op, false)
	if !reflect.DeepEqual(b.names, []string{"demom0"}) {
		t.Fatal(b.names)
	}
}

func TestAgentUpgradeExactBuildFenceAndCancel(t *testing.T) {
	p, rc, b := setupAgentRollout(t, "group")
	bad := mysqlAgentRolloutOperation(false)
	bad.Payload = json.RawMessage(`{"targetBuild":"different"}`)
	if _, err := p.RunOperation(t.Context(), rc, bad); err == nil || !strings.Contains(err.Error(), "differs from running") {
		t.Fatalf("missing build fence: %v", err)
	}
	if len(b.names) != 0 {
		t.Fatal("build mismatch disrupted a member")
	}
	op := mysqlAgentRolloutOperation(false)
	b.holdBuild = true
	for len(b.names) == 0 {
		agentRolloutTick(t, rc, b, op, false)
	}
	inFlight := b.names[0]
	for range 3 {
		result := agentRolloutTick(t, rc, b, op, true)
		if result.Done {
			t.Fatal("cancel abandoned in-flight member")
		}
	}
	if len(b.names) != 1 {
		t.Fatal("stale build advanced rollout", b.names)
	}
	b.builds[inFlight] = engine.CurrentAgentBuild().Build
	finishAgentRollout(t, rc, b, op, true)
	if len(b.names) != 1 {
		t.Fatal("cancel disrupted another member", b.names)
	}
}

func TestAgentUpgradeCrashDuringDeployment(t *testing.T) {
	_, rc, b := setupAgentRollout(t, "group")
	op := mysqlAgentRolloutOperation(false)
	for len(b.names) < 2 {
		saved := slices.Clone(op.Checkpoint)
		agentRolloutTick(t, rc, b, op, false)
		if len(b.names) == 2 {
			// Lose only the response containing the deployment receipt. Cluster and
			// backend state survive; the in-flight deploy intent remains durable.
			op.Checkpoint = saved
			break
		}
	}
	finishAgentRollout(t, rc, b, op, false)
	if !reflect.DeepEqual(b.names, []string{"demomx", "demom1", "demom2", "demom0"}) {
		t.Fatal("crash replay skipped/redeployed a member", b.names)
	}
}

type agentRolloutClock struct{ at time.Time }

func (c agentRolloutClock) Now() time.Time { return c.at }
func closeMySQLAgentWindow(rc *engine.Context) {
	at := time.Now().UTC()
	rc.Clock = agentRolloutClock{at: at}
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 " + at.Add(2*time.Hour).Format("15") + " * * *", Duration: "1h"}}}
}

func TestAgentUpgradeWindowBeforeEveryMember(t *testing.T) {
	_, rc, b := setupAgentRollout(t, "group")
	op := mysqlAgentRolloutOperation(false)
	agentRolloutTick(t, rc, b, op, false)
	for completed := range 4 {
		closeMySQLAgentWindow(rc)
		result := agentRolloutTick(t, rc, b, op, false)
		if result.Done || !strings.Contains(result.Message, "maintenance window") || len(b.names) != completed {
			t.Fatalf("member %d ignored window: %+v %v", completed, result, b.names)
		}
		rc.Cluster.Spec.Maintenance = nil
		for len(b.names) == completed {
			agentRolloutTick(t, rc, b, op, false)
		}
		closeMySQLAgentWindow(rc)
		// Already-submitted member completion is independent of the window.
		result = agentRolloutTick(t, rc, b, op, false)
		var cp agentUpgradeCheckpoint
		json.Unmarshal(result.Checkpoint, &cp)
		if cp.Member != "" || cp.Index != completed+1 {
			t.Fatalf("window stranded member: %+v", cp)
		}
	}
	if !agentRolloutTick(t, rc, b, op, false).Done {
		t.Fatal("final rollout not complete")
	}
}

func TestAgentUpgradeGRNativeGates(t *testing.T) {
	for _, failure := range []string{"RECOVERING", "no-primary", "minority"} {
		t.Run(failure, func(t *testing.T) {
			_, rc, b := setupAgentRollout(t, "group")
			s, _ := load(rc.Cluster)
			m := s.find("demom1")
			switch failure {
			case "RECOVERING":
				st := b.native["demom2"]
				st.MemberState = "RECOVERING"
				b.native["demom2"] = st
			case "no-primary":
				for name, st := range b.native {
					st.MemberRole = "SECONDARY"
					st.ReadOnly = true
					st.SuperReadOnly = true
					b.native[name] = st
				}
			case "minority":
				s.Members = []*member{s.find("demom0"), m}
			}
			if err := agentUpgradeGate(t.Context(), rc, s, m); err == nil {
				t.Fatal("unsafe GR gate accepted")
			}
			if len(b.names) != 0 {
				t.Fatal("native gate disrupted a member")
			}
		})
	}
}

func TestAgentUpgradePXCNativeAndHistoryGates(t *testing.T) {
	for _, failure := range []string{"not-synced", "non-primary", "not-ready", "disconnected", "wrong-size", "latched"} {
		t.Run(failure, func(t *testing.T) {
			p, rc, b := setupAgentRollout(t, "cluster")
			s, _ := load(rc.Cluster)
			if failure == "latched" {
				s.GTIDIncidents = append(s.GTIDIncidents, gtidIncident{DetectedAt: time.Now().UTC(), Members: []string{"demom2"}})
				if err := persistGTIDHistory(t.Context(), rc, s); err != nil {
					t.Fatal(err)
				}
			} else {
				prior := b.action
				b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
					if r.URL.Path == "/v1/mysql/galera-status" && name == "demom2" {
						native := myspec.GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, ClusterSize: 3, StateUUID: "11111111-2222-3333-4444-555555555555", LastCommitted: 10, GTIDExecuted: b.native[name].GTIDExecuted, ServerUUID: b.native[name].ServerUUID}
						switch failure {
						case "not-synced":
							native.LocalStateComment = "Donor/Desynced"
						case "non-primary":
							native.ClusterStatus = "non-Primary"
						case "not-ready":
							native.Ready = false
						case "disconnected":
							native.Connected = false
						case "wrong-size":
							native.ClusterSize = 2
						}
						json.NewEncoder(w).Encode(native)
						return true
					}
					return prior(w, r, name)
				}
			}
			// The proxy phase is independently safe. The data gate must still refuse.
			b.builds["demomx"] = engine.CurrentAgentBuild().Build
			op := mysqlAgentRolloutOperation(false)
			for range 2 {
				agentRolloutTick(t, rc, b, op, false)
			}
			_, err := p.RunOperation(t.Context(), rc, op)
			if err == nil {
				t.Fatal("unsafe PXC data rollout accepted")
			}
			if failure == "latched" && !strings.Contains(err.Error(), "latched") {
				t.Fatal(err)
			}
			if len(b.names) != 0 {
				t.Fatal("unsafe PXC gate disrupted a member", b.names)
			}
		})
	}
}

func TestAgentUpgradePXCSameVolumeRestartAndWriterHandoff(t *testing.T) {
	_, rc, b := setupAgentRollout(t, "cluster")
	before, _ := load(rc.Cluster)
	identities := map[string]string{}
	for _, m := range before.data() {
		identities[m.Name] = m.VolumeID
	}
	op := mysqlAgentRolloutOperation(false)
	finishAgentRollout(t, rc, b, op, false)
	if !reflect.DeepEqual(b.names, []string{"demomx", "demom1", "demom2", "demom0"}) {
		t.Fatal(b.names)
	}
	after, _ := load(rc.Cluster)
	for _, m := range after.data() {
		if m.VolumeID != identities[m.Name] || !m.Initialized || m.GaleraJoinFence || m.ProxyDraining || !pxcJoinAllowed(after, m) {
			t.Fatalf("same-volume restart lost admission/identity: %+v", m)
		}
	}
	if mapping := after.Formation.GTIDCertification; mapping != nil && len(mapping.EmptyMembers) > 0 {
		t.Fatal("restart acquired empty-member replacement authority")
	}
}

func TestAgentUpgradeRefusesStructuralOwners(t *testing.T) {
	for _, owner := range []string{"rolling", "replacement", "pending", "restore", "recovery", "reformation", "import", "scale", "proxy-scale"} {
		t.Run(owner, func(t *testing.T) {
			p, rc, b := setupAgentRollout(t, "group")
			s, _ := load(rc.Cluster)
			switch owner {
			case "rolling":
				s.Rolling = &rollingState{}
			case "replacement":
				s.Replacement = &replacementState{}
			case "pending":
				s.Pending = &pending{}
			case "restore":
				s.RestoreGTIDBlocked = "unresolved restore"
			case "recovery":
				s.Formation.State = "recovering"
			case "reformation":
				s.Formation.Reformation = &reformation{State: "observing"}
			case "import":
				s.data()[0].ImportBatch = &engine.ImportPlan{}
			case "proxy-scale":
				component := rc.Cluster.Spec.Components.Extra["proxy"]
				component.Replicas++
				rc.Cluster.Spec.Components.Extra["proxy"] = component
			case "scale":
				rc.Cluster.Spec.Components.Engine.Replicas = 5
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			if _, err := p.RunOperation(t.Context(), rc, mysqlAgentRolloutOperation(false)); err == nil {
				t.Fatal("structural owner admitted rollout")
			}
			if len(b.names) > 0 {
				t.Fatal("structural owner caused disruption")
			}
		})
	}
}

func TestAgentUpgradeWaitsForEveryProxyBuildAndConfig(t *testing.T) {
	for _, fault := range []string{"build", "config", "writer"} {
		t.Run(fault, func(t *testing.T) {
			_, rc, b := setupAgentRollout(t, "group")
			s, _ := load(rc.Cluster)
			proxy := s.find("demomx")
			proxy.Replicas = 2
			component := rc.Cluster.Spec.Components.Extra["proxy"]
			component.Replicas = 2
			rc.Cluster.Spec.Components.Extra["proxy"] = component
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			blockSecond := false
			agents := admissionAgents{fakeAgents: rc.Agents.(fakeAgents), proxy: proxy.Name, peers: map[string]string{}}
			for i, instance := range []string{"first", "second"} {
				server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/v1/status" {
						build, version := b.builds[proxy.Name], proxy.ConfigVersion
						if i == 1 && blockSecond {
							if fault == "build" {
								build = "old"
							}
							if fault == "config" {
								version = 0
							}
						}
						json.NewEncoder(w).Encode(agentapi.Status{Agent: agentapi.AgentBuild{Build: build}, ConfigVersion: version})
						return
					}
					if r.URL.Path == "/v1/reload" {
						json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
						return
					}
					status := myspec.ProxyStatusResponse{Backends: []myspec.ProxyBackend{admitted(b.leader, myspec.ProxyWriterGroup, "ONLINE")}}
					if i == 1 && blockSecond && fault == "writer" {
						status.Backends = nil
					}
					json.NewEncoder(w).Encode(status)
				}))
				t.Cleanup(server.Close)
				agents.ca = append(agents.ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})...)
				agents.peers[instance] = server.Listener.Addr().String()
			}
			rc.Agents = agents
			rc.Backend = &admissionBackend{Backend: b, proxy: proxy.Name, count: 2, stops: map[string]int{}}
			op := mysqlAgentRolloutOperation(false)
			for len(b.names) == 0 {
				agentRolloutTick(t, rc, b, op, false)
			}
			blockSecond = true
			for range 3 {
				result := agentRolloutTick(t, rc, b, op, false)
				var cp agentUpgradeCheckpoint
				json.Unmarshal(result.Checkpoint, &cp)
				if cp.Member != proxy.Name || cp.Index != 0 || len(b.names) != 1 {
					t.Fatalf("advanced with one stale proxy instance: %+v %v", cp, b.names)
				}
			}
			blockSecond = false
			result := agentRolloutTick(t, rc, b, op, false)
			var cp agentUpgradeCheckpoint
			json.Unmarshal(result.Checkpoint, &cp)
			if cp.Member != "" || cp.Index != 1 {
				t.Fatalf("all-instance acknowledgement did not release proxy phase: %+v", cp)
			}
			if !agentRolloutTick(t, rc, b, op, true).Done || len(b.names) != 1 {
				t.Fatal("cancellation after proxy phase disrupted data members")
			}
		})
	}
}

func TestAgentUpgradeDataBuildAndOnlineFences(t *testing.T) {
	_, rc, b := setupAgentRollout(t, "group")
	b.builds["demomx"] = engine.CurrentAgentBuild().Build
	b.holdBuild = true
	op := mysqlAgentRolloutOperation(false)
	for len(b.names) == 0 {
		agentRolloutTick(t, rc, b, op, false)
	}
	name := b.names[0]
	for range 3 {
		result := agentRolloutTick(t, rc, b, op, false)
		var cp agentUpgradeCheckpoint
		json.Unmarshal(result.Checkpoint, &cp)
		if cp.Member != name || cp.Index != 1 {
			t.Fatalf("native ONLINE advanced without target build: %+v", cp)
		}
	}
	b.builds[name] = engine.CurrentAgentBuild().Build
	native := b.native[name]
	native.MemberState = "RECOVERING"
	b.native[name] = native
	result := agentRolloutTick(t, rc, b, op, true)
	var cp agentUpgradeCheckpoint
	json.Unmarshal(result.Checkpoint, &cp)
	if result.Done || cp.Member != name || cp.Index != 1 {
		t.Fatalf("target build advanced while native recovery was unfinished: %+v", cp)
	}
	native.MemberState = "ONLINE"
	b.native[name] = native
	finishAgentRollout(t, rc, b, op, true)
	if !reflect.DeepEqual(b.names, []string{"demom1"}) {
		t.Fatal("cancellation disrupted another data member", b.names)
	}
}

type deferredAgentRolloutBackend struct {
	*agentRolloutBackend
	calls           int
	deferSubmission bool
	accepted        bool
}

func (b *deferredAgentRolloutBackend) Deploy(ctx context.Context, name string, deployment infra.Deployment) (infra.Op, error) {
	b.calls++
	if b.deferSubmission {
		if b.accepted {
			if _, err := b.agentRolloutBackend.Deploy(ctx, name, deployment); err != nil {
				return infra.Op{}, err
			}
		}
		return infra.Op{}, errors.New("deployment reply unavailable")
	}
	return b.agentRolloutBackend.Deploy(ctx, name, deployment)
}

func TestAgentUpgradeCancelUnreceiptedIntentNeverSubmits(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(fmt.Sprint(accepted), func(t *testing.T) {
			p, rc, b := setupAgentRollout(t, "single")
			deferred := &deferredAgentRolloutBackend{agentRolloutBackend: b, deferSubmission: true, accepted: accepted}
			rc.Backend = deferred
			op := mysqlAgentRolloutOperation(false)
			for {
				agentRolloutTick(t, rc, b, op, false)
				var cp agentUpgradeCheckpoint
				json.Unmarshal(op.Checkpoint, &cp)
				if cp.Stage == "deploy" {
					break
				}
			}
			if _, err := p.RunOperation(t.Context(), rc, op); err == nil {
				t.Fatal("missing simulated lost/deferred deployment reply")
			}
			if accepted {
				native := b.native["demom0"]
				native.MemberState = "OFFLINE"
				b.native["demom0"] = native
				result := agentRolloutTick(t, rc, b, op, true)
				var cp agentUpgradeCheckpoint
				json.Unmarshal(result.Checkpoint, &cp)
				if result.Done || cp.Member == "" {
					t.Fatal("cancel abandoned an accepted unreceipted deployment")
				}
			}
			finishAgentRollout(t, rc, b, op, true)
			if deferred.calls != 1 {
				t.Fatalf("cancel submitted a new deployment: %d calls", deferred.calls)
			}
			if !accepted && len(b.names) != 0 {
				t.Fatal("cancel started a disruption")
			}
		})
	}
}

func TestAgentUpgradeDeferredSubmissionRechecksWindowAndSafety(t *testing.T) {
	p, rc, b := setupAgentRollout(t, "group")
	b.builds["demomx"] = engine.CurrentAgentBuild().Build
	deferred := &deferredAgentRolloutBackend{agentRolloutBackend: b, deferSubmission: true}
	rc.Backend = deferred
	op := mysqlAgentRolloutOperation(false)
	for {
		agentRolloutTick(t, rc, b, op, false)
		var cp agentUpgradeCheckpoint
		json.Unmarshal(op.Checkpoint, &cp)
		if cp.Stage == "deploy" {
			break
		}
	}
	// Window closure after the durable intent must still block the first call.
	closeMySQLAgentWindow(rc)
	result := agentRolloutTick(t, rc, b, op, false)
	if deferred.calls != 0 || !strings.Contains(result.Message, "maintenance window") {
		t.Fatal("pre-deploy intent bypassed window")
	}
	rc.Cluster.Spec.Maintenance = nil
	if _, err := p.RunOperation(t.Context(), rc, op); err == nil {
		t.Fatal("missing deferred submission")
	}
	closeMySQLAgentWindow(rc)
	agentRolloutTick(t, rc, b, op, false)
	if deferred.calls != 1 {
		t.Fatal("deferred submission bypassed closed window")
	}
	rc.Cluster.Spec.Maintenance = nil
	native := b.native["demom2"]
	native.MemberState = "RECOVERING"
	b.native["demom2"] = native
	if _, err := p.RunOperation(t.Context(), rc, op); err == nil || deferred.calls != 1 {
		t.Fatal("deferred submission bypassed native safety")
	}
	native.MemberState = "ONLINE"
	b.native["demom2"] = native
	deferred.deferSubmission = false
	agentRolloutTick(t, rc, b, op, false)
	finishAgentRollout(t, rc, b, op, true)
	if deferred.calls != 2 || !reflect.DeepEqual(b.names, []string{"demom1"}) {
		t.Fatal("deferred deployment did not resume safely", deferred.calls, b.names)
	}
}

func TestAgentUpgradeBuildChangeCannotEscapeThroughSync(t *testing.T) {
	p, rc, b := setupAgentRollout(t, "group")
	op := mysqlAgentRolloutOperation(false)
	for {
		agentRolloutTick(t, rc, b, op, false)
		var cp agentUpgradeCheckpoint
		json.Unmarshal(op.Checkpoint, &cp)
		if cp.Stage == "deploy" {
			cp.Target.TargetBuild = "previous-control-plane-build"
			op.Checkpoint, _ = json.Marshal(cp)
			break
		}
	}
	if _, err := p.RunOperation(t.Context(), rc, op); err == nil {
		t.Fatal("changed control-plane build was not fenced")
	}
	for range 2 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		b.apply(t, p, rc)
	}
	if len(b.names) != 0 {
		t.Fatal("ordinary Sync deployed a failed frozen agent rollout", b.names)
	}
}

func TestAgentUpgradeWaitHealsRetainedPeerWithoutSync(t *testing.T) {
	_, rc, b := setupAgentRollout(t, "group")
	b.builds["demomx"] = engine.CurrentAgentBuild().Build
	op := mysqlAgentRolloutOperation(false)
	for len(b.names) == 0 {
		agentRolloutTick(t, rc, b, op, false)
	}
	native := b.native["demom2"]
	native.MemberState = "OFFLINE"
	b.native["demom2"] = native
	agentRolloutTick(t, rc, b, op, false)
	if b.native["demom2"].MemberState != "ONLINE" {
		t.Fatal("operation wait did not reuse retained-identity peer rejoin")
	}
	finishAgentRollout(t, rc, b, op, true)
	if !reflect.DeepEqual(b.names, []string{"demom1"}) {
		t.Fatal("peer healing redeployed an unrelated member", b.names)
	}
}

func TestAgentUpgradeWindowDistinguishesWriterSelectionFromHandoff(t *testing.T) {
	_, rc, b := setupAgentRollout(t, "group")
	for name := range b.builds {
		b.builds[name] = engine.CurrentAgentBuild().Build
	}
	b.builds["demom0"] = "old"
	op := mysqlAgentRolloutOperation(false)
	for {
		agentRolloutTick(t, rc, b, op, false)
		var cp agentUpgradeCheckpoint
		json.Unmarshal(op.Checkpoint, &cp)
		if cp.Member == "demom0" && cp.HandoffTarget != "" {
			if cp.Started {
				t.Fatal("writer selection was incorrectly marked disruptive")
			}
			break
		}
	}
	closeMySQLAgentWindow(rc)
	result := agentRolloutTick(t, rc, b, op, false)
	if !strings.Contains(result.Message, "maintenance window") || b.leader != "demom0" || len(b.names) != 0 {
		t.Fatal("writer selection bypassed window before actual handoff")
	}
	rc.Cluster.Spec.Maintenance = nil
	result = agentRolloutTick(t, rc, b, op, false)
	var cp agentUpgradeCheckpoint
	json.Unmarshal(result.Checkpoint, &cp)
	if !cp.Started || b.leader == "demom0" {
		t.Fatal("native handoff was not checkpointed as started")
	}
	closeMySQLAgentWindow(rc)
	finishAgentRollout(t, rc, b, op, false)
	if !reflect.DeepEqual(b.names, []string{"demom0"}) {
		t.Fatal("window closure stranded handed-off writer", b.names)
	}
}
