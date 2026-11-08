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
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type agentRolloutBackend struct {
	*fakeBackend
	rc        *engine.Context
	names     []string
	builds    map[string]string
	runtimes  map[string]infra.RuntimeSpec
	holdBuild bool
}

func (b *agentRolloutBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	s, _ := load(b.rc.Cluster)
	m := s.find(name)
	if m.Role == "data" && clustered(s.Topology) {
		if s.Topology == "group" && b.leader == name {
			return infra.Op{}, fmt.Errorf("writer %s was not handed off", name)
		}
		if s.Topology == "cluster" && name == b.leader && !m.ProxyDraining {
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
	b := &agentRolloutBackend{fakeBackend: fake, rc: rc, builds: map[string]string{}, runtimes: map[string]infra.RuntimeSpec{}}
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

func agentRolloutTwoProxyInstances(t *testing.T, rc *engine.Context, b *agentRolloutBackend, stale *[2]bool) *[2]string {
	t.Helper()
	s, _ := load(rc.Cluster)
	proxy := s.find("demomx")
	proxy.Replicas = 2
	component := rc.Cluster.Spec.Components.Extra["proxy"]
	component.Replicas = 2
	rc.Cluster.Spec.Components.Extra["proxy"] = component
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	writers := new([2]string)
	agents := admissionAgents{fakeAgents: rc.Agents.(fakeAgents), proxy: proxy.Name, peers: map[string]string{}}
	for i, instance := range []string{"first", "second"} {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/v1/status":
				current, _ := load(rc.Cluster)
				version := current.find(proxy.Name).ConfigVersion
				if stale[i] {
					version = 0
				}
				json.NewEncoder(w).Encode(agentapi.Status{Agent: agentapi.AgentBuild{Build: b.builds[proxy.Name]}, ConfigVersion: version})
			case "/v1/reload":
				json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
			default:
				var status myspec.ProxyStatusResponse
				current, _ := load(rc.Cluster)
				writer := b.leader
				if writers[i] != "" {
					writer = writers[i]
				}
				for name, native := range b.native {
					if native.MemberState != "ONLINE" || current.find(name).ProxyDraining {
						continue
					}
					group := myspec.ProxyReaderGroup
					if name == writer {
						group = myspec.ProxyWriterGroup
					}
					status.Backends = append(status.Backends, admitted(name, group, "ONLINE"))
				}
				json.NewEncoder(w).Encode(status)
			}
		}))
		t.Cleanup(server.Close)
		agents.ca = append(agents.ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})...)
		agents.peers[instance] = server.Listener.Addr().String()
	}
	rc.Agents = agents
	rc.Backend = &admissionBackend{Backend: b, proxy: proxy.Name, count: 2, stops: map[string]int{}}
	return writers
}

func TestAgentUpgradeLostReceiptUsesRecordedBuildRegardlessOfIncludeCurrent(t *testing.T) {
	for _, role := range []string{"data", "proxy"} {
		for _, include := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/include=%v", role, include), func(t *testing.T) {
				topology := "single"
				if role == "proxy" {
					topology = "group"
				}
				_, rc, b := setupAgentRollout(t, topology)
				if role == "proxy" {
					agentRolloutTwoProxyInstances(t, rc, b, new([2]bool))
				}
				op := mysqlAgentRolloutOperation(include)
				for {
					agentRolloutTick(t, rc, b, op, false)
					var cp agentUpgradeCheckpoint
					json.Unmarshal(op.Checkpoint, &cp)
					if cp.Stage == "deploy" {
						if len(cp.BeforeBuilds) != map[string]int{"data": 1, "proxy": 2}[role] {
							t.Fatalf("missing pre-upgrade instance builds: %+v", cp)
						}
						break
					}
				}
				intent := slices.Clone(op.Checkpoint)
				agentRolloutTick(t, rc, b, op, false)
				op.Checkpoint = intent // backend accepted, operation receipt was lost
				agentRolloutTick(t, rc, b, op, false)
				finishAgentRollout(t, rc, b, op, true)
				if len(b.names) != 1 {
					t.Fatal("IncludeCurrent repeated a proven changed-build deployment", b.names)
				}
			})
		}
	}
}

func TestAgentUpgradeCurrentRefreshLostReceiptConservativelyRedeploys(t *testing.T) {
	for _, role := range []string{"data", "proxy"} {
		t.Run(role, func(t *testing.T) {
			topology, member := "single", "demom0"
			if role == "proxy" {
				topology, member = "group", "demomx"
			}
			_, rc, b := setupAgentRollout(t, topology)
			if role == "proxy" {
				agentRolloutTwoProxyInstances(t, rc, b, new([2]bool))
			}
			b.builds[member] = engine.CurrentAgentBuild().Build
			op := mysqlAgentRolloutOperation(true)
			for {
				agentRolloutTick(t, rc, b, op, false)
				var cp agentUpgradeCheckpoint
				json.Unmarshal(op.Checkpoint, &cp)
				if cp.Stage == "deploy" {
					break
				}
			}
			intent := slices.Clone(op.Checkpoint)
			agentRolloutTick(t, rc, b, op, false)
			op.Checkpoint = intent
			closeMySQLAgentWindow(rc)
			result := agentRolloutTick(t, rc, b, op, false)
			if len(b.names) != 1 || !strings.Contains(result.Message, "maintenance window") {
				t.Fatal("ambiguous same-build refresh bypassed window")
			}
			rc.Cluster.Spec.Maintenance = nil
			agentRolloutTick(t, rc, b, op, false)
			finishAgentRollout(t, rc, b, op, true)
			if !reflect.DeepEqual(b.names, []string{member, member}) {
				t.Fatal("same-build refresh was incorrectly adopted without evidence", b.names)
			}
		})
	}
}

func TestAgentUpgradeCurrentProxyWaitsForConfigBeforeData(t *testing.T) {
	_, rc, b := setupAgentRollout(t, "group")
	stale := &[2]bool{false, true}
	agentRolloutTwoProxyInstances(t, rc, b, stale)
	b.builds["demomx"] = engine.CurrentAgentBuild().Build
	op := mysqlAgentRolloutOperation(false)
	agentRolloutTick(t, rc, b, op, false)
	for range 3 {
		result := agentRolloutTick(t, rc, b, op, false)
		var cp agentUpgradeCheckpoint
		json.Unmarshal(result.Checkpoint, &cp)
		if cp.Index != 0 || len(b.names) != 0 {
			t.Fatal("current proxy build bypassed stale credentials/config acknowledgement")
		}
	}
	stale[1] = false
	for len(b.names) == 0 {
		agentRolloutTick(t, rc, b, op, false)
	}
	finishAgentRollout(t, rc, b, op, true)
	if !reflect.DeepEqual(b.names, []string{"demom1"}) {
		t.Fatal("proxy readiness gate redeployed a current proxy or skipped data", b.names)
	}
}

func TestAgentUpgradePXCRechecksWriterAfterDeployIntent(t *testing.T) {
	_, rc, b := setupAgentRollout(t, "cluster")
	b.builds["demomx"] = engine.CurrentAgentBuild().Build
	op := mysqlAgentRolloutOperation(false)
	for {
		agentRolloutTick(t, rc, b, op, false)
		var cp agentUpgradeCheckpoint
		json.Unmarshal(op.Checkpoint, &cp)
		if cp.Stage == "deploy" && cp.Member == "demom1" {
			break
		}
	}
	b.leader = "demom1"
	result := agentRolloutTick(t, rc, b, op, false)
	var cp agentUpgradeCheckpoint
	json.Unmarshal(result.Checkpoint, &cp)
	if cp.Stage != "handoff" || len(b.names) != 0 {
		t.Fatal("new PXC routed writer was disrupted without handoff", b.names, cp)
	}
	for len(b.names) == 0 {
		agentRolloutTick(t, rc, b, op, false)
	}
	s, _ := load(rc.Cluster)
	if !s.find("demom1").ProxyDraining || b.leader == "demom1" {
		t.Fatal("new routed writer was deployed before withdrawal and successor admission")
	}
	finishAgentRollout(t, rc, b, op, true)
}

func TestAgentUpgradeOrphanedPXCDrainSettlesWithoutDeployment(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhaseFailed, domain.PhaseCancelled, "missing"} {
		t.Run(string(phase), func(t *testing.T) {
			p, rc, b := setupAgentRollout(t, "cluster")
			for name := range b.builds {
				b.builds[name] = engine.CurrentAgentBuild().Build
			}
			b.builds["demom0"] = "old"
			op := mysqlAgentRolloutOperation(false)
			op.Namespace, op.Cluster = rc.Cluster.Namespace, rc.Cluster.Name
			if phase != "missing" {
				if err := rc.Store.(*store.Store).EnqueueOperation(t.Context(), op); err != nil {
					t.Fatal(err)
				}
				op.Phase = domain.PhaseRunning
				if err := rc.Store.(*store.Store).UpdateOperation(t.Context(), op); err != nil {
					t.Fatal(err)
				}
			}
			for {
				agentRolloutTick(t, rc, b, op, false)
				s, _ := load(rc.Cluster)
				if s.find("demom0").ProxyDraining {
					break
				}
			}
			if phase != "missing" {
				op.Phase = phase
				if err := rc.Store.(*store.Store).UpdateOperation(t.Context(), op); err != nil {
					t.Fatal(err)
				}
			}
			for range 10 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				b.apply(t, p, rc)
				s, _ := load(rc.Cluster)
				if !s.find("demom0").ProxyDraining && s.find("demom0").ProxyDrainOperation == "" {
					break
				}
			}
			s, _ := load(rc.Cluster)
			if s.find("demom0").ProxyDraining || s.find("demom0").ProxyDrainOperation != "" || len(b.names) != 0 {
				t.Fatal("orphan recovery stranded routing or submitted an executable", b.names)
			}
			if b.builds["demom0"] != "old" {
				t.Fatal("orphan recovery replaced the old agent")
			}
			if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || !allowed {
				t.Fatal("orphan recovery did not release operation admission", allowed, err)
			}
			if err := proxyRejoinReady(t.Context(), rc, s, "demom0"); err != nil {
				t.Fatal("orphan recovery completed before writer routing/readmission", err)
			}
		})
	}
}

func TestAgentUpgradePXCDetectsWriterOnAnyProxyInstance(t *testing.T) {
	_, rc, b := setupAgentRollout(t, "cluster")
	writers := agentRolloutTwoProxyInstances(t, rc, b, new([2]bool))
	b.builds["demomx"] = engine.CurrentAgentBuild().Build
	op := mysqlAgentRolloutOperation(false)
	for {
		agentRolloutTick(t, rc, b, op, false)
		var cp agentUpgradeCheckpoint
		json.Unmarshal(op.Checkpoint, &cp)
		if cp.Stage == "deploy" && cp.Member == "demom1" {
			break
		}
	}
	// The logical service still reports m0; one instance now routes m1.
	writers[1] = "demom1"
	result := agentRolloutTick(t, rc, b, op, false)
	var cp agentUpgradeCheckpoint
	json.Unmarshal(result.Checkpoint, &cp)
	if cp.Stage != "handoff" || len(b.names) != 0 {
		t.Fatal("logical proxy observation concealed another instance's writer")
	}
	result = agentRolloutTick(t, rc, b, op, false)
	json.Unmarshal(result.Checkpoint, &cp)
	if cp.HandoffTarget == "" {
		t.Fatal("split proxy routing did not select a safe survivor")
	}
	// Both monitors converge to the selected survivor after withdrawal.
	b.leader = cp.HandoffTarget
	writers[1] = ""
	for len(b.names) == 0 {
		agentRolloutTick(t, rc, b, op, false)
	}
	finishAgentRollout(t, rc, b, op, true)
	if !reflect.DeepEqual(b.names, []string{"demom1"}) {
		t.Fatal("split-routing recovery disrupted another member", b.names)
	}
}

func TestAgentUpgradePXCAdoptsQualifiedRoutedWriter(t *testing.T) {
	_, rc, b := setupAgentRollout(t, "cluster")
	b.leader = "demom2"
	writers := agentRolloutTwoProxyInstances(t, rc, b, new([2]bool))
	op := mysqlAgentRolloutOperation(true)
	var cp agentUpgradeCheckpoint
	for range 100 {
		agentRolloutTick(t, rc, b, op, false)
		json.Unmarshal(op.Checkpoint, &cp)
		s, _ := load(rc.Cluster)
		if s.find("demom2").ProxyDraining {
			break
		}
	}
	if cp.Member != "demom2" || cp.HandoffTarget != "demom0" {
		t.Fatalf("expected first eligible successor before exclusion: %+v", cp)
	}
	before := len(b.names)
	// Galera monitors need not choose the first configured backend. Until
	// every pinned proxy agrees, neither successor is sufficient.
	b.leader = "demom1"
	writers[0], writers[1] = "demom0", "demom1"
	for range 3 {
		result, err := New(Options{Provider: rc.Cluster.Spec.Provider}).RunOperation(t.Context(), rc, op)
		if !errors.Is(err, engine.ErrUnavailable) {
			t.Fatalf("expected split-writer wait, got %v", err)
		}
		if len(result.Checkpoint) > 0 {
			op.Checkpoint = result.Checkpoint
		}
		b.apply(t, New(Options{Provider: rc.Cluster.Spec.Provider}), rc)
	}
	if len(b.names) != before {
		t.Fatal("split proxy writers admitted incumbent deployment")
	}
	writers[0] = "demom1"
	for range 10 {
		agentRolloutTick(t, rc, b, op, false)
		json.Unmarshal(op.Checkpoint, &cp)
		if len(b.names) > before {
			break
		}
	}
	if len(b.names) != before+1 || b.names[before] != "demom2" || cp.HandoffTarget != "demom1" {
		t.Fatalf("rollout did not adopt common qualified writer: deployments=%v checkpoint=%+v", b.names, cp)
	}
	finishAgentRollout(t, rc, b, op, false)
}

func TestAgentUpgradePXCWriterHandoffDeadline(t *testing.T) {
	p, rc, b := setupAgentRollout(t, "cluster")
	op := mysqlAgentRolloutOperation(false)
	for name := range b.builds {
		b.builds[name] = engine.CurrentAgentBuild().Build
	}
	b.builds[b.leader] = "old"
	var cp agentUpgradeCheckpoint
	for range 100 {
		agentRolloutTick(t, rc, b, op, false)
		json.Unmarshal(op.Checkpoint, &cp)
		s, _ := load(rc.Cluster)
		if s.find(cp.Member) != nil && s.find(cp.Member).ProxyDraining {
			break
		}
	}
	if cp.HandoffStartedAt.IsZero() {
		t.Fatal("routing withdrawal did not persist handoff deadline")
	}
	cp.HandoffStartedAt = time.Now().Add(-4 * time.Minute)
	op.Checkpoint, _ = json.Marshal(cp)
	_, err := p.RunOperation(t.Context(), rc, op)
	if err == nil || !strings.Contains(err.Error(), "timed out") || !strings.Contains(err.Error(), "hostgroup 10") || len(b.names) != 0 {
		t.Fatalf("handoff did not fail safely with routing guidance: deployments=%v err=%v", b.names, err)
	}
	// Cancellation/failed-operation cleanup still restores routing without
	// submitting an executable after a deadline.
	finishAgentRollout(t, rc, b, op, true)
	s, _ := load(rc.Cluster)
	if s.find(cp.Member).ProxyDraining || len(b.names) != 0 {
		t.Fatal("deadline cleanup stranded withdrawal or deployed incumbent")
	}
}

func TestAgentUpgradePXCSuccessorRequiresGTIDsAndQualification(t *testing.T) {
	_, rc, b := setupAgentRollout(t, "cluster")
	b.leader = "demom1"
	s, _ := load(rc.Cluster)
	gtids := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"
	native := b.native["demom1"]
	native.GTIDExecuted = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-9"
	b.native["demom1"] = native
	if target, err := agentUpgradePXCSuccessor(t.Context(), rc, s, "demom2", gtids, myspec.ProxyWriterGroup); err == nil || target != "" {
		t.Fatal("adopted routed peer missing required GTIDs", target, err)
	}
	native.GTIDExecuted = gtids
	b.native["demom1"] = native
	if target, err := agentUpgradePXCSuccessor(t.Context(), rc, s, "demom2", gtids, myspec.ProxyWriterGroup); err != nil || target != "demom1" {
		t.Fatal("rejected caught-up routed peer", target, err)
	}
	s.find("demom1").Offline = true
	if target, err := agentUpgradePXCSuccessor(t.Context(), rc, s, "demom2", gtids, myspec.ProxyWriterGroup); err == nil || target != "" {
		t.Fatal("adopted unqualified routed peer", target, err)
	}
}

type servingAgentRolloutBackend struct {
	*agentRolloutBackend
	capable       bool
	marked        bool
	approvalErr   error
	choices       []bool
	receipt       *infra.Op
	fail          bool
	waiting       bool
	requestID     string
	lookupCalls   int
	legacyReceipt bool
}

func (b *servingAgentRolloutBackend) Capabilities() infra.Capabilities {
	c := b.agentRolloutBackend.Capabilities()
	c.ProxyDeploymentPreservesServing = b.capable
	return c
}

func (b *servingAgentRolloutBackend) ProxyUpdatesPreserveServing(context.Context, string) (bool, error) {
	return b.marked, b.approvalErr
}

func (b *servingAgentRolloutBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	if name != "demomx" {
		return b.agentRolloutBackend.Deploy(ctx, name, d)
	}
	if d.ProxyPreserveServing == nil {
		return infra.Op{}, errors.New("proxy upgrade did not freeze rollout policy")
	}
	b.choices = append(b.choices, *d.ProxyPreserveServing)
	if b.receipt != nil {
		return *b.receipt, nil
	}
	op, err := b.agentRolloutBackend.Deploy(ctx, name, d)
	if err == nil {
		b.requestID = d.RequestID
		b.receipt = &op
	}
	return op, err
}

func (b *servingAgentRolloutBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if b.waiting {
		return infra.OpState{Message: "proxy rollout updating"}, nil
	}
	if b.fail {
		return infra.OpState{Failed: true, Message: "proxy rollout paused"}, nil
	}
	return b.agentRolloutBackend.OpStatus(ctx, op)
}

func (b *servingAgentRolloutBackend) LookupProxyDeployment(_ context.Context, name, requestID string) (*infra.Op, error) {
	b.lookupCalls++
	if b.legacyReceipt {
		// Dokploy excludes legacy non-health-gated receipts from serving lookup.
		return nil, nil
	}
	if b.receipt != nil && (name != "demomx" || requestID != b.requestID) {
		return nil, errors.New("proxy rollout request identity mismatch")
	}
	return b.receipt, nil
}

func TestAgentUpgradeProxyServingDeploymentDecisionAndReplay(t *testing.T) {
	for _, tc := range []struct {
		name            string
		capable, marked bool
		want            bool
	}{
		{"supported", true, true, true},
		{"legacy-unmarked", true, false, false},
		{"count-resource-only", false, true, false},
		{"unsupported", false, false, false},
	} {
		for _, include := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/include=%v", tc.name, include), func(t *testing.T) {
				_, rc, b := setupAgentRollout(t, "group")
				if include {
					b.builds["demomx"] = engine.CurrentAgentBuild().Build
				}
				b.holdBuild = true
				backend := &servingAgentRolloutBackend{agentRolloutBackend: b, capable: tc.capable, marked: tc.marked}
				rc.Backend = backend
				op := mysqlAgentRolloutOperation(include)
				var cp agentUpgradeCheckpoint
				for {
					agentRolloutTick(t, rc, b, op, false)
					json.Unmarshal(op.Checkpoint, &cp)
					if cp.Stage == "deploy" {
						break
					}
				}
				if cp.ProxyServing == nil || *cp.ProxyServing != tc.want {
					t.Fatalf("wrong persisted rollout choice: %+v", cp)
				}
				intent := append(json.RawMessage(nil), op.Checkpoint...)
				agentRolloutTick(t, rc, b, op, false)
				if tc.want && !include {
					// Exact changed-build acknowledgement must still recover the
					// serving receipt and its pause/rollback verdict.
					b.builds["demomx"] = engine.CurrentAgentBuild().Build
				}
				// Lose the receipt, not the backend operation. A later runtime or
				// capability observation must not change this in-flight policy.
				op.Checkpoint = intent
				backend.capable, backend.marked = !tc.capable, !tc.marked
				agentRolloutTick(t, rc, b, op, false)
				if !reflect.DeepEqual(backend.choices, []bool{tc.want, tc.want}) || !reflect.DeepEqual(b.names, []string{"demomx"}) {
					t.Fatalf("replay changed policy or double-deployed: %v %v", backend.choices, b.names)
				}
				if b.runtimes["demomx"].Env["KARAKORAM_PROXY_SERVING_PROBE"] != "1" {
					t.Fatal("agent upgrade omitted the serving probe marker")
				}
				b.holdBuild = false
				b.builds["demomx"] = engine.CurrentAgentBuild().Build
				finishAgentRollout(t, rc, b, op, false)
			})
		}
	}
}

func TestAgentUpgradeProxyServingApprovalErrorNeverDeploys(t *testing.T) {
	p, rc, b := setupAgentRollout(t, "group")
	backend := &servingAgentRolloutBackend{agentRolloutBackend: b, capable: true, approvalErr: errors.New("runtime evidence unavailable")}
	rc.Backend = backend
	op := mysqlAgentRolloutOperation(false)
	for {
		agentRolloutTick(t, rc, b, op, false)
		var cp agentUpgradeCheckpoint
		json.Unmarshal(op.Checkpoint, &cp)
		if cp.Stage == "handoff" {
			break
		}
	}
	if _, err := p.RunOperation(t.Context(), rc, op); err == nil || !strings.Contains(err.Error(), "runtime evidence unavailable") {
		t.Fatalf("runtime approval error was ignored: %v", err)
	}
	if len(b.names) != 0 {
		t.Fatal("unverified serving deployment was submitted")
	}
}

func TestAgentUpgradeProxyServingLostReceiptRetainsFailure(t *testing.T) {
	p, rc, b := setupAgentRollout(t, "group")
	backend := &servingAgentRolloutBackend{agentRolloutBackend: b, capable: true, marked: true}
	rc.Backend = backend
	op := mysqlAgentRolloutOperation(false)
	for {
		agentRolloutTick(t, rc, b, op, false)
		var cp agentUpgradeCheckpoint
		json.Unmarshal(op.Checkpoint, &cp)
		if cp.Stage == "deploy" {
			break
		}
	}
	intent := append(json.RawMessage(nil), op.Checkpoint...)
	agentRolloutTick(t, rc, b, op, false)
	op.Checkpoint = intent
	backend.fail = true
	// The build already changed, but native readiness cannot override a paused
	// provider receipt. Replay must adopt it before progressing to data.
	agentRolloutTick(t, rc, b, op, false)
	if _, err := p.RunOperation(t.Context(), rc, op); !errors.Is(err, engine.ErrInvalid) || !strings.Contains(err.Error(), "proxy rollout paused") || !strings.Contains(err.Error(), "manual recovery") {
		t.Fatalf("lost receipt bypassed permanent provider failure: %v", err)
	}
	if !reflect.DeepEqual(b.names, []string{"demomx"}) {
		t.Fatalf("failed proxy rollout touched data: %v", b.names)
	}
}

func TestAgentUpgradeCancelLostServingReceipt(t *testing.T) {
	for _, include := range []bool{false, true} {
		for _, outcome := range []string{"updating", "paused", "not-submitted", "legacy-receipt"} {
			t.Run(fmt.Sprintf("%s/include=%v", outcome, include), func(t *testing.T) {
				p, rc, b := setupAgentRollout(t, "group")
				if include {
					b.builds["demomx"] = engine.CurrentAgentBuild().Build
				}
				backend := &servingAgentRolloutBackend{agentRolloutBackend: b, capable: true, marked: true}
				rc.Backend = backend
				op := mysqlAgentRolloutOperation(include)
				for {
					agentRolloutTick(t, rc, b, op, false)
					var cp agentUpgradeCheckpoint
					json.Unmarshal(op.Checkpoint, &cp)
					if cp.Stage == "deploy" {
						break
					}
				}
				intent := append(json.RawMessage(nil), op.Checkpoint...)
				if outcome != "not-submitted" {
					agentRolloutTick(t, rc, b, op, false)
					// Retain a healthy incumbent/mixed fleet while Swarm replaces
					// tasks. Native health alone cannot prove terminal settlement.
					b.builds["demomx"] = "old"
				}
				op.Checkpoint = intent
				backend.waiting, backend.fail = outcome == "updating", outcome == "paused"
				backend.legacyReceipt = outcome == "legacy-receipt"
				result, err := p.CancelOperation(t.Context(), rc, op)
				if outcome == "paused" {
					if err == nil || !result.Done || !strings.Contains(err.Error(), "manual recovery required") || !strings.Contains(err.Error(), "proxy rollout paused") {
						t.Fatalf("paused mixed fleet cancelled successfully: %+v %v", result, err)
					}
				} else {
					if err != nil || result.Done {
						t.Fatalf("unexpected cancellation settlement: %+v %v", result, err)
					}
					op.Checkpoint = result.Checkpoint
					var cp agentUpgradeCheckpoint
					json.Unmarshal(op.Checkpoint, &cp)
					if outcome == "updating" {
						if cp.Stage != "waiting" || cp.Deploy == nil || cp.Member != "demomx" || !strings.Contains(result.Message, "Waiting for deployment") {
							t.Fatalf("in-flight serving rollout was abandoned: %+v %+v", cp, result)
						}
						backend.waiting = false
						result = agentRolloutTick(t, rc, b, op, true)
						json.Unmarshal(op.Checkpoint, &cp)
						if result.Done || cp.Member == "" || !strings.Contains(result.Message, "ONLINE with build") {
							t.Fatal("settlement skipped exact target-build acknowledgement")
						}
						b.builds["demomx"] = engine.CurrentAgentBuild().Build
					}
					finishAgentRollout(t, rc, b, op, true)
				}
				want := []string(nil)
				if outcome != "not-submitted" {
					want = []string{"demomx"}
				}
				if !reflect.DeepEqual(b.names, want) || backend.lookupCalls != 1 {
					t.Fatalf("cancellation submitted work or skipped lookup: %v, lookups=%d", b.names, backend.lookupCalls)
				}
			})
		}
	}
}

type ambiguousServingRolloutBackend struct {
	*servingAgentRolloutBackend
	state infra.OpState
	err   error
}

func (b *ambiguousServingRolloutBackend) OpStatus(context.Context, infra.Op) (infra.OpState, error) {
	return b.state, b.err
}

func TestAgentUpgradeServingAmbiguousFailureRemainsRetryable(t *testing.T) {
	for _, mode := range []string{"stalled", "expired", "transport"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, b := setupAgentRollout(t, "group")
			serving := true
			cp := agentUpgradeCheckpoint{
				Target: engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build},
				Order:  []string{"demomx"}, Member: "demomx", Stage: "waiting",
				ProxyServing: &serving, Deploy: &infra.Op{ID: "ambiguous"},
			}
			raw, _ := json.Marshal(cp)
			backend := &ambiguousServingRolloutBackend{servingAgentRolloutBackend: &servingAgentRolloutBackend{agentRolloutBackend: b}, state: infra.OpState{Failed: true, Status: "stalled"}}
			if mode == "expired" {
				backend.state.Status, backend.state.Expired = "expired", true
			}
			if mode == "transport" {
				backend.err = errors.New("transport unavailable")
			}
			rc.Backend = backend
			_, err := p.RunOperation(t.Context(), rc, &domain.Operation{Kind: domain.OperationUpgradeAgents, Checkpoint: raw})
			if err == nil || errors.Is(err, engine.ErrInvalid) {
				t.Fatalf("ambiguous serving deployment became permanent: %v", err)
			}
		})
	}
}
