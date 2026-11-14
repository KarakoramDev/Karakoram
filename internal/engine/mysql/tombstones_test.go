// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/nomad"
)

type tombstoneProjector struct {
	infra.Backend
	sets map[string][]string
	fail bool
}

func (p *tombstoneProjector) ProjectResolutionTombstones(_ context.Context, member string, hosts []string) error {
	if p.fail {
		return errors.New("projection unavailable")
	}
	p.sets[member] = slices.Clone(hosts)
	return nil
}

func TestResolutionTombstoneGate(t *testing.T) {
	for _, mode := range []string{"supported-unverified", "old", "unsupported", "before", "equal", "verified", "mixed"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, b, s, lost := recoveryFixture(t)
			lost.ConfirmedVolumeLost = true
			created := time.Unix(2000, 0).UTC()
			rc.Clock = agentRolloutClock{at: created}
			projector := &tombstoneProjector{Backend: rc.Backend, sets: map[string][]string{}}
			rc.Backend = projector
			ok, err := gateResolutionTombstone(t.Context(), rc, s, lost)
			if ok || err != nil || len(s.ResolutionTombstones) != 1 || len(projector.sets) != 0 {
				t.Fatal(ok, err, s.ResolutionTombstones, projector.sets)
			}
			journal := s.ResolutionTombstones[0]
			if journal.RuntimeID != lost.ServiceID || journal.NativeUUID != lost.ServerUUID || journal.Epoch == "" {
				t.Fatal(journal)
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			s, _ = load(rc.Cluster)
			lost = s.find(lost.Name)
			var reloads int
			b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				switch r.URL.Path {
				case "/v1/reload":
					reloads++
					json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
					return true
				case "/v1/status":
					var proof *agentapi.ResolutionTombstoneStatus
					if mode != "old" {
						proof = &agentapi.ResolutionTombstoneStatus{Supported: mode != "unsupported", Hosts: []string{lost.Name}, SetHash: agentapi.ResolutionSetHash([]string{lost.Name}), Generation: "agent/launch-1", ProcessGeneration: agentapi.ResolutionProcessGeneration(agentapi.ProcessStatus{})}
					}
					if mode == "before" {
						proof.VerifiedAt = created.Add(-time.Second)
					}
					if mode == "equal" {
						proof.VerifiedAt = created
					}
					if mode == "verified" {
						proof.VerifiedAt = created.Add(time.Second)
					}
					if mode == "mixed" && name == s.data()[0].Name {
						proof.Supported = false
					}
					json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: b.applied[name], ResolutionTombstones: proof})
					return true
				}
				return false
			}
			ok, err = gateResolutionTombstone(freshMySQLReads(t.Context()), rc, s, lost)
			if err != nil {
				t.Fatal(err)
			}
			if ok != (mode == "verified") {
				t.Fatal("gate result", ok, mode)
			}
			if mode == "old" || mode == "unsupported" || mode == "mixed" {
				if s.RetirementUnprotected == "" || !s.ResolutionTombstones[0].Unprotected || reloads != 0 {
					t.Fatal(s.RetirementUnprotected, reloads)
				}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				s, _ = load(rc.Cluster)
				lost = s.find(lost.Name)
				ok, err = gateResolutionTombstone(freshMySQLReads(t.Context()), rc, s, lost)
				if !ok || err != nil {
					t.Fatal(ok, err)
				}
				var status domain.ClusterStatus
				applyResolutionTombstoneStatus(rc, s, &status)
				if len(status.Conditions) != 2 || status.Conditions[1].Reason != "RetirementUnprotected" || !strings.Contains(status.Conditions[1].Message, "~5 min") {
					t.Fatal(status)
				}
			}
			if mode == "supported-unverified" || mode == "before" || mode == "equal" {
				if reloads == 0 || !strings.Contains(rc.SyncWait, "verification") {
					t.Fatal(reloads, rc.SyncWait)
				}
			}
			for _, m := range s.data() {
				if slices.Contains(projector.sets[m.Name], lost.Name) != (m.Name != lost.Name) {
					t.Fatal(projector.sets)
				}
			}
			// The complete desired list is also in member configuration, independent of joining stage.
			joiner := *s.data()[0]
			joiner.Name = "joiner"
			joiner.Stage = "planned"
			s.Members = append(s.Members, &joiner)
			if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			for _, m := range s.data() {
				cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				if slices.Contains(cfg.ResolutionTombstones, lost.Name) != (m.Name != lost.Name) {
					t.Fatal(m.Name, cfg.ResolutionTombstones)
				}
			}
		})
	}
}

func TestResolutionTombstoneHoldAndRelease(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	at := time.Unix(3000, 0).UTC()
	rc.Clock = agentRolloutClock{at: at}
	retired := &member{Name: "retired", Role: "data"}
	s.ResolutionTombstones = []resolutionTombstone{{Host: retired.Name, CreatedAt: at.Add(-time.Minute), LastVerification: at.Add(time.Second)}}
	holdResolutionTombstone(rc, s, retired)
	deadline := s.ResolutionTombstones[0].ReleaseNotBefore
	if !deadline.Equal(at.Add(601 * time.Second)) {
		t.Fatal(deadline)
	}
	rc.Clock = agentRolloutClock{at: at.Add(time.Hour)}
	holdResolutionTombstone(rc, s, retired)
	if !s.ResolutionTombstones[0].ReleaseNotBefore.Equal(deadline) {
		t.Fatal("replay changed deadline")
	}
	for _, m := range s.data() {
		recordResolutionGeneration(&s.ResolutionTombstones[0], m.Name, &agentapi.ResolutionTombstoneStatus{Generation: "stable", VerifiedAt: at})
	}
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path != "/v1/status" {
			return false
		}
		proof := resolutionTestProof(resolutionTombstoneHosts(s, s.find(name)), now(rc), "stable")
		json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: s.find(name).ConfigVersion, ResolutionTombstones: proof})
		return true
	}
	projector := &tombstoneProjector{Backend: rc.Backend, sets: map[string][]string{}}
	rc.Backend = projector
	rc.Clock = agentRolloutClock{at: deadline.Add(-time.Second)}
	if err := reconcileResolutionTombstones(t.Context(), rc, s); err != nil || len(s.ResolutionTombstones) != 1 {
		t.Fatal(err)
	}
	rc.Clock = agentRolloutClock{at: deadline}
	s.data()[0].Offline = true
	if err := reconcileResolutionTombstones(t.Context(), rc, s); err != nil || len(s.ResolutionTombstones) != 1 {
		t.Fatal(err)
	}
	s.data()[0].Offline = false
	before := s.ConfigVersion
	if err := reconcileResolutionTombstones(freshMySQLReads(t.Context()), rc, s); err != nil {
		t.Fatal(err)
	}
	if len(s.ResolutionTombstones) != 1 || !s.ResolutionTombstones[0].ReleasePending {
		t.Fatal("release did not persist pending intent", s.ResolutionTombstones)
	}
	if err := reconcileResolutionTombstones(freshMySQLReads(t.Context()), rc, s); err != nil {
		t.Fatal(err)
	}
	if len(s.ResolutionTombstones) != 0 || s.ConfigVersion <= before {
		t.Fatal("expired healthy release missing", s.ResolutionTombstones)
	}
	for _, hosts := range projector.sets {
		if len(hosts) != 0 {
			t.Fatal(projector.sets)
		}
	}
}

func TestResolutionTombstoneHealthyRetirementUnchanged(t *testing.T) {
	for _, tc := range []struct {
		topology string
		lost     bool
	}{{"group", false}, {"cluster", true}, {"single", true}} {
		s := &state{Topology: tc.topology}
		m := &member{Name: "retired", Role: "data", ConfirmedVolumeLost: tc.lost}
		ok, err := gateResolutionTombstone(t.Context(), &engine.Context{}, s, m)
		if !ok || err != nil || len(s.ResolutionTombstones) != 0 {
			t.Fatal(tc, ok, err)
		}
	}
}

func TestResolutionTombstoneProjectionFailureRetainsJournal(t *testing.T) {
	at := time.Unix(4000, 0).UTC()
	retiree := &member{Name: "retired", Role: "data", ServiceID: "runtime", ConfirmedVolumeLost: true}
	survivor := &member{Name: "survivor", Role: "data", ServiceID: "survivor-runtime"}
	s := &state{Topology: "group", ConfigVersion: 7, Members: []*member{retiree, survivor}, ResolutionTombstones: []resolutionTombstone{{Host: retiree.Name, RuntimeID: retiree.ServiceID, CreatedAt: at, ReleaseNotBefore: at}}}
	backend := &tombstoneProjector{fail: true, sets: map[string][]string{}}
	rc := &engine.Context{Cluster: &domain.Cluster{}, Backend: backend, Clock: agentRolloutClock{at: at}}
	ok, err := gateResolutionTombstone(t.Context(), rc, s, retiree)
	if ok || err == nil {
		t.Fatal("projection failure authorized stop", ok, err)
	}
	if err := reconcileResolutionTombstones(t.Context(), rc, s); err == nil {
		t.Fatal("release ignored projection failure")
	}
	if len(s.ResolutionTombstones) != 1 || s.ConfigVersion != 7 {
		t.Fatal("failed projection discarded intent", s.ResolutionTombstones, s.ConfigVersion)
	}
}

func resolutionTestProof(hosts []string, at time.Time, generation string) *agentapi.ResolutionTombstoneStatus {
	return &agentapi.ResolutionTombstoneStatus{Supported: true, Hosts: slices.Clone(hosts), SetHash: agentapi.ResolutionSetHash(hosts), VerifiedAt: at, Generation: generation, ProcessGeneration: agentapi.ResolutionProcessGeneration(agentapi.ProcessStatus{})}
}

func TestResolutionTombstoneProofRejectsStaleSetAndProcess(t *testing.T) {
	at := time.Now()
	status := agentapi.Status{ResolutionTombstones: resolutionTestProof([]string{"retired"}, at, "agent/1")}
	if !currentResolutionProof(status, []string{"retired"}, at) {
		t.Fatal("valid proof rejected")
	}
	if currentResolutionProof(status, []string{"retired", "second"}, at) {
		t.Fatal("partial set accepted")
	}
	if currentResolutionProof(status, []string{"retired"}, at.Add(agentapi.ResolutionProofLease+time.Nanosecond)) {
		t.Fatal("stale proof accepted")
	}
	status.Processes = map[string]agentapi.ProcessStatus{"mysqld": {PID: 42, Launch: 2}}
	if currentResolutionProof(status, []string{"retired"}, at) {
		t.Fatal("old process proof accepted")
	}
}

func TestResolutionTombstoneGenerationExtendsOnly(t *testing.T) {
	at := time.Unix(1000, 0)
	entry := resolutionTombstone{ReleaseNotBefore: at.Add(600 * time.Second)}
	recordResolutionGeneration(&entry, "survivor", resolutionTestProof(nil, at.Add(599*time.Second), "agent/1"))
	want := at.Add(1199 * time.Second)
	if !entry.ReleaseNotBefore.Equal(want) {
		t.Fatal(entry)
	}
	recordResolutionGeneration(&entry, "survivor", resolutionTestProof(nil, at.Add(700*time.Second), "agent/1"))
	if !entry.ReleaseNotBefore.Equal(want) {
		t.Fatal("same launch rearmed", entry)
	}
	recordResolutionGeneration(&entry, "survivor", resolutionTestProof(nil, at.Add(800*time.Second), "agent/2"))
	want = at.Add(1400 * time.Second)
	if !entry.ReleaseNotBefore.Equal(want) {
		t.Fatal("restart did not rearm", entry)
	}
	recordResolutionGeneration(&entry, "joiner", resolutionTestProof(nil, at.Add(900*time.Second), "joiner/1"))
	if !entry.ReleaseNotBefore.Equal(at.Add(1500 * time.Second)) {
		t.Fatal("new survivor did not rearm", entry)
	}
	recordResolutionGeneration(&entry, "survivor", resolutionTestProof(nil, at, "agent/3"))
	if !entry.ReleaseNotBefore.Equal(at.Add(1500 * time.Second)) {
		t.Fatal("deadline shortened", entry)
	}
}

func TestResolutionTombstoneReleaseNonProjectorPartialAcknowledgement(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	at := time.Now().UTC()
	rc.Clock = agentRolloutClock{at: at}
	entry := resolutionTombstone{Host: "retired", ReleaseNotBefore: at, Generations: map[string]string{}}
	for _, m := range s.data() {
		entry.Generations[m.Name] = "stable"
	}
	s.ResolutionTombstones = []resolutionTombstone{entry}
	failing := ""
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path != "/v1/status" {
			return false
		}
		hosts := resolutionTombstoneHosts(s, s.find(name))
		proof := resolutionTestProof(hosts, at, "stable")
		if name == failing {
			proof.Reason = "helper write failed"
			proof.SetHash = ""
			proof.VerifiedAt = time.Time{}
		}
		json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: s.find(name).ConfigVersion, ResolutionTombstones: proof})
		return true
	}
	if err := reconcileResolutionTombstones(freshMySQLReads(t.Context()), rc, s); err != nil {
		t.Fatal(err)
	}
	if len(s.ResolutionTombstones) != 1 || !s.ResolutionTombstones[0].ReleasePending {
		t.Fatal("missing release journal", s)
	}
	// Round-trip the durable pending checkpoint before reduced-set publication.
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	failing = s.data()[1].Name
	if err := reconcileResolutionTombstones(freshMySQLReads(t.Context()), rc, s); err != nil {
		t.Fatal(err)
	}
	if len(s.ResolutionTombstones) != 1 || len(resolutionTombstoneHosts(s, s.data()[0])) != 0 {
		t.Fatal("partial failure forgot journal", s)
	}
	failing = ""
	if err := reconcileResolutionTombstones(freshMySQLReads(t.Context()), rc, s); err != nil {
		t.Fatal(err)
	}
	if len(s.ResolutionTombstones) != 0 {
		t.Fatal("complete release not settled", s)
	}
}

type realNomadResolutionBackend struct {
	*fakeBackend
	projector  infra.ResolutionTombstoneProjector
	variableMu sync.Mutex
	variables  map[string]map[string]string
	t          *testing.T
}

func (b *realNomadResolutionBackend) ProjectResolutionTombstones(ctx context.Context, name string, hosts []string) error {
	return b.projector.ProjectResolutionTombstones(ctx, name, hosts)
}

func (b *realNomadResolutionBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	b.variableMu.Lock()
	if b.variables[name]["karakoram_tombstone_retired"] != "retired" {
		b.t.Errorf("first start lacks retained set: %s %v", name, b.variables[name])
	}
	b.variableMu.Unlock()
	return b.fakeBackend.Deploy(ctx, name, d)
}

func TestResolutionTombstoneRealNomadPlannedProvisioning(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(map[bool]string{false: "scale-out", true: "second-replacement"}[replacement], func(t *testing.T) {
			p, rc, b := setup(t, "group")
			converge(t, p, rc, b)
			s, _ := load(rc.Cluster)
			s.ResolutionTombstones = []resolutionTombstone{{Host: "retired", CreatedAt: time.Now(), ReleaseNotBefore: time.Now().Add(time.Hour)}}
			bridge := realNomadResolutionFixture(t, b)
			variables := bridge.variables
			rc.Backend = bridge
			name := fmt.Sprintf("%sm%d", rc.Cluster.Name, s.NextIndex)
			names := []string{name}
			if replacement {
				// Exact durable allocation shape made by replaceMember, before import.
				old := s.data()[1]
				s.Members = append(s.Members, &member{Name: name, Index: s.NextIndex, Role: "data", Volume: name + "v", Stage: "planned", ConfigVersion: s.ConfigVersion})
				s.NextIndex++
				s.Replacement = &replacementState{Operation: "second", Old: old.Name, New: name, Stage: "join"}
			} else {
				rc.Cluster.Spec.Components.Engine.Replicas = 5
				names = append(names, fmt.Sprintf("%sm%d", rc.Cluster.Name, s.NextIndex+1))
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			for range 50 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				b.apply(t, p, rc)
				if !slices.ContainsFunc(names, func(name string) bool { return !b.deployed[name] }) {
					break
				}
			}
			for _, name := range names {
				bridge.variableMu.Lock()
				protected := variables[name]["karakoram_tombstone_retired"] == "retired"
				bridge.variableMu.Unlock()
				if !b.deployed[name] || !protected {
					t.Fatal("planned runtime did not import/deploy with protection", name, variables)
				}
			}
		})
	}
}

func (b *realNomadResolutionBackend) ImportServices(ctx context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	b.fakeBackend.mu.Lock()
	defer b.fakeBackend.mu.Unlock()
	return b.fakeBackend.ImportServices(ctx, specs)
}

func realNomadResolutionFixture(t *testing.T, b *fakeBackend) *realNomadResolutionBackend {
	t.Helper()
	bridge := &realNomadResolutionBackend{fakeBackend: b, variables: map[string]map[string]string{}, t: t}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := strings.TrimPrefix(r.URL.Path, "/v1/var/")
		if strings.HasPrefix(path, "karakoram/services/") {
			b.mu.Lock()
			defer b.mu.Unlock()
			name := strings.TrimPrefix(path, "karakoram/services/")
			service := b.services[name]
			if service == nil {
				http.NotFound(w, r)
				return
			}
			record, _ := json.Marshal(map[string]any{"ID": service.ID, "Hostname": name, "Kind": service.Kind})
			json.NewEncoder(w).Encode(map[string]any{"Path": path, "ModifyIndex": 1, "Items": map[string]string{"record": string(record)}})
			return
		}
		if strings.HasPrefix(path, "nomad/jobs/") {
			bridge.variableMu.Lock()
			defer bridge.variableMu.Unlock()
			name := strings.TrimPrefix(path, "nomad/jobs/")
			if r.Method == http.MethodPut {
				var v struct{ Items map[string]string }
				if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
					t.Error(err)
				}
				bridge.variables[name] = v.Items
			}
			json.NewEncoder(w).Encode(map[string]any{"Path": path, "ModifyIndex": 1, "Items": bridge.variables[name]})
			return
		}
		t.Errorf("unexpected Nomad endpoint %s", r.URL.Path)
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)
	backend, err := nomad.NewFactory(nomad.WithAllowedAddrs(server.URL)).New(infra.Target{Kind: "nomad", ID: "example", Token: "fixture", Config: map[string]string{"address": server.URL}})
	if err != nil {
		t.Fatal(err)
	}
	bridge.projector = backend.(infra.ResolutionTombstoneProjector)
	return bridge
}

func TestResolutionTombstoneRealNomadDeletionCheckpointReplay(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	target := s.data()[2]
	target.Removing, target.Offline, target.Retirement = true, true, "delete"
	s.ResolutionTombstones = []resolutionTombstone{{Host: "retired", CreatedAt: time.Now(), ReleaseNotBefore: time.Now().Add(time.Hour)}}
	s.Pending = &pending{Action: "delete-runtime", Member: target.Name, Op: infra.Op{ID: "settling-delete"}}
	staleID := target.ServiceID
	delete(b.services, target.Name)
	delete(b.deployed, target.Name)
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	if s.find(target.Name).ServiceID != staleID || staleID == "" || s.Pending == nil {
		t.Fatal("deletion crash checkpoint was not reconstructed", s)
	}
	rc.Backend = realNomadResolutionFixture(t, b)
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal("projection prevented deletion receipt replay", err)
	}
	s, _ = load(rc.Cluster)
	if s.Pending != nil || s.find(target.Name).ServiceID != "" {
		t.Fatal("durable deletion receipt did not settle", s.Pending, s.find(target.Name))
	}
	// A terminal retired row is also ineligible even if an old checkpoint
	// carries a stale runtime ID. A live survivor with the same absence is not.
	target = s.find(target.Name)
	target.ServiceID, target.Retirement = staleID, "credentials"
	if err := projectResolutionTombstones(t.Context(), rc, s); err != nil {
		t.Fatal("completed retirement was projected", err)
	}
	target.Retirement, target.Removing, target.Offline = "", false, false
	if err := projectResolutionTombstones(t.Context(), rc, s); !errors.Is(err, infra.ErrNotFound) {
		t.Fatal("missing live survivor record was swallowed", err)
	}
}

func TestResolutionTombstoneReleaseExemptsOnlyUnsupportedAgents(t *testing.T) {
	for _, mode := range []string{"old", "unsupported", "supported-failed", "unreachable"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, b := setup(t, "group")
			converge(t, p, rc, b)
			s, _ := load(rc.Cluster)
			at := time.Now().UTC()
			rc.Clock = agentRolloutClock{at: at}
			entry := resolutionTombstone{Host: "retired", ReleaseNotBefore: at, Generations: map[string]string{}}
			for _, m := range s.data() {
				entry.Generations[m.Name] = "stable"
			}
			s.ResolutionTombstones = []resolutionTombstone{entry}
			special := s.data()[1].Name
			b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				if r.URL.Path != "/v1/status" {
					return false
				}
				proof := resolutionTestProof(resolutionTombstoneHosts(s, s.find(name)), at, "stable")
				if name == special {
					switch mode {
					case "old":
						proof = nil
					case "unsupported":
						proof.Supported, proof.Reason = false, "no resolver mechanism"
					case "supported-failed":
						proof.Reason, proof.SetHash, proof.VerifiedAt = "helper failed", "", time.Time{}
					case "unreachable":
						http.Error(w, "unreachable", http.StatusServiceUnavailable)
						return true
					}
				}
				json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: s.find(name).ConfigVersion, ResolutionTombstones: proof})
				return true
			}
			for range 2 {
				if err := reconcileResolutionTombstones(freshMySQLReads(t.Context()), rc, s); err != nil && mode != "unreachable" {
					t.Fatal(err)
				}
			}
			blocked := mode == "supported-failed" || mode == "unreachable"
			if (len(s.ResolutionTombstones) != 0) != blocked {
				t.Fatal("unsupported release policy mismatch", mode, s.ResolutionTombstones)
			}
		})
	}
}

func TestResolutionTombstonePendingReleaseRearmsOnCohortChange(t *testing.T) {
	for _, change := range []string{"restart", "new-joiner"} {
		t.Run(change, func(t *testing.T) {
			p, rc, b := setup(t, "group")
			converge(t, p, rc, b)
			s, _ := load(rc.Cluster)
			at := time.Now().UTC()
			rc.Clock = agentRolloutClock{at: at}
			entry := resolutionTombstone{Host: "retired", ReleaseNotBefore: at, Generations: map[string]string{}}
			generations := map[string]string{}
			for _, m := range s.data() {
				entry.Generations[m.Name], generations[m.Name] = "stable", "stable"
			}
			s.ResolutionTombstones = []resolutionTombstone{entry}
			b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				if r.URL.Path != "/v1/status" {
					return false
				}
				proof := resolutionTestProof(resolutionTombstoneHosts(s, s.find(name)), now(rc), generations[name])
				json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: s.find(name).ConfigVersion, ResolutionTombstones: proof})
				return true
			}
			if err := reconcileResolutionTombstones(freshMySQLReads(t.Context()), rc, s); err != nil {
				t.Fatal(err)
			}
			if !s.ResolutionTombstones[0].ReleasePending {
				t.Fatal("release did not reach pending checkpoint")
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			s, _ = load(rc.Cluster)
			rc.Clock = agentRolloutClock{at: at.Add(5 * time.Second)}
			changed := s.data()[1]
			b.mu.Lock()
			if change == "restart" {
				generations[changed.Name] = "restarted"
			} else {
				changed = &member{Name: "new-joiner", Role: "data", Stage: "planned", ConfigVersion: s.ConfigVersion}
				s.Members = append(s.Members, changed)
				generations[changed.Name] = "new-launch"
			}
			b.mu.Unlock()
			before := s.ConfigVersion
			changedName := changed.Name
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s, _ = load(rc.Cluster)
			changed = s.find(changedName)
			if change == "new-joiner" && (changed.ImportBatch != nil || b.deployed[changedName]) {
				t.Fatal("new member provisioned before cancellation checkpoint", changed)
			}
			journal := &s.ResolutionTombstones[0]
			if journal.ReleasePending || s.ConfigVersion <= before || !slices.Equal(resolutionTombstoneHosts(s, changed), []string{"retired"}) ||
				journal.ReleaseNotBefore.Before(now(rc).Add(600*time.Second)) {
				t.Fatal("pending release did not restore a full generation hold", journal, s.ConfigVersion)
			}
			// Reconstruct the cancellation checkpoint, then qualify the full
			// set. Qualification, not just reduced-set proof, gets 600 seconds.
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			name := changed.Name
			s, _ = load(rc.Cluster)
			if change == "new-joiner" {
				changed = s.find(name)
				changed.Stage, changed.ServiceID = "joined", "new-runtime"
				b.mu.Lock()
				b.native[name] = b.native[s.data()[0].Name]
				b.mu.Unlock()
			}
			rc.Clock = agentRolloutClock{at: at.Add(6 * time.Second)}
			if err := reconcileResolutionTombstones(freshMySQLReads(t.Context()), rc, s); err != nil {
				t.Fatal(err)
			}
			if len(s.ResolutionTombstones) != 1 || s.ResolutionTombstones[0].ReleasePending ||
				s.ResolutionTombstones[0].ReleaseNotBefore.Before(now(rc).Add(600*time.Second)) {
				t.Fatal("restored full-set qualification did not extend hold", s.ResolutionTombstones)
			}
		})
	}
}

func TestResolutionTombstoneReducedInstallFailureRetainsNonemptyJournal(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	at := time.Now().UTC()
	rc.Clock = agentRolloutClock{at: at}
	for _, host := range []string{"retired-a", "retired-b"} {
		entry := resolutionTombstone{Host: host, ReleaseNotBefore: at, Generations: map[string]string{}}
		if host == "retired-b" {
			entry.ReleaseNotBefore = at.Add(time.Hour)
		}
		for _, m := range s.data() {
			entry.Generations[m.Name] = "stable"
		}
		s.ResolutionTombstones = append(s.ResolutionTombstones, entry)
	}
	failing := false
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path != "/v1/status" {
			return false
		}
		proof := resolutionTestProof(resolutionTombstoneHosts(s, s.find(name)), at, "stable")
		if failing && name == s.data()[1].Name {
			// The retained name still resolves, but the complete-set Install
			// failed. The agent regression proves refresh keeps this negative.
			proof.Hosts = []string{"retired-b"}
			proof.Reason, proof.SetHash, proof.VerifiedAt = "reduced-set installation failed", "", time.Time{}
		}
		json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: s.find(name).ConfigVersion, ResolutionTombstones: proof})
		return true
	}
	if err := reconcileResolutionTombstones(freshMySQLReads(t.Context()), rc, s); err != nil {
		t.Fatal(err)
	}
	if !s.ResolutionTombstones[0].ReleasePending || s.ResolutionTombstones[1].ReleasePending {
		t.Fatal("incorrect partial release checkpoint", s.ResolutionTombstones)
	}
	failing = true
	if err := reconcileResolutionTombstones(freshMySQLReads(t.Context()), rc, s); err != nil {
		t.Fatal(err)
	}
	if len(s.ResolutionTombstones) != 2 || !s.ResolutionTombstones[0].ReleasePending {
		t.Fatal("failed nonempty reduced-set installation discarded its journal", s.ResolutionTombstones)
	}
	failing = false
	if err := reconcileResolutionTombstones(freshMySQLReads(t.Context()), rc, s); err != nil {
		t.Fatal(err)
	}
	if len(s.ResolutionTombstones) != 1 || s.ResolutionTombstones[0].Host != "retired-b" {
		t.Fatal("successful reduced-set installation did not settle only the expired journal", s.ResolutionTombstones)
	}
}

func TestResolutionTombstonePendingReleaseAllowsLifecycleProgress(t *testing.T) {
	for _, checkpoint := range []string{"stopped-retirement", "absent-delete-receipt", "missing-runtime"} {
		t.Run(checkpoint, func(t *testing.T) {
			p, rc, b := setup(t, "group")
			converge(t, p, rc, b)
			s, _ := load(rc.Cluster)
			at := time.Now().UTC()
			rc.Clock = agentRolloutClock{at: at}
			entry := resolutionTombstone{Host: "retired", ReleasePending: true, ReleaseNotBefore: at, Generations: map[string]string{}}
			for _, m := range s.data() {
				entry.Generations[m.Name] = "stable"
			}
			s.ResolutionTombstones = []resolutionTombstone{entry}
			target := s.data()[2]
			name, volume, volumeID := target.Name, target.Volume, target.VolumeID
			if checkpoint == "stopped-retirement" {
				receipt, err := b.Stop(t.Context(), name)
				if err != nil {
					t.Fatal(err)
				}
				target.Removing, target.Offline, target.Retirement = true, true, "stop"
				target.RetirementOp, target.RetirementFenceID = &receipt, target.ServiceID
			} else {
				delete(b.services, name)
				if checkpoint == "absent-delete-receipt" {
					target.Removing, target.Offline, target.Retirement = true, true, "delete"
					s.Pending = &pending{Action: "delete-runtime", Member: name, Op: infra.Op{ID: "settling-delete"}}
				}
			}
			delete(b.native, name)
			delete(b.deployed, name)
			b.action = func(w http.ResponseWriter, r *http.Request, member string) bool {
				if member == name && (r.URL.Path == "/v1/status" || r.URL.Path == "/v1/mysql/status") {
					http.Error(w, "runtime agent is unavailable", http.StatusServiceUnavailable)
					return true
				}
				if r.URL.Path != "/v1/status" {
					return false
				}
				proof := resolutionTestProof(resolutionTombstoneHosts(s, s.find(member)), at, "stable")
				json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: b.applied[member], ResolutionTombstones: proof})
				return true
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			// Use the actual Nomad projector: a missing runtime would fail
			// projection if deferral did not precede that provider mutation.
			rc.Backend = realNomadResolutionFixture(t, b)
			for tick := range 3 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal("release observation blocked ordinary lifecycle reconciliation", err)
				}
				s, _ = load(rc.Cluster)
				target = s.find(name)
				if len(s.ResolutionTombstones) != 1 || !s.ResolutionTombstones[0].ReleasePending ||
					!s.ResolutionTombstones[0].ReleaseNotBefore.Equal(at) ||
					s.ResolutionTombstones[0].Generations[name] != "stable" {
					t.Fatal("unavailable member released/rearmed the journal or became unsupported", s.ResolutionTombstones)
				}
				if s.RetirementUnprotected != "" {
					t.Fatal("status failure was counted as unsupported", s.RetirementUnprotected)
				}
				if checkpoint != "missing-runtime" {
					break
				}
				switch tick {
				case 0:
					if target.RuntimeRecovery.ImportPlan == nil || target.RuntimeRecovery.Op != nil || b.services[name] != nil {
						t.Fatal("missing runtime recovery did not checkpoint its import", target.RuntimeRecovery, b.services[name])
					}
				case 1:
					if target.RuntimeRecovery.Op == nil || b.services[name] == nil {
						t.Fatal("missing runtime recovery did not submit its import", target.RuntimeRecovery, b.services[name])
					}
				case 2:
					if target.Stage != "imported" || target.ServiceID == "" || target.RuntimeRecovery.Op != nil || target.RuntimeRecovery.ImportPlan != nil {
						t.Fatal("missing runtime recovery did not adopt its receipt", target)
					}
				}
			}
			switch checkpoint {
			case "stopped-retirement":
				if target.Retirement != "delete" || !target.RetirementFenceComplete || len(b.stopped) != 1 {
					t.Fatal("stopped healthy retiree did not settle its original stop", target, b.stopped)
				}
			case "absent-delete-receipt":
				if s.Pending != nil || target.ServiceID != "" {
					t.Fatal("absent runtime deletion receipt did not settle", s.Pending, target)
				}
			}
			if kept := b.services[volume]; kept == nil || kept.ID != volumeID {
				t.Fatal("ordinary progress changed the retained volume", kept)
			}
		})
	}
}
