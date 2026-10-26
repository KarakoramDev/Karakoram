// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

// Deployment clocks advance independently of polling. Platform startup
// completion deliberately does not imply broker membership.
type createClockBackend struct {
	*fake.Admission
	tick      int
	due       map[string]int
	lastDone  int
	backend   string
	durations map[string]int
	started   map[string]int
}

func (b *createClockBackend) Name() string { return b.backend }

func (b *createClockBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	op, err := b.Admission.Deploy(ctx, name, d)
	if err == nil {
		duration := 40
		if b.durations[name] != 0 {
			duration = b.durations[name]
		}
		b.due[name] = b.tick + duration
		if b.started == nil {
			b.started = make(map[string]int)
		}
		b.started[name] = b.tick
	}
	return op, err
}

func (b *createClockBackend) advance() {
	b.tick++
	// The shared model releases platform slots after executing startup readiness.
	// Keep agents offline until the deployment's independent clock expires.
	for name, due := range b.due {
		b.Members[name].AgentRunning = b.tick >= due
		if b.tick == due {
			b.lastDone = b.tick
		}
	}
	b.Admission.Advance()
}

func TestInitialCreateDeploysOverlapBeforeFormation(t *testing.T) {
	for _, backend := range []string{"zerops", "nomad", "dokploy"} {
		for _, count := range []int{3, 5} {
			t.Run(fmt.Sprintf("%s/%d", backend, count), func(t *testing.T) {
				p, rc, base, agents := setup(t)
				rc.Cluster.Spec.Components.Engine.Replicas = count
				peers := make([]string, count)
				joined := make(map[string]string)
				agents.joinHandler = func(name, seed string) {
					if seed != "rabbit@demom0" || !agents.broker("demom0").joined {
						t.Errorf("%s joined an uninitialized or divergent seed %s", name, seed)
					}
					joined[name] = seed
				}
				for i := range peers {
					peers[i] = fmt.Sprintf("demom%d", i)
				}
				b := &createClockBackend{Admission: &fake.Admission{Backend: base, Limit: 2, Peers: peers}, due: make(map[string]int), backend: backend}
				rc.Backend = b
				for range 500 {
					b.advance()
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					s := state(t, rc)
					if s.Phase != "ready" {
						continue
					}
					if b.Peak != 2 || len(b.Submitted) != count || !b.Ready() {
						t.Fatalf("deployments=%d peak=%d ready=%v", len(b.Submitted), b.Peak, b.Ready())
					}
					if len(joined) != count-1 {
						t.Fatalf("expected one bootstrap and %d joins, got %v", count-1, joined)
					}
					// Allow ten reconciliation ticks per batch for import checkpoints;
					// serial submission requires at least count*40 simulated seconds.
					if b.lastDone > ((count+1)/2)*50 || b.tick >= count*40 {
						t.Fatalf("serial critical path: deployments done=%ds Ready=%ds", b.lastDone, b.tick)
					}
					var cookie string
					for _, name := range peers {
						st, err := p.brokerStatus(t.Context(), rc, name)
						if err != nil || len(st.RunningNodes) != count {
							t.Fatalf("%s has separate membership: %v %v", name, st.RunningNodes, err)
						}
						cfg, err := p.MemberConfig(t.Context(), rc, name)
						if err != nil {
							t.Fatal(err)
						}
						var spec rmqspec.MemberSpec
						if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
							t.Fatal(err)
						}
						if cookie == "" {
							cookie = spec.Cookie
						}
						if spec.Cookie == "" || spec.Cookie != cookie {
							t.Fatal("members do not share one cluster cookie")
						}
						if !slices.Contains(spec.Peers, "rabbit@demom0") || !agents.broker(name).joined {
							t.Fatalf("%s did not converge on seed", name)
						}
					}
					t.Logf("peak=%d deployment completion=%ds fresh Ready=%ds", b.Peak, b.lastDone, b.tick)
					return
				}
				t.Fatal("creation never reached Ready")
			})
		}
	}
}

// Model staggered deployment durations without advertising seed capability before
// its platform startup completes.
func TestInitialCreateDeploysBeforeSeedCapability(t *testing.T) {
	for _, legacySeed := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy-seed=%v", legacySeed), func(t *testing.T) {
			p, rc, base, agents := setup(t)
			agents.legacyFormation = map[string]bool{"demom0": legacySeed}
			b := &createClockBackend{
				Admission: &fake.Admission{Backend: base, Limit: 2, Peers: []string{"demom0", "demom1", "demom2"}},
				due:       make(map[string]int), backend: "zerops",
				durations: map[string]int{"demom0": 108, "demom1": 237, "demom2": 102},
			}
			rc.Backend = b
			overlappedUnknown := false
			for range 500 {
				b.advance()
				seedAvailable := b.due["demom0"] != 0 && b.tick >= b.due["demom0"]
				if seedAvailable {
					delete(agents.failures, "demom0:formation-capabilities")
				} else {
					agents.failures["demom0:formation-capabilities"] = 503
				}
				agents.broker("demom0").running = seedAvailable
				for name := range b.Submitted {
					if name == "demom0" {
						continue
					}
					cfg, err := p.MemberConfig(t.Context(), rc, name)
					if err != nil {
						t.Fatal(err)
					}
					process := slices.Contains(cfg.DesiredProcesses, "rabbitmq-server")
					agents.broker(name).running = process
					s := state(t, rc)
					if !s.CreationGateChecked {
						overlappedUnknown = true
						if process {
							t.Fatal("peer broker started before seed capability was known")
						}
					}
					if legacySeed && process {
						if !s.find("demom0").FormationVerified || name == "demom2" && !s.find("demom1").FormationVerified {
							t.Fatal("legacy seed released native peers outside verified serial turns")
						}
					}
				}
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				if b.tick == 20 && (b.Peak != 2 || len(b.Submitted) != 2 || !overlappedUnknown) {
					t.Fatalf("first wave waited for seed capability: peak=%d submitted=%d held-overlap=%v", b.Peak, len(b.Submitted), overlappedUnknown)
				}
				if state(t, rc).Phase != "ready" {
					continue
				}
				if !overlappedUnknown || b.started["demom1"] >= b.due["demom0"] || b.Peak != 2 {
					t.Fatalf("seed-first serialization persists: starts=%v due=%v peak=%d", b.started, b.due, b.Peak)
				}
				// Work-conserving refill may beat the conservative two-wave
				// budget max(108,237)+102 = 339 seconds.
				if b.lastDone-b.started["demom0"] > 349 {
					t.Fatalf("deployment completion=%ds exceeds cap-two waves plus checkpoint overhead", b.lastDone-b.started["demom0"])
				}
				for _, m := range state(t, rc).Members {
					if !m.FormationVerified {
						t.Fatalf("%s reached Ready without native verification", m.Name)
					}
				}
				t.Logf("peak=%d starts=%v deployment completion=%ds Ready=%ds", b.Peak, b.started, b.lastDone, b.tick)
				return
			}
			t.Fatal("creation never reached Ready")
		})
	}
}

func TestInitialCreateProvisionsBeforeSeedHealth(t *testing.T) {
	p, rc, base, agents := setup(t)
	b := &createClockBackend{Admission: &fake.Admission{Backend: base, Limit: 2, Peers: []string{"demom0", "demom1", "demom2"}}, due: make(map[string]int), backend: "zerops"}
	rc.Backend = b
	for range 100 {
		b.advance()
		// Deployment starts the fixture's process; hold native health back
		// independently on every observation, not just before submission.
		agents.broker("demom0").running = false
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		if state(t, rc).Phase == "ready" || state(t, rc).Bootstrapped {
			t.Fatal("unavailable seed allowed premature bootstrap/Ready")
		}
	}
	if len(b.Submitted) != 3 || b.Peak != 2 || !b.Ready() {
		t.Fatalf("native seed health blocked platform provisioning: submitted=%d peak=%d", len(b.Submitted), b.Peak)
	}
	agents.broker("demom0").running = true
	for range 30 {
		b.advance()
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		if state(t, rc).Phase == "ready" {
			return
		}
	}
	t.Fatal("formation did not recover after seed became healthy")
}

func TestInitialCreateNeverInitializesNonseedStandalone(t *testing.T) {
	p, rc, base, agents := setup(t)
	b := &createClockBackend{Admission: &fake.Admission{Backend: base, Limit: 2, Peers: []string{"demom0", "demom1", "demom2"}}, due: make(map[string]int), backend: "zerops"}
	rc.Backend = b
	identities := make(map[string]string)
	attempts := make(map[string]int)
	for range 250 {
		b.advance()
		// A blank classic-config node falls back to standalone after thirty
		// failed discoveries. Keep the intended seed unavailable much longer.
		for _, name := range []string{"demom0", "demom1", "demom2"} {
			if _, submitted := b.Submitted[name]; !submitted {
				continue
			}
			cfg, err := p.MemberConfig(t.Context(), rc, name)
			if err != nil {
				t.Fatal(err)
			}
			var spec struct {
				FormationSeed string `json:"formationSeed"`
			}
			if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
				t.Fatal(err)
			}
			process := slices.Contains(cfg.DesiredProcesses, "rabbitmq-server")
			agents.broker(name).running = process && (name != "demom0" || b.tick >= 130)
			if !process || identities[name] != "" {
				continue
			}
			if name == "demom0" {
				if b.tick >= 130 {
					identities[name] = name
				}
			} else if spec.FormationSeed != "" {
				if spec.FormationSeed != "rabbit@demom0" {
					t.Fatalf("nondeterministic seed %q", spec.FormationSeed)
				}
				// Native fixed-seed discovery has no standalone fallback.
				if identities["demom0"] != "" {
					identities[name] = identities["demom0"]
				}
			} else {
				attempts[name]++
				if identities["demom0"] != "" {
					identities[name] = identities["demom0"]
				} else if attempts[name] > 30 {
					identities[name] = name
				}
			}
			if identities[name] != "" && identities[name] != "demom0" {
				t.Fatalf("%s initialized independent cluster %s before the seed", name, identities[name])
			}
		}
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		if state(t, rc).Phase == "ready" {
			if len(identities) != 3 || b.Peak != 2 {
				t.Fatalf("identities=%v peak=%d", identities, b.Peak)
			}
			return
		}
	}
	t.Fatal("fixed-seed creation did not converge")
}

func TestInitialCreateLegacyAgentsKeepSerialFormation(t *testing.T) {
	for _, oldSeed := range []bool{true, false} {
		t.Run(fmt.Sprintf("legacy-seed=%v", oldSeed), func(t *testing.T) {
			p, rc, base, agents := setup(t)
			agents.legacyFormation = map[string]bool{"demom1": true, "demom2": true}
			agents.legacyFormation["demom0"] = oldSeed
			b := &createClockBackend{Admission: &fake.Admission{Backend: base, Limit: 2, Peers: []string{"demom0", "demom1", "demom2"}}, due: make(map[string]int), backend: "zerops"}
			rc.Backend = b
			for range 300 {
				b.advance()
				held := 0
				for name := range b.Submitted {
					cfg, err := p.MemberConfig(t.Context(), rc, name)
					if err != nil {
						t.Fatal(err)
					}
					if !slices.Contains(cfg.DesiredProcesses, "rabbitmq-server") {
						agents.broker(name).running = false
						held++
					} else {
						agents.broker(name).running = true
						if name != "demom0" && !agents.broker("demom0").joined {
							t.Fatal("legacy peer released before seed bootstrap")
						}
					}
				}
				if held == 0 {
					s := state(t, rc)
					// Mixed-version peers are released only in ordered turns;
					// m2 cannot boot until m1 has verified seed membership.
					if m := s.find("demom2"); m != nil && m.FormationLegacyRelease && s.find("demom1").Stage != "ready" {
						t.Fatal("legacy peers released concurrently")
					}
				}
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				if state(t, rc).Phase == "ready" {
					if b.Peak != 2 {
						t.Fatalf("platform peak=%d want=2 (native turns remain serial)", b.Peak)
					}
					return
				}
			}
			t.Fatal("legacy formation did not converge")
		})
	}
}

func TestInitialCreateUnknownCapabilityKeepsPeerHeld(t *testing.T) {
	p, rc, base, agents := setup(t)
	agents.failures["demom1:formation-capabilities"] = 503
	b := &createClockBackend{Admission: &fake.Admission{Backend: base, Limit: 2, Peers: []string{"demom0", "demom1", "demom2"}}, due: make(map[string]int), backend: "zerops"}
	rc.Backend = b
	for range 110 {
		b.advance()
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		s := state(t, rc)
		if m := s.find("demom1"); m != nil && m.InitialFormation {
			cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
			if err != nil {
				t.Fatal(err)
			}
			if len(cfg.DesiredProcesses) != 0 || m.FormationLegacyRelease || m.FormationGate || m.FormationChecked {
				t.Fatal("unknown capability released native startup")
			}
		}
		if s.Phase == "ready" {
			t.Fatal("unknown formation capability permitted Ready")
		}
	}
	delete(agents.failures, "demom1:formation-capabilities")
	for range 30 {
		b.advance()
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		if state(t, rc).Phase == "ready" {
			return
		}
	}
	t.Fatal("positive capability did not release formation")
}

func TestInitialFormationGateDoesNotChangeScaleOut(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	rc.Cluster.Spec.Components.Engine.Replicas = 5
	for range 60 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"demom3", "demom4"} {
			m := state(t, rc).find(name)
			if m == nil {
				continue
			}
			cfg, err := p.MemberConfig(t.Context(), rc, name)
			if err != nil {
				t.Fatal(err)
			}
			var spec rmqspec.MemberSpec
			if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
				t.Fatal(err)
			}
			if m.InitialFormation || spec.FormationSeed != "" || !slices.Contains(cfg.DesiredProcesses, "rabbitmq-server") {
				t.Fatal("initial formation gate escaped into running-cluster scale-out")
			}
		}
		if state(t, rc).Phase == "ready" {
			return
		}
	}
	t.Fatal("scale-out did not converge")
}

func TestInitialFormationSurvivesVerifiedVolumeProcessRestart(t *testing.T) {
	p, rc, base, agents := setup(t)
	agents.volumeStatus = func(name string) *agentapi.VolumeStatus {
		s := state(t, rc)
		m := s.find(name)
		if m == nil {
			return nil
		}
		return &agentapi.VolumeStatus{Verified: true, Identity: agentapi.VolumeIdentity{ClusterUID: s.UID, Member: name, Volume: m.Volume, VolumeID: m.VolumeID}}
	}
	b := &createClockBackend{Admission: &fake.Admission{Backend: base, Limit: 2, Peers: []string{"demom0", "demom1", "demom2"}}, due: make(map[string]int), backend: "zerops"}
	rc.Backend = b
	for range 105 {
		b.advance()
		agents.broker("demom0").running = false
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	m := state(t, rc).find("demom1")
	if !m.RuntimeRecovery.Initialized || m.FormationVerified || m.Stage != "deployed" || agents.broker(m.Name).joined {
		t.Fatalf("fixture did not reach running-but-unjoined verified storage: %+v", m.RuntimeRecovery)
	}
	// The generic storage/process checkpoint is now true. Restart both the
	// peer process and the CP before the seed is available: the next pull
	// must still select the same native no-standalone gate.
	agents.redeploy(m.Name)
	p = New(p.options)
	agents.p = p
	for range 35 {
		b.advance()
		agents.broker("demom0").running = false
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		var spec rmqspec.MemberSpec
		if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		if spec.FormationSeed != "rabbit@demom0" {
			t.Fatal("verified fresh volume/process removed native gate before membership; restarted peer can initialize standalone")
		}
		if state(t, rc).Phase == "ready" {
			t.Fatal("unjoined restarted peer permitted Ready")
		}
	}
	agents.broker("demom0").running = true
	for range 30 {
		b.advance()
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		if state(t, rc).Phase == "ready" {
			for _, member := range state(t, rc).Members {
				cfg, err := p.MemberConfig(t.Context(), rc, member.Name)
				if err != nil {
					t.Fatal(err)
				}
				var spec rmqspec.MemberSpec
				if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
					t.Fatal(err)
				}
				if !member.FormationVerified || spec.FormationSeed != "" || !agents.broker(member.Name).joined {
					t.Fatal("verified native formation did not finish normally")
				}
			}
			return
		}
	}
	t.Fatal("formation did not converge after seed returned")
}

// Unreachable capability endpoints cost their observation budget independently
// of platform progress. They must not delay an available provisioning action.
func TestInitialCreateCapabilityWaitDoesNotDelayProvisioning(t *testing.T) {
	p, rc, base, agents := setup(t)
	b := &createClockBackend{
		Admission: &fake.Admission{Backend: base, Limit: 2, Peers: []string{"demom0", "demom1", "demom2"}},
		due:       make(map[string]int), backend: "zerops",
		durations: map[string]int{"demom0": 140, "demom1": 169, "demom2": 164},
	}
	rc.Backend = b
	waitSeconds := 0
	agents.requestHook = func(action string) {
		if action == "formation-capabilities" {
			waitSeconds += 5
		}
	}
	agents.failures["demom0:formation-capabilities"] = 503
	for range 30 {
		b.advance()
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		if len(b.Submitted) == 2 {
			if waitSeconds != 0 {
				t.Fatalf("first platform wave paid %ds of unavailable capability waits before submission", waitSeconds)
			}
			t.Logf("first wave submitted at ticks %v without capability waits", b.started)
			return
		}
	}
	t.Fatal("first platform wave never submitted")
}

func TestInitialCreateVerifiesSeedWhilePeerDeploymentRuns(t *testing.T) {
	p, rc, base, agents := setup(t)
	b := &createClockBackend{
		Admission: &fake.Admission{Backend: base, Limit: 2, Peers: []string{"demom0", "demom1", "demom2"}},
		due:       make(map[string]int), backend: "zerops",
		durations: map[string]int{"demom0": 140, "demom1": 169, "demom2": 164},
	}
	rc.Backend = b
	verifiedDuringDeploy := false
	for range 400 {
		b.advance()
		agents.broker("demom0").running = b.due["demom0"] != 0 && b.tick >= b.due["demom0"]
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		s := state(t, rc)
		if seed := s.find("demom0"); seed != nil && seed.FormationVerified && b.tick < b.due["demom2"] {
			verifiedDuringDeploy = true
		}
		if s.Phase != "ready" {
			continue
		}
		if !verifiedDuringDeploy {
			t.Fatal("seed bootstrap waited for the last independent peer deployment")
		}
		for _, m := range s.Members {
			if !m.FormationVerified || m.ResumeDeploy != nil {
				t.Fatalf("%s reached Ready without terminal deployment and formation verification", m.Name)
			}
		}
		t.Logf("seed verified during peer deploy; last platform completion=%ds Ready=%ds", b.lastDone, b.tick)
		return
	}
	t.Fatal("creation never reached Ready")
}

func TestInitialCreateExhaustedPredecessorKeepsLegacyPeerHeld(t *testing.T) {
	p, rc, _, agents := setup(t)
	agents.legacyFormation = map[string]bool{"demom1": true, "demom2": true}
	backend := &deploymentGuardBackend{
		Backend: rc.Backend, target: "demom1",
		siblings: map[string]bool{}, observed: map[string]bool{},
	}
	rc.Backend = backend
	for range 60 {
		if _, err := p.Sync(t.Context(), rc); err != nil && !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
			t.Fatal(err)
		}
	}
	s := state(t, rc)
	seed, blocked, peer := s.find("demom0"), s.find("demom1"), s.find("demom2")
	if seed == nil || !seed.FormationVerified || blocked == nil || blocked.Stage != "imported" || blocked.FormationVerified {
		t.Fatalf("did not reach verified seed with exhausted imported predecessor: %+v", s)
	}
	if len(backend.epochs) != 1 || len(rc.Cluster.Status.DeploymentFailures) != 1 {
		t.Fatalf("exhaustion was not durably fenced: epochs=%v failures=%v", backend.epochs, rc.Cluster.Status.DeploymentFailures)
	}
	if peer == nil || peer.Stage != "deployed" || peer.ResumeDeploy != nil {
		t.Fatalf("later peer's independent platform deployment did not complete: %+v", peer)
	}
	cfg, err := p.MemberConfig(t.Context(), rc, peer.Name)
	if err != nil {
		t.Fatal(err)
	}
	if peer.FormationLegacyRelease || peer.FormationVerified || len(cfg.DesiredProcesses) != 0 || s.Phase == "ready" {
		t.Fatalf("legacy peer released past exhausted predecessor: release=%v verified=%v processes=%v phase=%s", peer.FormationLegacyRelease, peer.FormationVerified, cfg.DesiredProcesses, s.Phase)
	}
}
