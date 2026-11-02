// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type proxyScaleBackend struct {
	*admissionBackend
	scales, resources, deploys int

	pending bool

	disruptiveScale bool

	ports []infra.Port
}

func (b *proxyScaleBackend) Capabilities() infra.Capabilities {
	c := b.Backend.Capabilities()
	c.ReplicaScalePreservesServing = !b.disruptiveScale
	return c
}

func (b *proxyScaleBackend) SetContainerCount(ctx context.Context, name string, min, max int) (infra.Op, error) {
	if name == b.proxy {
		b.scales++
		b.count = min
	}
	return b.Backend.SetContainerCount(ctx, name, min, max)
}

func (b *proxyScaleBackend) SetResources(ctx context.Context, name string, resources infra.Resources) (infra.Op, error) {
	if name == b.proxy {
		b.resources++
	}
	return b.Backend.SetResources(ctx, name, resources)
}

func (b *proxyScaleBackend) Deploy(ctx context.Context, name string, deployment infra.Deployment) (infra.Op, error) {
	if name == b.proxy {
		b.deploys++
		b.ports = deployment.Runtime.Ports
	}
	return b.Backend.Deploy(ctx, name, deployment)
}

func (b *proxyScaleBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if b.pending {
		return infra.OpState{}, nil
	}
	return b.Backend.OpStatus(ctx, op)
}

// Use independent instance metadata so one stale new replica cannot borrow the
// old replica's configuration acknowledgement or writer health.
func proxyScaleFixture(t *testing.T, rc *engine.Context, s *state) (*proxyScaleBackend, *[2]bool) {
	t.Helper()
	var proxy *member
	for _, m := range s.Members {
		if m.Role == "proxy" {
			proxy = m
		}
	}
	if proxy == nil {
		t.Fatal("missing proxy")
	}
	stale := new([2]bool)
	agents := admissionAgents{fakeAgents: rc.Agents.(fakeAgents), proxy: proxy.Name, peers: map[string]string{}}
	for i, instance := range []string{"first", "second"} {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v1/status" {
				current, err := load(rc.Cluster)
				if err != nil {
					http.Error(w, "checkpoint unavailable", http.StatusServiceUnavailable)
					return
				}
				version := current.find(proxy.Name).ConfigVersion
				if stale[i] {
					version = 0
				}
				json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: version})
				return
			}
			json.NewEncoder(w).Encode(myspec.ProxyStatusResponse{Backends: []myspec.ProxyBackend{admitted(s.data()[0].Name, myspec.ProxyWriterGroup, "ONLINE")}})
		}))
		t.Cleanup(server.Close)
		agents.ca = append(agents.ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})...)
		agents.peers[instance] = server.Listener.Addr().String()
	}
	b := &proxyScaleBackend{admissionBackend: &admissionBackend{Backend: rc.Backend, proxy: proxy.Name, count: proxy.Replicas, stops: map[string]int{}}}
	rc.Backend, rc.Agents = b, agents
	return b, stale
}

func setProxyScaleTarget(rc *engine.Context, count int, resources bool) {
	target := rc.Cluster.Spec.Components.Extra["proxy"]
	target.Replicas = count
	if resources {
		target.Resources.MinRAM++
		target.Resources.MaxRAM++
	}
	rc.Cluster.Spec.Components.Extra["proxy"] = target
	rc.Cluster.Generation++
}

func TestProxyCountOnlyScaleReplay(t *testing.T) {
	for _, topology := range []string{"single", "group", "cluster"} {
		t.Run(topology, func(t *testing.T) {
			var p *Core
			var rc *engine.Context
			var base *fakeBackend
			if topology == "cluster" {
				p, rc, base = setupPXC(t)
			} else {
				p, rc, base = setup(t, topology)
				if topology == "single" {
					setProxyScaleTarget(rc, 1, false)
				}
			}
			converge(t, p, rc, base)
			s, _ := load(rc.Cluster)
			b, stale := proxyScaleFixture(t, rc, s)
			for _, count := range []int{2, 1} {
				setProxyScaleTarget(rc, count, false)
				b.pending = true
				checkpointed := false
				for range 30 {
					p = New(Options{Provider: p.Name()})
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					base.apply(t, p, rc)
					s, _ = load(rc.Cluster)
					if s.find(b.proxy).Offline {
						t.Fatal("count-only scale took proxy offline")
					}
					if s.Rolling != nil && s.Rolling.Stage == "scaled" {
						checkpointed = true
						break
					}
				}
				if !checkpointed || s.find(b.proxy).PendingReplicas != count || s.find(b.proxy).Replicas == count {
					t.Fatal("scale intent/receipt not checkpointed with expected replica count")
				}
				// Replay a durable pending receipt through another CP instance.
				p = New(Options{Provider: p.Name()})
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				if rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
					t.Fatal("pending scale acknowledged")
				}
				b.pending = false
				if count == 2 {
					// A missing new instance cannot satisfy either readiness or
					// the D103 per-instance admission barrier while scaling.
					b.count = 1
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					s, _ = load(rc.Cluster)
					if s.Rolling == nil || s.Rolling.Stage != "scaled" {
						t.Fatal("missing new replica released scale checkpoint")
					}
					if err := proxyAdmission(t.Context(), rc, s, s.data()[0].Name, myspec.ProxyWriterGroup); err == nil {
						t.Fatal("admission used the pre-scale expected count")
					}
					b.count = 2
					stale[1] = true
					for range 3 {
						if _, err := p.Sync(t.Context(), rc); err != nil {
							t.Fatal(err)
						}
					}
					s, _ = load(rc.Cluster)
					if s.Rolling == nil || s.Rolling.Stage != "scaled" || rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
						t.Fatal("stale new proxy acknowledged")
					}
					status, err := p.Status(t.Context(), rc)
					if err != nil || status.Phase == domain.PhaseReady {
						t.Fatalf("stale replica readiness: %+v %v", status, err)
					}
					stale[1] = false
				}
				converge(t, p, rc, base)
			}
			if b.scales != 2 || b.stops[b.proxy] != 0 || b.resources != 0 || b.deploys != 0 {
				t.Fatalf("count-only calls: scale=%d stop=%d resources=%d deploy=%d", b.scales, b.stops[b.proxy], b.resources, b.deploys)
			}
		})
	}
}

func TestProxyResourceChangesKeepRestartPath(t *testing.T) {
	for _, count := range []int{1, 2} {
		t.Run(map[int]string{1: "resources", 2: "combined"}[count], func(t *testing.T) {
			p, rc, base := setup(t, "group")
			converge(t, p, rc, base)
			s, _ := load(rc.Cluster)
			b, _ := proxyScaleFixture(t, rc, s)
			setProxyScaleTarget(rc, count, true)
			converge(t, p, rc, base)
			if b.stops[b.proxy] != 1 || b.resources != 1 || b.deploys != 1 || b.scales != count-1 {
				t.Fatalf("restart calls: scale=%d stop=%d resources=%d deploy=%d", b.scales, b.stops[b.proxy], b.resources, b.deploys)
			}
		})
	}
}

func TestProxyScaleRejectsZeroReplicas(t *testing.T) {
	for _, topology := range []string{"group", "cluster"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, base := setup(t, "group")
			if topology == "cluster" {
				p, rc, base = setupPXC(t)
			}
			converge(t, p, rc, base)
			s, _ := load(rc.Cluster)
			b, _ := proxyScaleFixture(t, rc, s)
			setProxyScaleTarget(rc, 0, false)
			if _, err := p.reconcileRolling(t.Context(), rc, s); err == nil {
				t.Fatal("accepted scale below topology minimum")
			}
			if b.scales != 0 || b.stops[b.proxy] != 0 {
				t.Fatal("invalid scale caused backend side effects")
			}
		})
	}
}

func TestProxyCountOnlyDetection(t *testing.T) {
	m := &member{Role: "proxy", Version: "8.4.11-11", Replicas: 1, Resources: defaultResources(false), ProxyRuntimeHash: "admitted"}
	target := domain.ComponentSpec{Replicas: 2, Resources: m.Resources}
	capabilities := infra.Capabilities{ReplicaScalePreservesServing: true}
	if !proxyCountOnly(m, m.Version, target, "admitted", capabilities) {
		t.Fatal("count-only update was not detected")
	}
	if proxyCountOnly(m, "8.4.12-12", target, "admitted", capabilities) {
		t.Fatal("version change bypassed restart")
	}
	if proxyCountOnly(m, m.Version, target, "changed", capabilities) {
		t.Fatal("runtime/startup change bypassed restart")
	}
	if proxyCountOnly(m, m.Version, target, "admitted", infra.Capabilities{}) {
		t.Fatal("backend without preserving scale bypassed restart")
	}
	m.ProxyRuntimeHash = ""
	if proxyCountOnly(m, m.Version, target, "admitted", capabilities) {
		t.Fatal("unknown legacy runtime bypassed restart")
	}
	m.ProxyRuntimeHash = "admitted"
	target.Resources.MinRAM++
	if proxyCountOnly(m, m.Version, target, "admitted", capabilities) {
		t.Fatal("combined resource/count change bypassed restart")
	}
	target.Replicas = m.Replicas
	if proxyCountOnly(m, m.Version, target, "admitted", capabilities) {
		t.Fatal("resource-only change bypassed restart")
	}
}

func TestProxyScaleRuntimeAndBackendRestart(t *testing.T) {
	for _, change := range []string{"reader-port", "backend-reloads", "legacy-runtime"} {
		t.Run(change, func(t *testing.T) {
			p, rc, base := setup(t, "group")
			converge(t, p, rc, base)
			s, _ := load(rc.Cluster)
			b, _ := proxyScaleFixture(t, rc, s)
			if change == "reader-port" {
				rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"proxy":{"readerPort":true}}`)
			} else if change == "backend-reloads" {
				b.disruptiveScale = true
			} else {
				s.find(b.proxy).ProxyRuntimeHash = ""
				replayAdmissionState(t, rc, s)
			}
			setProxyScaleTarget(rc, 2, false)
			converge(t, p, rc, base)
			if b.stops[b.proxy] != 1 || b.resources != 1 || b.deploys != 1 || b.scales != 1 {
				t.Fatalf("restart calls: scale=%d stop=%d resources=%d deploy=%d", b.scales, b.stops[b.proxy], b.resources, b.deploys)
			}
			if change == "reader-port" {
				exposed := false
				for _, port := range b.ports {
					exposed = exposed || port.Port == 3307
				}
				if !exposed {
					t.Fatal("reader-port change did not reach deployed runtime ports")
				}
			}
		})
	}
}

func TestProxyScaleInRechecksMaintenanceWindow(t *testing.T) {
	p, rc, base := setup(t, "group")
	converge(t, p, rc, base)
	s, _ := load(rc.Cluster)
	b, _ := proxyScaleFixture(t, rc, s)
	setProxyScaleTarget(rc, 2, false)
	converge(t, p, rc, base)
	setProxyScaleTarget(rc, 1, false)
	s, _ = load(rc.Cluster)
	proxy := s.find(b.proxy)
	s.Rolling = &rollingState{To: rc.Cluster.Spec.Version, Order: []string{proxy.Name}, Stage: "prepare", Targets: map[string]domain.ComponentSpec{proxy.Name: desiredComponent(rc, proxy)}}
	closeWindow := func() {
		rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
	}
	closeWindow()
	if _, err := p.reconcileRolling(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if s.Rolling.Stage != "prepare" || proxy.PendingReplicas != 0 || proxy.Replicas != 2 || b.scales != 1 {
		t.Fatal("closed window allowed scale-in preparation")
	}
	rc.Cluster.Spec.Maintenance = nil
	s = replayAdmissionState(t, rc, s)
	if _, err := p.reconcileRolling(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if s.Rolling.Stage != "scale" || !s.Rolling.ScaleIn || s.find(b.proxy).Replicas != 2 || s.find(b.proxy).PendingReplicas != 1 {
		t.Fatal("scale-in intent lost committed size or disruptive flag")
	}
	closeWindow()
	s = replayAdmissionState(t, rc, s)
	p = New(Options{Provider: p.Name()})
	if _, err := p.reconcileRolling(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if s.Rolling.Stage != "scale" || b.scales != 1 {
		t.Fatal("replay crossed closed maintenance window")
	}
	rc.Cluster.Spec.Maintenance = nil
	replayAdmissionState(t, rc, s)
	converge(t, p, rc, base)
	if b.scales != 2 || b.stops[b.proxy] != 0 {
		t.Fatal("resumed scale-in did not use preserving scale")
	}
}

type terminalProxyScaleBackend struct {
	*proxyScaleBackend

	allowRecovery bool

	recoveries int
}

func (b *terminalProxyScaleBackend) SetContainerCount(ctx context.Context, name string, min, max int) (infra.Op, error) {
	op, err := b.proxyScaleBackend.SetContainerCount(ctx, name, min, max)
	if name == b.proxy && err == nil {
		op.ID = "terminal-scale"
	}
	return op, err
}

func (b *terminalProxyScaleBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "terminal-scale" {
		b.count = 1
		service, err := b.Backend.GetService(ctx, b.proxy)
		if err != nil {
			return infra.OpState{}, err
		}
		service.Status = infra.StatusFailed
		return infra.OpState{Failed: true, Message: "terminal placement failure"}, nil
	}
	return b.proxyScaleBackend.OpStatus(ctx, op)
}

func (b *terminalProxyScaleBackend) RecoverFailedScale(ctx context.Context, name string, min, max int) (infra.Op, bool, error) {
	if !b.allowRecovery || b.recoveries != 0 {
		return infra.Op{}, false, nil
	}
	b.recoveries++
	b.count = min
	service, err := b.Backend.GetService(ctx, name)
	if err != nil {
		return infra.Op{}, false, err
	}
	service.Status = infra.StatusRunning
	return infra.Op{ID: "retained-recovery"}, true, nil
}

func TestProxyTerminalScaleFailureReplayAndRevert(t *testing.T) {
	for _, recovery := range []string{"backend-proof", "restored-health"} {
		t.Run(recovery, func(t *testing.T) {
			p, rc, base := setup(t, "group")
			converge(t, p, rc, base)
			s, _ := load(rc.Cluster)
			scale, _ := proxyScaleFixture(t, rc, s)
			b := &terminalProxyScaleBackend{proxyScaleBackend: scale}
			rc.Backend = b
			setProxyScaleTarget(rc, 2, false)
			failed := false
			for range 30 {
				p = New(Options{Provider: p.Name()})
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				base.apply(t, p, rc)
				s, _ = load(rc.Cluster)
				if s.Rolling != nil && s.Rolling.Stage == "scale-failed" {
					failed = true
					break
				}
			}
			if !failed || s.find(b.proxy).PendingReplicas != 0 {
				t.Fatal("terminal failure did not release the pending readiness expectation")
			}
			for range 3 {
				p = New(Options{Provider: p.Name()})
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			}
			s, _ = load(rc.Cluster)
			if s.Rolling == nil || s.Rolling.Stage != "scale-failed" || s.find(b.proxy).Replicas != 1 || b.scales != 1 || b.recoveries != 0 {
				t.Fatal("terminal replay retried placement or failed to observe survivors")
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil || status.Phase != domain.PhaseDegraded {
				t.Fatalf("terminal scale readiness: %+v %v", status, err)
			}
			recorded := false
			for _, condition := range status.Conditions {
				recorded = recorded || condition.Type == "MySQLProxyScale" && condition.Status == "False" && condition.Reason == "ScaleFailed"
			}
			if !recorded || rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
				t.Fatal("terminal scale failure was not reported or was acknowledged as success")
			}
			setProxyScaleTarget(rc, 1, false)
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s, _ = load(rc.Cluster)
			if s.Rolling == nil || rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
				t.Fatal("revert accepted failed backend as healthy")
			}
			if recovery == "backend-proof" {
				b.allowRecovery = true
			} else {
				base.services[b.proxy].Status = infra.StatusRunning
			}
			p = New(Options{Provider: p.Name()})
			converge(t, p, rc, base)
			s, _ = load(rc.Cluster)
			if s.Rolling != nil || s.find(b.proxy).Replicas != 1 || s.find(b.proxy).PendingReplicas != 0 || b.scales != 1 || b.stops[b.proxy] != 0 || b.deploys != 0 {
				t.Fatal("reverted fleet did not converge without replaying terminal placement")
			}
			if recovery == "backend-proof" && b.recoveries != 1 {
				t.Fatal("did not adopt backend-proven recovery")
			}
		})
	}
}
