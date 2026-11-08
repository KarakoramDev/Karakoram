// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package companion

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/datamcp/companionspec"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

type cpUpgradeProvider struct {
	*provider
	upstream string
}

func (p *cpUpgradeProvider) DataMcp(*domain.Cluster) (engine.DataMcpBackend, error) {
	return engine.DataMcpBackend{Kind: engine.DataMcpCompanion, Upstream: p.upstream}, nil
}

type cpUpgradeBackend struct {
	infra.Backend
	service   *infra.Service
	submitted []infra.Deployment
	reject    error
	targets   []string
	failed    bool
	atSubmit  func()
}

func (b *cpUpgradeBackend) GetService(context.Context, string) (*infra.Service, error) {
	return b.service, nil
}
func (b *cpUpgradeBackend) Deploy(_ context.Context, name string, d infra.Deployment) (infra.Op, error) {
	if b.atSubmit != nil {
		b.atSubmit()
	}
	d.Artifact.Reader = nil
	b.submitted = append(b.submitted, d)
	b.targets = append(b.targets, name)
	if b.reject != nil {
		return infra.Op{}, b.reject
	}
	return infra.Op{ID: "prepared-deployment"}, nil
}
func (b *cpUpgradeBackend) OpStatus(context.Context, infra.Op) (infra.OpState, error) {
	if b.failed {
		return infra.OpState{Failed: true, Status: "FAILED", Message: "prepare failed"}, nil
	}
	return infra.OpState{Done: true}, nil
}

func restoreCompanionCP(t *testing.T, upstream, path string) (*fixture, *cpUpgradeBackend, *enginetest.CPUpgradeFixture) {
	t.Helper()
	f := setup(t)
	snapshot := enginetest.RestoreCPUpgrade(t, f.rc, path)
	registry, err := engine.NewRegistry(&cpUpgradeProvider{provider: f.provider, upstream: upstream})
	if err != nil {
		t.Fatal(err)
	}
	f.controller.Registry = registry
	backend := &cpUpgradeBackend{Backend: f.backend, service: &snapshot.Services[0]}
	f.rc.Artifact = cpUpgradeArtifact{identity: "current-agent-build"}
	f.rc.Backend = backend
	return f, backend, snapshot
}

type cpUpgradeEvents struct{ events []domain.Event }

func (e *cpUpgradeEvents) Record(_ context.Context, event domain.Event) error {
	e.events = append(e.events, event)
	return nil
}

func TestCPUpgradeCompanionLockedBytes(t *testing.T) {
	for _, tc := range []struct {
		name, upstream, path string
		redeploy             bool
	}{
		{"mongodb-rebuilt", companionspec.UpstreamMongoDB, "testdata/cp_upgrade_eea8bd7.json", true},
		{"pgedge-unchanged", companionspec.UpstreamPostgres, "testdata/cp_upgrade_pgedge_eea8bd7.json", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, backend, snapshot := restoreCompanionCP(t, tc.upstream, tc.path)
			events := &cpUpgradeEvents{}
			f.rc.Events = events
			providerState := string(f.rc.Cluster.Internal)
			before, err := MemberConfig(t.Context(), f.rc, Member(f.rc.Cluster.Name))
			if err != nil {
				t.Fatal(err)
			}
			_, b, err := f.controller.backend(f.rc)
			if err != nil {
				t.Fatal(err)
			}
			current, err := f.controller.runtime(f.rc, b)
			if err != nil {
				t.Fatal(err)
			}
			if reflect.DeepEqual(snapshot.Runtime, current) == tc.redeploy {
				t.Fatal("legacy fixture does not exercise expected lock delta")
			}
			backend.atSubmit = func() {
				cfg, err := MemberConfig(t.Context(), f.rc, Member(f.rc.Cluster.Name))
				if err != nil || !reflect.DeepEqual(cfg, before) {
					t.Fatal("config activated before preparation was submitted", err)
				}
			}
			if tc.redeploy {
				f.rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{TimeZone: "UTC", Windows: []domain.MaintenanceWindow{{Schedule: "0 1 * * *", Duration: "1h"}}}
				for range 3 {
					result, err := f.controller.Sync(t.Context(), f.rc)
					if err != nil || result.RequeueAfter == 0 || len(backend.submitted) != 0 || len(events.events) != 0 {
						t.Fatal("CP upgrade bypassed the closed maintenance window", result, err)
					}
				}
				s, err := load(t.Context(), f.rc)
				if err != nil || s.Attempts != 0 {
					t.Fatal("closed window consumed deployment budget", s, err)
				}
				f.rc.Cluster.Spec.Maintenance = nil
				if _, err = f.controller.Sync(t.Context(), f.rc); err != nil {
					t.Fatal(err)
				}
				// A window closing cannot strand an already admitted rollout.
				f.rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{TimeZone: "UTC", Windows: []domain.MaintenanceWindow{{Schedule: "0 1 * * *", Duration: "1h"}}}
			}
			f.converge(t)
			for range 10 {
				f.converge(t)
			}
			want := 0
			if tc.redeploy {
				want = 1
			}
			if len(backend.submitted) != want {
				t.Fatalf("CP upgrade submitted %d deployments, want %d", len(backend.submitted), want)
			}
			if len(events.events) != want || want != 0 && events.events[0].Reason != "DataMcpCompanionRollout" {
				t.Fatal("missing or repeated rollout event", events.events)
			}
			if string(f.rc.Cluster.Internal) != providerState {
				t.Fatal("companion rollout changed database provider state")
			}
			for i, deployment := range backend.submitted {
				if backend.targets[i] != Member(f.rc.Cluster.Name) || deployment.Runtime.Volume != "" {
					t.Fatal("automatic rollout touched a database member or volume", backend.targets[i], deployment.Runtime.Volume)
				}
			}
			after, err := MemberConfig(t.Context(), f.rc, Member(f.rc.Cluster.Name))
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("same-path lock rebuild changed published config", err)
			}
			if tc.redeploy {
				a, err := nativeartifacts.Get(tc.upstream)
				if err != nil {
					t.Fatal(err)
				}
				prepare := strings.Join(backend.submitted[0].Runtime.Prepare, "\n")
				verify := strings.Index(prepare, "sha256sum -c -")
				extract := strings.Index(prepare, "tar --no-same-owner")
				if !strings.Contains(prepare, a.SHA256) || !strings.Contains(prepare, "curl -fsSL") || verify < 0 || extract <= verify {
					t.Fatal("automatic rollout omitted download/checksum verification before installing current locked bytes")
				}
			}
		})
	}
}

type cpUpgradeArtifact struct {
	artifact
	identity string
}

func (a cpUpgradeArtifact) ArtifactIdentity() string { return a.identity }

func TestCompanionAgentArchiveUpgrade(t *testing.T) {
	for _, upstream := range []string{companionspec.UpstreamPostgres, companionspec.UpstreamMongoDB} {
		t.Run(upstream, func(t *testing.T) {
			path := "testdata/cp_upgrade_eea8bd7.json"
			if upstream == companionspec.UpstreamPostgres {
				path = "testdata/cp_upgrade_pgedge_eea8bd7.json"
			}
			f, backend, _ := restoreCompanionCP(t, upstream, path)
			events := &cpUpgradeEvents{}
			f.rc.Events = events
			f.rc.Artifact = cpUpgradeArtifact{identity: "agent-build-one"}
			_, b, err := f.controller.backend(f.rc)
			if err != nil {
				t.Fatal(err)
			}
			runtime, err := f.controller.runtime(f.rc, b)
			if err != nil {
				t.Fatal(err)
			}
			s, err := load(t.Context(), f.rc)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(runtime)
			if err != nil {
				t.Fatal(err)
			}
			s.RuntimeHash = secrets.Hash(string(raw))
			if err = save(t.Context(), f.rc, s); err != nil {
				t.Fatal(err)
			}
			f.converge(t)
			// Reconstruct the CP controller and source with the same archive.
			f.controller = &Controller{Registry: f.controller.Registry, InternalCAPEM: f.controller.InternalCAPEM}
			f.rc.Artifact = cpUpgradeArtifact{identity: "agent-build-one"}
			f.converge(t)
			if len(backend.submitted) != 0 || len(events.events) != 0 {
				t.Fatal("unchanged archive or CP restart triggered a rollout")
			}
			f.rc.Artifact = cpUpgradeArtifact{identity: "agent-build-two"}
			closed := &domain.MaintenancePolicy{TimeZone: "UTC", Windows: []domain.MaintenanceWindow{{Schedule: "0 1 * * *", Duration: "1h"}}}
			f.rc.Cluster.Spec.Maintenance = closed
			for range 3 {
				result, err := f.controller.Sync(t.Context(), f.rc)
				if err != nil || result.RequeueAfter == 0 || len(backend.submitted) != 0 {
					t.Fatal("archive upgrade bypassed maintenance window", result, err)
				}
			}
			f.rc.Cluster.Spec.Maintenance = nil
			if _, err = f.controller.Sync(t.Context(), f.rc); err != nil {
				t.Fatal(err)
			}
			f.rc.Cluster.Spec.Maintenance = closed
			f.converge(t)
			f.controller = &Controller{Registry: f.controller.Registry, InternalCAPEM: f.controller.InternalCAPEM}
			for range 3 {
				f.converge(t)
			}
			if len(backend.submitted) != 1 || len(events.events) != 1 || events.events[0].Reason != "DataMcpCompanionRollout" {
				t.Fatalf("archive upgrade submitted %d rollouts and %d events, want exactly one each", len(backend.submitted), len(events.events))
			}
		})
	}
}

func TestCPUpgradeCompanionFailuresBounded(t *testing.T) {
	for _, mode := range []string{"rejected", "failed-receipt"} {
		t.Run(mode, func(t *testing.T) {
			f, backend, _ := restoreCompanionCP(t, companionspec.UpstreamMongoDB, "testdata/cp_upgrade_eea8bd7.json")
			before, err := MemberConfig(t.Context(), f.rc, Member(f.rc.Cluster.Name))
			if err != nil {
				t.Fatal(err)
			}
			if mode == "rejected" {
				backend.reject = errors.New("submission rejected")
				f.provider.password = "not-admitted"
			} else {
				backend.failed = true
			}
			for range 20 {
				_, _ = f.controller.Sync(t.Context(), f.rc)
				// Reconstruct the controller each time: the cap must survive CP restarts.
				f.controller = &Controller{Registry: f.controller.Registry, InternalCAPEM: f.controller.InternalCAPEM}
			}
			if len(backend.submitted) != deploymentAttemptLimit {
				t.Fatalf("%s submitted %d times, want bounded %d", mode, len(backend.submitted), deploymentAttemptLimit)
			}
			failure, ok := f.rc.Cluster.Status.DeploymentFailures[Member(f.rc.Cluster.Name)]
			if !ok || failure.Attempts != deploymentAttemptLimit {
				t.Fatal("missing terminal deployment evidence", failure)
			}
			status := domain.ClusterStatus{}
			if err = f.controller.Observe(t.Context(), f.rc, &status); err != nil || status.DataMcp.Phase != domain.DataMcpPhaseDegraded || !strings.Contains(status.DataMcp.Message, "DeploymentRetriesExhausted") {
				t.Fatal("exhaustion did not expose a clear terminal companion condition", status.DataMcp, err)
			}
			if backend.service.Status != infra.StatusActive {
				t.Fatal("exhausted rollout stopped the retained predecessor", backend.service.Status)
			}
			if mode == "rejected" {
				after, err := MemberConfig(t.Context(), f.rc, Member(f.rc.Cluster.Name))
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatal("rejected preparation published new config", err)
				}
			}
			f.rc.Cluster.Spec.RetryEpoch++
			_, _ = f.controller.Sync(t.Context(), f.rc)
			if len(backend.submitted) != deploymentAttemptLimit+1 {
				t.Fatal("explicit retry did not rearm companion")
			}
			f.rc.Artifact = cpUpgradeArtifact{identity: "next-agent-build"}
			for range 20 {
				_, _ = f.controller.Sync(t.Context(), f.rc)
				f.controller = &Controller{Registry: f.controller.Registry, InternalCAPEM: f.controller.InternalCAPEM}
			}
			if len(backend.submitted) != 2*deploymentAttemptLimit+1 {
				t.Fatal("changed agent archive did not receive its own bounded retry budget", len(backend.submitted))
			}
		})
	}
}

func TestCompanionPublicationUsesAdmittedArtifact(t *testing.T) {
	f, _, _ := restoreCompanionCP(t, companionspec.UpstreamMongoDB, "testdata/cp_upgrade_eea8bd7.json")
	s, err := load(t.Context(), f.rc)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := nativeartifacts.Get(companionspec.UpstreamMongoDB)
	if err != nil {
		t.Fatal(err)
	}
	admitted.Version = "previous-installed-version"
	s.Artifact = &admitted
	_, b, err := f.controller.backend(f.rc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = publish(t.Context(), f.rc, f.provider, b, s); err != nil {
		t.Fatal(err)
	}
	cfg, err := MemberConfig(t.Context(), f.rc, Member(f.rc.Cluster.Name))
	if err != nil {
		t.Fatal(err)
	}
	var spec companionspec.Spec
	if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Version != admitted.Version || spec.InstallDir != installDir(admitted) {
		t.Fatal("publication ignored persisted admitted artifact", spec)
	}
}

func TestPendingCompanionKeepsAdmittedArtifactAcrossCPUpgrade(t *testing.T) {
	f, backend, _ := restoreCompanionCP(t, companionspec.UpstreamMongoDB, "testdata/cp_upgrade_eea8bd7.json")
	s, err := load(t.Context(), f.rc)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := nativeartifacts.Get(companionspec.UpstreamMongoDB)
	if err != nil {
		t.Fatal(err)
	}
	// Model a pending prepared rollout accepted by the previous CP. A later
	// lock's new version must not replace that admitted config while waiting.
	admitted.Version = "previous-admitted-version"
	s.Artifact = &admitted
	s.Stage = "deploy"
	s.Pending = &infra.Op{ID: "previous-cp-deployment"}
	if err = save(t.Context(), f.rc, s); err != nil {
		t.Fatal(err)
	}
	if _, err = f.controller.Sync(t.Context(), f.rc); err != nil {
		t.Fatal(err)
	}
	cfg, err := MemberConfig(t.Context(), f.rc, Member(f.rc.Cluster.Name))
	if err != nil {
		t.Fatal(err)
	}
	var spec companionspec.Spec
	if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Version != admitted.Version || spec.InstallDir != installDir(admitted) || len(backend.submitted) != 0 {
		t.Fatal("pending receipt activated a newer unprepared lock", spec)
	}
}

func TestCompanionDeploymentWaitPreservesBudget(t *testing.T) {
	f, backend, _ := restoreCompanionCP(t, companionspec.UpstreamMongoDB, "testdata/cp_upgrade_eea8bd7.json")
	backend.reject = &infra.DeploymentWaitError{Reason: "DeploymentSlot", Key: "companion-slot", Detail: "waiting for deployment capacity", RetryAt: time.Now().Add(time.Minute)}
	for range deploymentAttemptLimit + 2 {
		_, err := f.controller.Sync(t.Context(), f.rc)
		var waiting *engine.PlatformRetryWait
		if !errors.As(err, &waiting) {
			t.Fatal("lost platform backpressure", err)
		}
	}
	s, err := load(t.Context(), f.rc)
	if err != nil {
		t.Fatal(err)
	}
	if s.Attempts != 0 || len(f.rc.Cluster.Status.DeploymentFailures) != 0 {
		t.Fatal("platform waiting consumed the failure budget", s.Attempts)
	}
	backend.reject = nil
	f.converge(t)
}
