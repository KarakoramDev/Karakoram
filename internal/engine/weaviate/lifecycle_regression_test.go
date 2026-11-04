// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type exhaustedLifecycleBackend struct {
	infra.Backend
	exhausted  bool
	statusOnly bool
	polls      int
}

func (b *exhaustedLifecycleBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	b.polls++
	if b.exhausted {
		st := infra.OpState{Failed: true, Status: "DeploymentRetriesExhausted", Message: "deployment budget exhausted"}
		if b.statusOnly {
			return st, nil
		}
		return st, &infra.DeploymentExhaustedError{Service: op.Hostname, Attempts: 3, LastError: st.Message}
	}
	return b.Backend.OpStatus(ctx, op)
}

func TestPendingDeploymentExhaustionRearmsSameOwner(t *testing.T) {
	for _, statusOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "terminal-error", true: "terminal-status"}[statusOnly], func(t *testing.T) {
			f := coreSetup(t, 1)
			s := coreConverge(t, f)
			m := s.Members[0]
			s.Pending = &pending{Action: "deploy", Member: m.Name}
			s.Rolling = &rolling{Reason: "upgrade", Order: []string{m.Name}, Step: "ack"}
			b := &exhaustedLifecycleBackend{Backend: f.backend, statusOnly: statusOnly}
			f.rc.Backend = b
			if _, err := f.p.advancePending(t.Context(), f.rc, s); err != nil {
				t.Fatal(err)
			}
			b.exhausted = true
			if _, err := f.p.advancePending(t.Context(), f.rc, s); !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
				t.Fatalf("missing exhaustion: %v", err)
			}
			if s.Pending == nil || s.Pending.Op != nil || s.Rolling.Step != "ack" || s.Rolling.Position != 0 {
				t.Fatal("lost same-member owner")
			}
			if err := s.save(f.rc.Cluster); err != nil {
				t.Fatal(err)
			}
			status, err := f.p.Status(t.Context(), f.rc)
			if err != nil || !strings.Contains(status.Message, "DeploymentRetriesExhausted") {
				t.Fatalf("missing durable failure: %+v %v", status, err)
			}
			before, polls := len(f.backend.Calls()), b.polls
			for range 3 {
				if _, err := f.p.Sync(t.Context(), f.rc); err != nil {
					t.Fatal(err)
				}
			}
			for _, call := range f.backend.Calls()[before:] {
				if call == "Deploy" {
					t.Fatal("redeployed fenced intent")
				}
			}
			if b.polls != polls {
				t.Fatal("terminal receipt polled forever")
			}
			f.rc.Cluster.Spec.RetryEpoch++
			b.exhausted = false
			if _, err := f.p.Sync(t.Context(), f.rc); err != nil {
				t.Fatal(err)
			}
			s, _ = load(f.rc.Cluster)
			if s.Pending == nil || s.Pending.Op == nil || s.Rolling.Step != "ack" {
				t.Fatal("rearm did not resubmit same owner")
			}
			deployed := false
			for _, call := range f.backend.Calls()[before:] {
				deployed = deployed || call == "Deploy"
			}
			if !deployed {
				t.Fatal("retryEpoch did not redeploy")
			}
		})
	}
}

type interruptedTokenDeletion struct {
	engine.SecretStore
	interrupted bool
}

var errTeardownInterrupted = errors.New("teardown interrupted after token deletion")

func (s *interruptedTokenDeletion) DeleteAgentToken(ctx context.Context, ref domain.ClusterRef, name string) error {
	if err := s.SecretStore.DeleteAgentToken(ctx, ref, name); err != nil {
		return err
	}
	if !s.interrupted {
		s.interrupted = true
		return errTeardownInterrupted
	}
	return nil
}
func TestCleanupResumesAfterPartialTokenDeletion(t *testing.T) {
	f := coreSetup(t, 3)
	coreConverge(t, f)
	secretStore := &interruptedTokenDeletion{SecretStore: f.rc.Secrets}
	f.rc.Secrets = secretStore
	interrupted := false
	for range 150 {
		done, err := f.p.Cleanup(t.Context(), f.rc, domain.DeleteOptions{})
		if errors.Is(err, errTeardownInterrupted) {
			interrupted = true
			break
		}
		if err != nil || done {
			t.Fatalf("premature teardown result: %v %v", done, err)
		}
	}
	if !interrupted {
		t.Fatal("did not exercise partial secret deletion")
	}
	s, err := load(f.rc.Cluster)
	if err != nil || s.Phase != "cleanup-secrets" {
		t.Fatalf("missing durable teardown phase: %+v %v", s, err)
	}
	if tok, err := secretStore.GetAgentToken(t.Context(), f.rc.Cluster.Ref(), s.Members[0].Name); err == nil && tok != nil {
		t.Fatal("first token was not deleted")
	}
	if err := s.save(f.rc.Cluster); err != nil {
		t.Fatal(err)
	}
	before := len(f.backend.Calls())
	done, err := f.p.Cleanup(t.Context(), f.rc, domain.DeleteOptions{})
	if err != nil || !done {
		t.Fatalf("replay stranded teardown: %v %v", done, err)
	}
	if len(f.backend.Calls()) != before {
		t.Fatal("secret-only replay revisited backend ownership")
	}
}

func replaceWithForeignService(t *testing.T, f *coreFixture, name string, kind infra.ServiceKind) {
	t.Helper()
	if _, err := f.backend.DeleteService(t.Context(), name); err != nil {
		t.Fatal(err)
	}
	spec := infra.ServiceSpec{Hostname: name, Kind: kind}
	if kind == infra.KindRuntime {
		spec.Base = infra.BaseUbuntu2404
	}
	if _, err := f.backend.ImportServices(t.Context(), []infra.ServiceSpec{spec}); err != nil {
		t.Fatal(err)
	}
}
func TestPendingMutationsRejectReusedHostname(t *testing.T) {
	for _, action := range []string{"deploy", "stop", "delete-runtime", "delete-volume", "resources", "volume-resources"} {
		t.Run(action, func(t *testing.T) {
			f := coreSetup(t, 1)
			s := coreConverge(t, f)
			m := s.Members[0]
			s.Pending = &pending{Action: action, Member: m.Name}
			name, kind := m.Name, infra.KindRuntime
			if action == "delete-volume" || action == "volume-resources" {
				name, kind = m.Volume, infra.KindVolume
				if _, err := f.backend.DeleteService(t.Context(), m.Name); err != nil {
					t.Fatal(err)
				}
			}
			replaceWithForeignService(t, f, name, kind)
			before := len(f.backend.Calls())
			for range 2 {
				s.Pending.RetryAt = time.Time{}
				if _, err := f.p.advancePending(t.Context(), f.rc, s); !errors.Is(err, engine.ErrConflict) || !strings.Contains(err.Error(), "ownership") {
					t.Fatalf("replacement accepted: %v", err)
				}
			}
			for _, call := range f.backend.Calls()[before:] {
				switch call {
				case "Deploy", "Stop", "DeleteServiceByID", "SetResources":
					t.Fatalf("mutated replacement: %s", call)
				}
			}
		})
	}
}
func TestScaleDownStopRevalidatesQueuedOwner(t *testing.T) {
	f := coreSetup(t, 4)
	s := coreConverge(t, f)
	m := s.Members[3]
	f.rc.Cluster.Spec.Components.Engine.Replicas = 3
	s.ScaleDown = &scaleDown{Member: m.Name, Stage: "stop", Target: 3}
	if _, err := f.p.advanceScaleDown(t.Context(), f.rc, s); err != nil {
		t.Fatal(err)
	}
	replaceWithForeignService(t, f, m.Name, infra.KindRuntime)
	before := len(f.backend.Calls())
	if _, err := f.p.advancePending(t.Context(), f.rc, s); !errors.Is(err, engine.ErrConflict) {
		t.Fatalf("queued stop accepted replacement: %v", err)
	}
	for _, call := range f.backend.Calls()[before:] {
		if call == "Stop" {
			t.Fatal("stopped foreign replacement")
		}
	}
}
func TestRuntimeRecoveryImportRevalidatesOwner(t *testing.T) {
	f := coreSetup(t, 1)
	s := coreConverge(t, f)
	m := s.Members[0]
	m.RuntimeRecovery.ImportPlan = engine.NewImportPlan([]infra.ServiceSpec{{Hostname: m.Name, Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404}})
	replaceWithForeignService(t, f, m.Name, infra.KindRuntime)
	before := len(f.backend.Calls())
	if _, err := drainRuntimeRecovery(t.Context(), f.rc, s); !errors.Is(err, engine.ErrConflict) || !strings.Contains(err.Error(), "ownership") {
		t.Fatalf("recovery accepted replacement: %v", err)
	}
	for _, call := range f.backend.Calls()[before:] {
		if call == "ImportServices" {
			t.Fatal("resubmitted import over foreign replacement")
		}
	}
}
