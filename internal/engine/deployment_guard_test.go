// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type deploymentGuardBackend struct {
	infra.Backend
	calls int
	epoch int64
	err   error
}

func (b *deploymentGuardBackend) Deploy(_ context.Context, _ string, d infra.Deployment) (infra.Op, error) {
	b.calls++
	b.epoch = d.RetryEpoch
	return infra.Op{ID: "deployment"}, b.err
}

type deploymentGuardEvents struct{ events []domain.Event }

func (r *deploymentGuardEvents) Record(_ context.Context, e domain.Event) error {
	r.events = append(r.events, e)
	return nil
}

type deploymentGuardWait struct{}

func (*deploymentGuardWait) Error() string {
	return "waiting for a platform deployment slot: 2/2 in flight"
}
func (e *deploymentGuardWait) SafeDetail() string    { return e.Error() }
func (*deploymentGuardWait) DeploymentWaiting() bool { return true }

func TestDeploymentGuardPersistsExhaustionAndRearms(t *testing.T) {
	b := &deploymentGuardBackend{err: &infra.DeploymentExhaustedError{Service: "member", Version: "app-version", Attempts: 3, LastError: "build failed"}}
	events := &deploymentGuardEvents{}
	rc := &Context{Cluster: &domain.Cluster{Name: "demo"}, Backend: b, Events: events, Artifact: runtimeArtifactSource{artifact: &trackedArtifact{Reader: strings.NewReader("archive")}}}
	spec := infra.RuntimeSpec{Args: []string{"agent"}}
	if _, err := Deploy(t.Context(), rc, "member", spec); !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
		t.Fatal(err)
	}
	if b.calls != 1 || len(events.events) != 1 {
		t.Fatal(b.calls, events.events)
	}
	status := domain.ClusterStatus{Phase: domain.PhaseReady}
	ApplyDeploymentStatus(rc, &status)
	if status.Phase != domain.PhaseDegraded || len(status.Conditions) != 1 || status.Conditions[0].Reason != "DeploymentRetriesExhausted" {
		t.Fatal(status)
	}
	for _, text := range []string{"member", "app-version", "3 attempts", "build failed", "karactl clusters retry-deployments demo"} {
		if !strings.Contains(status.Message, text) {
			t.Fatalf("missing %q: %s", text, status.Message)
		}
	}
	// Round-trip durable state, then retry with no artifact source: blocked paths
	// must not even reopen an artifact or invoke the backend.
	raw, err := json.Marshal(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	var restored domain.Cluster
	if err = json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	rc.Cluster = &restored
	rc.Artifact = nil
	if _, err = Deploy(t.Context(), rc, "member", spec); !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
		t.Fatal(err)
	}
	if b.calls != 1 || len(events.events) != 1 {
		t.Fatal(b.calls, events.events)
	}
	rc.Cluster.Spec.RetryEpoch++
	rc.Artifact = runtimeArtifactSource{artifact: &trackedArtifact{Reader: strings.NewReader("archive")}}
	b.err = nil
	if _, err = Deploy(t.Context(), rc, "member", spec); err != nil {
		t.Fatal(err)
	}
	if b.calls != 2 || b.epoch != 1 || len(rc.Cluster.Status.DeploymentFailures) != 0 {
		t.Fatal(b.calls, b.epoch, rc.Cluster.Status)
	}
}

func TestDeploymentGuardNewDesiredRuntimeUnblocks(t *testing.T) {
	rc := &Context{Cluster: &domain.Cluster{Name: "demo"}}
	spec := infra.RuntimeSpec{Args: []string{"agent"}}
	cause := &infra.DeploymentExhaustedError{Attempts: 3}
	RecordDeploymentExhausted(t.Context(), rc, "member", spec, cause)
	if !DeploymentBlocked(rc, "member", spec) {
		t.Fatal("missing fence")
	}
	if DeploymentBlocked(rc, "member", infra.RuntimeSpec{Args: []string{"different"}}) {
		t.Fatal("changed runtime blocked")
	}
	rc.Cluster.Spec.RetryEpoch = -1
	if !DeploymentBlocked(rc, "member", spec) {
		t.Fatal("lower epoch rearmed")
	}
}

func TestDeploymentWaitingRetainsDiagnosticAndSchedule(t *testing.T) {
	b := &deploymentGuardBackend{err: &infra.RetryAfterError{Cause: &deploymentGuardWait{}, At: time.Now().Add(time.Minute)}}
	events := &deploymentGuardEvents{}
	rc := &Context{Cluster: &domain.Cluster{Name: "demo"}, Backend: b, Events: events, Artifact: runtimeArtifactSource{artifact: &trackedArtifact{Reader: strings.NewReader("archive")}}}
	_, err := Deploy(t.Context(), rc, "member", infra.RuntimeSpec{})
	var wait *PlatformRetryWait
	if !errors.As(err, &wait) || !strings.Contains(wait.Message, "waiting for a platform deployment slot") {
		t.Fatal(err)
	}
	status := domain.ClusterStatus{}
	ApplyDeploymentStatus(rc, &status)
	if !strings.Contains(status.Message, "2/2 in flight") || status.Conditions[0].Type != "PlatformDeploymentWaiting" || len(events.events) != 1 {
		t.Fatal(status, events.events)
	}
}

func TestDeploymentWaitDeduplicatesStableIdentity(t *testing.T) {
	at := time.Now().Add(time.Minute)
	b := &deploymentGuardBackend{}
	events := &deploymentGuardEvents{}
	rc := &Context{Cluster: &domain.Cluster{Name: "demo"}, Backend: b, Events: events, Artifact: runtimeArtifactSource{artifact: &trackedArtifact{Reader: strings.NewReader("archive")}}}
	for i, detail := range []string{"process age 10s", "process age 25s", "different blocker age 1s"} {
		key := "service/process/RUNNING"
		if i == 2 {
			key = "service/new-process/PENDING"
		}
		b.err = &infra.DeploymentWaitError{Reason: "DeploymentSlot", Key: key, Detail: detail, RetryAt: at.Add(time.Duration(i) * time.Minute)}
		_, err := Deploy(t.Context(), rc, "member", infra.RuntimeSpec{})
		var wait *PlatformRetryWait
		if !errors.As(err, &wait) || !infra.RetryAt(err).Equal(at.Add(time.Duration(i)*time.Minute)) {
			t.Fatalf("converted wait lost backend deadline: %v", err)
		}
		status := domain.ClusterStatus{}
		ApplyDeploymentStatus(rc, &status)
		if !strings.Contains(status.Message, detail) || status.Conditions[0].Key != key {
			t.Fatalf("status lost fresh diagnostic: %+v", status)
		}
		rc.Cluster.Status = status
		want := 1
		if i == 2 {
			want = 2
		}
		if len(events.events) != want {
			t.Fatalf("events=%+v", events.events)
		}
	}
}

type identifiedArtifact struct {
	runtimeArtifactSource
	identity string
}

func (a identifiedArtifact) ArtifactIdentity() string { return a.identity }

func TestDeploymentFenceCorrectedArtifactUnblocksOnce(t *testing.T) {
	b := &deploymentGuardBackend{err: &infra.DeploymentExhaustedError{Attempts: 3}}
	rc := &Context{Cluster: &domain.Cluster{Name: "demo"}, Backend: b}
	source := runtimeArtifactSource{artifact: &trackedArtifact{Reader: strings.NewReader("archive")}}
	rc.Artifact = identifiedArtifact{runtimeArtifactSource: source, identity: "karakoram/100/build-one"}
	spec := infra.RuntimeSpec{}
	_, _ = Deploy(t.Context(), rc, "member", spec)
	raw, err := json.Marshal(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	var restored domain.Cluster
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	rc.Cluster = &restored
	for range 3 {
		_, _ = Deploy(t.Context(), rc, "member", spec)
	}
	if b.calls != 1 {
		t.Fatalf("unchanged release replayed %d times", b.calls)
	}
	rc.Artifact = identifiedArtifact{runtimeArtifactSource: source, identity: "karakoram/100/build-two"}
	_, _ = Deploy(t.Context(), rc, "member", spec)
	_, _ = Deploy(t.Context(), rc, "member", spec)
	if b.calls != 2 {
		t.Fatalf("corrected release did not unfence exactly once: %d", b.calls)
	}
}

func TestExecutableArtifactHasStableCheapIdentity(t *testing.T) {
	a := ExecutableArtifact{}
	if id := a.ArtifactIdentity(); id == "" || id != a.ArtifactIdentity() {
		t.Fatalf("unstable executable identity: %q", id)
	}
}
