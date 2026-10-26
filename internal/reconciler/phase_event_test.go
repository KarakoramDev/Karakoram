// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestPhaseChangedRetainsReasonAfterUnchangedGenerationRecovery(t *testing.T) {
	p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
	phase := domain.PhaseUpdating
	message := "Elasticsearch convergence: waiting for initializing or relocating shards; initializing=1, relocating=0"
	p.syncFn = func(*engine.Context) (engine.Result, error) {
		return engine.Result{RequeueAfter: time.Second}, nil
	}
	p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
		status := domain.ClusterStatus{Phase: phase}
		if phase == domain.PhaseUpdating {
			status.Message = message
			status.Conditions = []domain.Condition{
				{Type: "ClusterHealth", Status: "False", Reason: "yellow", Message: "private-condition-body"},
				{Type: "NativeAccess", Status: "Unknown", Reason: "ObservationPending"},
				{Type: "Certificates", Status: "True", Reason: "ClusterTLS"},
			}
		}
		return status, nil
	}
	m, st := newTestManager(t, p)
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake", Version: "private-spec-value"}, Internal: json.RawMessage(`{"private":"private-internal-value"}`)}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	c.Status = domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: c.Generation}
	if err := st.UpdateClusterState(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	for _, next := range []domain.Phase{domain.PhaseUpdating, domain.PhaseReady} {
		phase = next
		if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
			t.Fatal(err)
		}
		got, err := st.GetCluster(t.Context(), c.Ref())
		if err != nil || got.Status.Phase != next || got.Generation != c.Generation || got.Status.ObservedGeneration != c.Generation {
			t.Fatalf("cluster=%+v err=%v", got, err)
		}
	}
	events, err := st.ListEvents(t.Context(), store.EventFilter{Cluster: c.Name})
	if err != nil {
		t.Fatal(err)
	}
	transitions := 0
	foundUpdating := false
	for _, event := range events {
		if event.Reason != "PhaseChanged" {
			continue
		}
		transitions++
		if !strings.HasPrefix(event.Message, "Ready → Updating") {
			continue
		}
		foundUpdating = true
		for _, want := range []string{message, "ClusterHealth / False / yellow", "NativeAccess / Unknown / ObservationPending"} {
			if !strings.Contains(event.Message, want) {
				t.Fatalf("missing %q in retained event: %s", want, event.Message)
			}
		}
		for _, excluded := range []string{"private-condition-body", "private-spec-value", "private-internal-value", "Certificates / True"} {
			if strings.Contains(event.Message, excluded) {
				t.Fatalf("unexpected %q in retained event", excluded)
			}
		}
	}
	if transitions != 2 || !foundUpdating {
		t.Fatalf("phase transitions=%d, found Updating explanation=%v", transitions, foundUpdating)
	}
}

func TestPhaseChangeMessageBoundsStatusAndConditions(t *testing.T) {
	c := &domain.Cluster{Status: domain.ClusterStatus{Phase: domain.PhaseUpdating, Message: strings.Repeat("界", 512)}}
	for range 100 {
		c.Status.Conditions = append(c.Status.Conditions, domain.Condition{Type: strings.Repeat("界", 64), Status: "Unknown", Reason: strings.Repeat("界", 64)})
	}
	message := phaseChangeMessage(c, domain.PhaseReady)
	if len(message) > 1024 || !utf8.ValidString(message) || !strings.Contains(message, "[conditions:") {
		t.Fatalf("invalid bounded phase explanation: %q", message)
	}
}

func TestPhaseChangedPreservesAssociatedSafeProbeFailure(t *testing.T) {
	p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
	const reason = "Elasticsearch health probe failed; shard health is unavailable"
	probeErr := errors.New("private-probe-response")
	p.syncFn = func(rc *engine.Context) (engine.Result, error) {
		rc.SyncWait = reason
		return engine.Result{RequeueAfter: time.Second}, &safePhaseProbeError{cause: probeErr, message: reason}
	}
	p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
		return domain.ClusterStatus{Phase: domain.PhaseUpdating, Message: reason}, nil
	}
	m, st := newTestManager(t, p)
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	c.Status = domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: c.Generation}
	if err := st.UpdateClusterState(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(t.Context(), c.Ref()); !errors.Is(err, probeErr) {
		t.Fatalf("probe error was not preserved: %v", err)
	}
	got, err := st.GetCluster(t.Context(), c.Ref())
	if err != nil || !strings.Contains(got.Status.Message, reason) || strings.Contains(got.Status.Message, probeErr.Error()) || got.Status.Phase != domain.PhaseUpdating || got.Status.ObservedGeneration != c.Generation {
		t.Fatalf("status=%+v err=%v", got, err)
	}
	events, err := st.ListEvents(t.Context(), store.EventFilter{Cluster: c.Name})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Reason == "PhaseChanged" {
			if !strings.Contains(event.Message, reason) || strings.Contains(event.Message, probeErr.Error()) {
				t.Fatalf("unsafe phase event: %s", event.Message)
			}
			return
		}
	}
	t.Fatal("missing phase event")
}

type safePhaseProbeError struct {
	cause   error
	message string
}

func (e *safePhaseProbeError) Error() string { return e.message }
func (e *safePhaseProbeError) Unwrap() error { return e.cause }

func TestPhaseChangedRedactsStatusDiagnosticsBeforeRecording(t *testing.T) {
	m, st := newTestManager(t, lifecycleProvider())
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	c.Status = domain.ClusterStatus{
		Phase:   domain.PhaseUpdating,
		Message: "probe failed mongodb://fixture-user:fixture-password@db.invalid/ token=fixture-token",
	}
	safe := phaseChangeMessage(c, domain.PhaseReady)
	for _, secret := range []string{"fixture-user", "fixture-password", "fixture-token"} {
		if strings.Contains(safe, secret) {
			t.Fatalf("phase diagnostic leaked credential before recording: %q", secret)
		}
	}
	if err := m.state(t.Context(), c, domain.PhaseReady); err != nil {
		t.Fatal(err)
	}
	events, err := st.ListEvents(t.Context(), store.EventFilter{Cluster: c.Name})
	if err != nil || len(events) != 1 {
		t.Fatalf("events=%+v err=%v", events, err)
	}
	for _, secret := range []string{"fixture-user", "fixture-password", "fixture-token"} {
		if strings.Contains(events[0].Message, secret) {
			t.Fatalf("phase diagnostic leaked credential %q", secret)
		}
	}
	if !strings.Contains(events[0].Message, "probe failed") || !strings.Contains(events[0].Message, "[redacted]") {
		t.Fatalf("missing redacted diagnostic: %s", events[0].Message)
	}
}

func TestStaleSyncWaitDoesNotMaskLaterFailure(t *testing.T) {
	p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
	failure := errors.New("later platform step failed")
	p.syncFn = func(rc *engine.Context) (engine.Result, error) {
		rc.SyncWait = "retiring unplaced scale-down members"
		return engine.Result{}, failure
	}
	p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
		return domain.ClusterStatus{Phase: domain.PhaseUpdating}, nil
	}
	m, st := newTestManager(t, p)
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	c.Status = domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: c.Generation}
	if err := st.UpdateClusterState(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(t.Context(), c.Ref()); !errors.Is(err, failure) {
		t.Fatalf("missing original failure: %v", err)
	}
	got, err := st.GetCluster(t.Context(), c.Ref())
	if err != nil || !strings.Contains(got.Status.Message, failure.Error()) || strings.Contains(got.Status.Message, "retiring unplaced") {
		t.Fatalf("masked failure: status=%+v err=%v", got, err)
	}
}

func TestPhaseChangedRedactsCredentialBeforeStatusTruncation(t *testing.T) {
	m, st := newTestManager(t, lifecycleProvider())
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	c.Status = domain.ClusterStatus{
		Phase:   domain.PhaseUpdating,
		Message: strings.Repeat("x", 480) + " mongodb://alice:abcdef" + strings.Repeat("p", 100) + "@db.invalid/",
	}
	if err := m.state(t.Context(), c, domain.PhaseReady); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetCluster(t.Context(), c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	events, err := st.ListEvents(t.Context(), store.EventFilter{Cluster: c.Name})
	if err != nil || len(events) != 1 {
		t.Fatalf("events=%+v err=%v", events, err)
	}
	for _, message := range []string{c.Status.Message, got.Status.Message, events[0].Message} {
		if strings.Contains(message, "alice") || strings.Contains(message, "abcdef") || !strings.Contains(message, "[redacted]") {
			t.Fatalf("credential was truncated before redaction: %q", message)
		}
	}
}
