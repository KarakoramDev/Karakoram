// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package orphanvolumes

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type memoryStore struct {
	items map[string]domain.OrphanedVolume
}

func (s *memoryStore) ListOrphanedVolumes(_ context.Context, namespace string) ([]domain.OrphanedVolume, error) {
	out := make([]domain.OrphanedVolume, 0, len(s.items))
	for _, v := range s.items {
		if namespace != "" && domain.NormalizedNamespace(v.Namespace) != namespace {
			continue
		}
		out = append(out, v)
	}
	return out, nil
}
func (s *memoryStore) PutOrphanedVolume(_ context.Context, v *domain.OrphanedVolume) error {
	s.items[domain.ClusterRef{Namespace: v.Namespace, Name: v.Hostname}.Key()] = *v
	return nil
}
func (s *memoryStore) DeleteOrphanedVolume(_ context.Context, namespace, name string) error {
	delete(s.items, domain.ClusterRef{Namespace: namespace, Name: name}.Key())
	return nil
}

type fakeBackend struct {
	infra.Backend
	service      *infra.Service
	deletes      int
	polls        int
	op           infra.Op
	status       infra.OpState
	deleteErr    error
	pollErr      error
	vanishOnPoll bool
}

func (b *fakeBackend) GetService(context.Context, string) (*infra.Service, error) {
	return b.service, nil
}
func (b *fakeBackend) DeleteServiceByID(_ context.Context, hostname, id string) (infra.Op, error) {
	if b.service == nil || b.service.ID != id || b.service.Hostname != hostname {
		return infra.Op{}, errors.New("identity mismatch")
	}
	b.deletes++
	return b.op, b.deleteErr
}
func (b *fakeBackend) OpStatus(_ context.Context, op infra.Op) (infra.OpState, error) {
	b.polls++
	if op.ID != b.op.ID || op.ServiceID != "volume-id" {
		return infra.OpState{}, errors.New("wrong process")
	}
	if b.vanishOnPoll {
		b.service = nil
	}
	return b.status, b.pollErr
}

type eventRecorder struct{ items []domain.Event }

func (e *eventRecorder) Record(_ context.Context, v domain.Event) error {
	e.items = append(e.items, v)
	return nil
}

func fixture() (*Janitor, *memoryStore, *fakeBackend, *eventRecorder, *time.Time) {
	at := time.Date(2024, 1, 7, 0, 0, 0, 0, time.UTC)
	v := domain.OrphanedVolume{Hostname: "demom0v", ServiceID: "volume-id", Cluster: "demo", Member: "demom0", Attempts: 130, LastProcessID: "old-failed-process", NextAttemptAt: at}
	s := &memoryStore{items: map[string]domain.OrphanedVolume{v.Hostname: v}}
	b := &fakeBackend{service: &infra.Service{ID: v.ServiceID, Hostname: v.Hostname}, op: infra.Op{ID: "new-process"}}
	e := &eventRecorder{}
	j := New(Options{Store: s, Namespaces: engine.StaticNamespaces{DefaultBackend: b}, Events: e, Now: func() time.Time { return at }})
	return j, s, b, e, &at
}

func TestJanitorAbsentVolumeRemovesRecordAndEmitsEvent(t *testing.T) {
	j, s, b, e, _ := fixture()
	b.service = nil
	if err := j.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(s.items) != 0 || b.deletes != 0 || len(e.items) != 1 || e.items[0].Reason != "OrphanedVolumeDeleted" || e.items[0].Type != "Normal" || e.items[0].Cluster != "demo" {
		t.Fatal(s.items, b, e.items)
	}
}

func TestJanitorReplacementIsDroppedWithoutDeletion(t *testing.T) {
	j, s, b, e, _ := fixture()
	b.service.ID = "replacement"
	if err := j.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(s.items) != 0 || b.deletes != 0 || b.polls != 0 || len(e.items) != 0 {
		t.Fatal(s.items, b, e.items)
	}
}

func TestJanitorDoesNotTouchUnrecordedServices(t *testing.T) {
	j, s, b, _, _ := fixture()
	s.items = map[string]domain.OrphanedVolume{}
	if err := j.Tick(context.Background()); err != nil || b.deletes != 0 {
		t.Fatal(err, b.deletes)
	}
}

func TestJanitorInflightPollingAndConfirmedSuccess(t *testing.T) {
	j, s, b, e, at := fixture()
	ctx := context.Background()
	if err := j.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	v := s.items["demom0v"]
	if !v.ProcessPending || v.LastProcessID != "new-process" || v.Attempts != 131 || !v.LastAttemptAt.Equal(*at) || b.deletes != 1 || b.polls != 0 {
		t.Fatal(v, b)
	}
	// Reconstruct the janitor: in-flight bookkeeping must not depend on memory.
	j = New(j.Options)
	if err := j.Tick(ctx); err != nil || b.polls != 0 {
		t.Fatal(err, b.polls)
	}
	*at = v.NextAttemptAt
	if err := j.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	v = s.items[v.Hostname]
	if !v.ProcessPending || b.polls != 1 || b.deletes != 1 {
		t.Fatal(v, b)
	}
	*at = v.NextAttemptAt
	b.status, b.vanishOnPoll = infra.OpState{Done: true}, true
	if err := j.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if len(s.items) != 0 || b.deletes != 1 || len(e.items) != 1 || e.items[0].Reason != "OrphanedVolumeDeleted" {
		t.Fatal(s.items, b, e.items)
	}
}

func TestJanitorFailedProcessBackoffPreservesLatestProcess(t *testing.T) {
	j, s, b, e, at := fixture()
	ctx := context.Background()
	if err := j.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	*at = s.items["demom0v"].NextAttemptAt
	b.status = infra.OpState{Failed: true, Message: "volume deletion rejected"}
	if err := j.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	v := s.items["demom0v"]
	if v.ProcessPending || v.Attempts != 131 || v.JanitorFailures != 1 || v.LastError != "volume deletion rejected" || v.LastProcessID != "new-process" || v.NextAttemptAt.Sub(*at) != 2*time.Minute {
		t.Fatal(v)
	}
	if len(e.items) != 1 || e.items[0].Reason != "OrphanedVolumeDeleteFailed" || e.items[0].Type != "Warning" {
		t.Fatal(e.items)
	}
	if err := j.Tick(ctx); err != nil || b.deletes != 1 || b.polls != 1 {
		t.Fatal("backoff bypassed", err, b)
	}
	*at = v.NextAttemptAt
	if err := j.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if b.deletes != 2 || b.polls != 1 || s.items[v.Hostname].Attempts != 132 {
		t.Fatal("failed process was polled again instead of issuing next attempt", b)
	}
}

func TestJanitorImmediateFailuresDoubleBackoffToCap(t *testing.T) {
	j, s, b, _, at := fixture()
	b.deleteErr = errors.New("volume deletion rejected")
	b.op = infra.Op{}
	for i, delay := range []time.Duration{2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 16 * time.Minute, 32 * time.Minute, time.Hour, time.Hour} {
		if err := j.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
		v := s.items["demom0v"]
		if v.NextAttemptAt.Sub(*at) != delay || v.Attempts != 131+i || v.JanitorFailures != i+1 {
			t.Fatal(i, v)
		}
		*at = v.NextAttemptAt
	}
}

func TestJanitorPollingErrorDoesNotReissueDelete(t *testing.T) {
	j, s, b, _, at := fixture()
	ctx := context.Background()
	if err := j.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	*at = s.items["demom0v"].NextAttemptAt
	b.pollErr = errors.New("platform temporarily unavailable")
	if err := j.Tick(ctx); !errors.Is(err, b.pollErr) {
		t.Fatal(err)
	}
	v := s.items["demom0v"]
	if !v.ProcessPending || v.Attempts != 131 || b.deletes != 1 {
		t.Fatal(v, b)
	}
}

func TestJanitorRunReturnsOnCancellation(t *testing.T) {
	j, _, _, _, _ := fixture()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := j.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

type namespaceBackends struct {
	engine.StaticNamespaces
	backends map[string]*fakeBackend
}

func (n namespaceBackends) Backend(namespace string) (infra.Backend, error) {
	if namespace == "broken" {
		return nil, errors.New("relay disconnected")
	}
	b, ok := n.backends[namespace]
	if !ok {
		return nil, errors.New("namespace not found")
	}
	return b, nil
}

func TestJanitorSelectsNamespaceBackendAndPreservesUnavailableFence(t *testing.T) {
	at := time.Now().UTC()
	s := &memoryStore{items: map[string]domain.OrphanedVolume{}}
	backends := map[string]*fakeBackend{}
	for _, namespace := range []string{"default", "other", "broken"} {
		v := domain.OrphanedVolume{Namespace: namespace, Cluster: "same", Hostname: "samem0v", ServiceID: namespace + "-volume", NextAttemptAt: at}
		if namespace == "broken" {
			v.ProcessPending = true
			v.LastProcessID = "pending"
		}
		if err := s.PutOrphanedVolume(context.Background(), &v); err != nil {
			t.Fatal(err)
		}
		backends[namespace] = &fakeBackend{service: &infra.Service{ID: v.ServiceID, Hostname: v.Hostname}, op: infra.Op{ID: namespace + "-delete"}}
	}
	e := &eventRecorder{}
	j := New(Options{Store: s, Namespaces: namespaceBackends{backends: backends}, Events: e, Now: func() time.Time { return at }})
	if err := j.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, namespace := range []string{"default", "other"} {
		key := domain.ClusterRef{Namespace: namespace, Name: "samem0v"}.Key()
		if backends[namespace].deletes != 1 || s.items[key].LastProcessID != namespace+"-delete" {
			t.Fatal("wrong project backend selected", namespace, s.items, backends)
		}
	}
	v := s.items["broken:samem0v"]
	if !v.ProcessPending || v.LastProcessID != "pending" || v.Attempts != 0 || !v.NextAttemptAt.After(at) || backends["broken"].deletes != 0 {
		t.Fatal("namespace failure cleared deletion fence", v)
	}
	if len(e.items) != 1 || e.items[0].Namespace != "broken" || e.items[0].Reason != "NamespaceUnavailable" {
		t.Fatal(e.items)
	}
}
