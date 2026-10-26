// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestInitialRestoreClaimConflictNamesOperation(t *testing.T) {
	s, _, token := testServer(t)
	ctx := context.Background()
	c := &domain.Cluster{Name: "initialclone", Spec: domain.ClusterSpec{Provider: "fake", Version: "8.0", Topology: domain.Topology{Type: "replicaSet"}}}
	op := &domain.Operation{Kind: domain.OperationCreate}
	if err := s.Store.CreateCluster(store.WithOperation(ctx, op), c); err != nil {
		t.Fatal(err)
	}
	// Install source intent directly: this fixture tests mutation routing, not
	// source backup admission (covered by store dependency tests).
	if _, err := s.Store.GetOperation(ctx, op.ID); err != nil {
		t.Fatal(err)
	}
	c.Spec.DataSource = &domain.DataSource{Cluster: "source", Backup: "daily"}
	if err := s.Store.UpdateClusterSpec(ctx, c.Ref(), c.Spec); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.ClaimInitialDataSource(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	w := call(s, "PATCH", "/api/v1/clusters/initialclone", `{"spec":{"components":{"engine":{"replicas":3}}}}`, token)
	var body api.Error
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if w.Code != 409 || body.Code != "conflict" || !strings.Contains(body.Message, op.ID) || body.Details == nil || (*body.Details)["name"] != op.ID {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestInitialRestoreFreshPersistedReadyRemainsFencedOnRead(t *testing.T) {
	s, _, token := testServer(t)
	c := &domain.Cluster{Name: "pendingclone", Spec: domain.ClusterSpec{Provider: "fake", Version: "8.0"}}
	provider, err := s.Registry.Get(c.Spec.Provider)
	if err != nil {
		t.Fatal(err)
	}
	provider.ApplyDefaults(&c.Spec)
	if err := s.Store.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	c.Spec.DataSource = &domain.DataSource{Cluster: "source", Backup: "daily"}
	if err := s.Store.UpdateClusterSpec(t.Context(), c.Ref(), c.Spec); err != nil {
		t.Fatal(err)
	}
	c.Status = domain.ClusterStatus{Phase: domain.PhaseReady, ObservedAt: time.Now().UTC(), ObservationEpoch: s.Context.ObservationEpoch, Conditions: []domain.Condition{{Type: "Ready", Status: "True", Reason: "Healthy"}}}
	if err := s.Store.UpdateClusterState(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", "/status"} {
		w := call(s, "GET", "/api/v1/clusters/pendingclone"+suffix, "", token)
		var status api.ClusterStatus
		if suffix == "" {
			var body api.Cluster
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			status = body.Status
		} else if err := json.Unmarshal(w.Body.Bytes(), &status); err != nil {
			t.Fatal(err)
		}
		if w.Code != 200 || status.Phase == api.ClusterStatusPhase(domain.PhaseReady) || !strings.Contains(value(status.Message), "Initial data source") {
			t.Fatalf("read %s exposed pending clone: %d %s", suffix, w.Code, w.Body.String())
		}
	}
	// Unknown provider lookup must not bypass the marker fence either.
	c.Spec.Provider = "missing-provider"
	projected := s.clusterAPI(*c)
	if projected.Status.Phase == api.ClusterStatusPhase(domain.PhaseReady) {
		t.Fatal("missing provider exposed Ready")
	}
	c.Status.Conditions = append(c.Status.Conditions, domain.Condition{Type: "InitialDataSource", Status: "False", Reason: "RestoreAborted", Message: "initial restore cancelled"})
	projected = s.clusterAPI(*c)
	if projected.Status.Phase != api.ClusterStatusPhase(domain.PhaseFailed) || !strings.Contains(value(projected.Status.Message), "cancelled") {
		t.Fatal("terminal restore abort lost", projected.Status)
	}
}
