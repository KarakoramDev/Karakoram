// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestClusterObservationCannotOverwriteLifecycleCheckpoint(t *testing.T) {
	for _, change := range []string{"none", "internal", "status", "generation", "deletion", "incarnation", "operation"} {
		t.Run(change, func(t *testing.T) {
			st, _ := testStore(t)
			ctx := t.Context()
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Internal: json.RawMessage(`{"checkpoint":1}`),
				Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1}}
			if err := st.CreateCluster(ctx, c); err != nil {
				t.Fatal(err)
			}
			before, previousStatus, err := st.GetClusterObservation(ctx, c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			// Preserve updated deliberately: wall time can repeat, so the full
			// status/Internal checkpoint must fence independently of timestamps.
			switch change {
			case "internal":
				_, err = st.writer.ExecContext(ctx, `UPDATE clusters SET internal=? WHERE name='demo'`, []byte(`{"checkpoint":2}`))
			case "status":
				next := before.Status
				next.Message = "new lifecycle warning"
				var data []byte
				data, err = json.Marshal(next)
				if err == nil {
					_, err = st.writer.ExecContext(ctx, `UPDATE clusters SET status=? WHERE name='demo'`, data)
				}
			case "generation":
				_, err = st.writer.ExecContext(ctx, `UPDATE clusters SET generation=generation+1 WHERE name='demo'`)
			case "deletion":
				_, err = st.writer.ExecContext(ctx, `UPDATE clusters SET deleting=1 WHERE name='demo'`)
			case "incarnation":
				_, err = st.writer.ExecContext(ctx, `UPDATE clusters SET created=created+1 WHERE name='demo'`)
			case "operation":
				err = st.EnqueueOperation(ctx, &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning})
			}
			if err != nil {
				t.Fatal(err)
			}
			observed := domain.ClusterStatus{Phase: domain.PhaseDegraded, ObservedAt: time.Now(), ObservationEpoch: "current-process", ObservedGeneration: 99}
			published, err := st.UpdateClusterObservation(ctx, before, previousStatus, observed)
			if err != nil || published != (change == "none") {
				t.Fatalf("publication crossed %s fence: published=%v error=%v", change, published, err)
			}
			got, err := st.GetCluster(ctx, c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			if change == "none" {
				if got.Status.Phase != domain.PhaseDegraded || got.Status.ObservationEpoch != "current-process" || got.Status.ObservedGeneration != 1 || string(got.Internal) != string(before.Internal) {
					t.Fatal("status-only publication changed provider state or generation", got)
				}
			} else if got.Status.ObservationEpoch == "current-process" {
				t.Fatal("stale observation reached authoritative state", got)
			}
		})
	}
}

func TestClusterObservationFencesRawStatusBeforeReadTimeProjection(t *testing.T) {
	st, _ := testStore(t)
	ctx := t.Context()
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
	if err := st.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	// Field ordering, whitespace, unknown fields and diagnostics are not a
	// lifecycle change. Re-encoding decoded public-safe status is not a token.
	raw := []byte(`{ "unknownFutureField": 42, "message": "password=synthetic-private-value", "phase": "Ready", "members": [] }`)
	if _, err := st.writer.ExecContext(ctx, `UPDATE clusters SET status=? WHERE name='demo'`, raw); err != nil {
		t.Fatal(err)
	}
	before, checkpoint, err := st.GetClusterObservation(ctx, c.Ref())
	if err != nil || string(checkpoint) != string(raw) {
		t.Fatal("raw equality checkpoint was projected", err)
	}
	decoded, err := json.Marshal(before.Status)
	if err != nil || string(decoded) == string(raw) {
		t.Fatal("test failed to distinguish stored bytes from decoded status", err)
	}
	published, err := st.UpdateClusterObservation(ctx, before, checkpoint, domain.ClusterStatus{Phase: domain.PhaseReady})
	if err != nil || !published {
		t.Fatal("unchanged raw status prevented fresh publication", published, err)
	}
}
