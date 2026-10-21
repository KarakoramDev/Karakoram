// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestPostgresRestoreAcknowledgementUsesAdmissionBindings(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	c := domain.Cluster{Name: "source", Spec: domain.ClusterSpec{Provider: "postgres", Version: "17.11.1"}}
	if err := s.CreateCluster(ctx, &c); err != nil {
		t.Fatal(err)
	}
	// The request inspected an unbound source. Binding admission does not alter
	// this snapshot, so only the restore writer transaction can catch it.
	inspected, err := s.GetCluster(ctx, c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	request := WithClusterSnapshot(ctx, inspected)
	bindings, err := s.ListPostgresBindings(request, c.Ref())
	if err != nil || len(bindings) != 0 {
		t.Fatalf("initial bindings=%+v err=%v", bindings, err)
	}
	binding := domain.PostgresBinding{ID: "binding", SourceCluster: c.Name, ConsumerKind: "keycloak", ConsumerName: "identity", State: "pending"}
	if _, err := s.EnsurePostgresBinding(ctx, &binding); err != nil {
		t.Fatal(err)
	}
	r := domain.Restore{Name: "recover", Cluster: c.Name}
	op := domain.Operation{Cluster: c.Name, Kind: domain.OperationRestore}
	var previewBound *BoundConsumersError
	if err := s.CheckRestore(request, &r); !errors.As(err, &previewBound) || len(previewBound.Consumers) != 1 {
		t.Fatalf("preview omitted consumer acknowledgement: %v", err)
	}
	if err := s.CheckRestore(WithRestoreAcknowledgement(request), &r); err != nil {
		t.Fatalf("acknowledged preview rejected: %v", err)
	}
	err = s.EnqueueRestore(request, &r, &op)
	var bound *BoundConsumersError
	if !errors.Is(err, ErrConflict) || !errors.As(err, &bound) || len(bound.Consumers) != 1 || bound.Consumers[0].ConsumerName != "identity" {
		t.Fatalf("missing typed consumer conflict: %v", err)
	}
	if _, err := s.GetRestore(ctx, c.Ref(), r.Name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected restore persisted: %v", err)
	}
	operations, err := s.ListOperations(ctx, c.Ref())
	if err != nil || len(operations) != 0 {
		t.Fatalf("rejected restore enqueued work: %+v %v", operations, err)
	}
	if err := s.EnqueueRestore(WithRestoreBinding(request, "another-binding"), &r, &op); !errors.As(err, &bound) {
		t.Fatalf("unrelated consumer bypassed acknowledgement: %v", err)
	}
	if err := s.EnqueueRestore(WithRestoreAcknowledgement(request), &r, &op); err != nil {
		t.Fatalf("acknowledged restore rejected: %v", err)
	}
}

func TestPostgresRestoreBindingAcknowledgesOnlyOwnConsumer(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	c := domain.Cluster{Name: "source", Spec: domain.ClusterSpec{Provider: "postgres", Version: "17.11.1"}}
	if err := s.CreateCluster(ctx, &c); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"own", "other"} {
		b := domain.PostgresBinding{ID: name, SourceCluster: c.Name, ConsumerKind: "keycloak", ConsumerName: name, State: "ready"}
		if _, err := s.EnsurePostgresBinding(ctx, &b); err != nil {
			t.Fatal(err)
		}
	}
	request := WithRestoreBinding(ctx, "own")
	r := domain.Restore{Name: "recover", Cluster: c.Name}
	op := domain.Operation{Cluster: c.Name, Kind: domain.OperationRestore}
	err := s.EnqueueRestore(request, &r, &op)
	var bound *BoundConsumersError
	if !errors.As(err, &bound) || len(bound.Consumers) != 1 || bound.Consumers[0].ID != "other" {
		t.Fatalf("requesting binding hid another consumer: %v", err)
	}
	if err := s.DeletePostgresBinding(ctx, "other"); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueRestore(request, &r, &op); err != nil {
		t.Fatalf("dedicated consumer restore rejected: %v", err)
	}
}

func TestPostgresRestoreBlocksNewBindingsUntilTerminal(t *testing.T) {
	for _, terminal := range []domain.Phase{domain.PhaseSucceeded, domain.PhaseFailed, domain.PhaseCancelled} {
		t.Run(string(terminal), func(t *testing.T) {
			s, _ := testStore(t)
			ctx := t.Context()
			c := domain.Cluster{Name: "source", Spec: domain.ClusterSpec{Provider: "postgres", Version: "17.11.1"}}
			if err := s.CreateCluster(ctx, &c); err != nil {
				t.Fatal(err)
			}
			existing := domain.PostgresBinding{ID: "existing", SourceCluster: c.Name, ConsumerKind: "keycloak", ConsumerName: "existing", Databases: []string{"existing"}, State: "ready"}
			if _, err := s.EnsurePostgresBinding(ctx, &existing); err != nil {
				t.Fatal(err)
			}
			r := domain.Restore{Name: "recover", Cluster: c.Name}
			op := domain.Operation{Cluster: c.Name, Kind: domain.OperationRestore}
			if err := s.EnqueueRestore(WithRestoreAcknowledgement(ctx), &r, &op); err != nil {
				t.Fatal(err)
			}
			for _, phase := range []domain.Phase{domain.PhasePending, domain.PhaseRunning} {
				r.Phase = phase
				if err := s.UpdateRestore(ctx, &r); err != nil {
					t.Fatal(err)
				}
				fresh := domain.PostgresBinding{ID: "fresh", SourceCluster: c.Name, ConsumerKind: "keycloak", ConsumerName: "fresh", State: "pending"}
				_, err := s.EnsurePostgresBinding(ctx, &fresh)
				var active *RestoreInProgressError
				if !errors.Is(err, ErrConflict) || !errors.As(err, &active) || !strings.Contains(err.Error(), r.Name) {
					t.Fatalf("phase %s admitted new binding: %v", phase, err)
				}
				if _, err := s.GetPostgresBinding(ctx, fresh.ID); !errors.Is(err, ErrNotFound) {
					t.Fatalf("blocked binding persisted: %v", err)
				}
				// Both an idempotent replay and additive Ensure remain allowed.
				if _, err := s.EnsurePostgresBinding(ctx, &existing); err != nil {
					t.Fatalf("existing binding rejected: %v", err)
				}
				existing.Databases = append(existing.Databases, "extra_"+string(phase))
				if _, err := s.EnsurePostgresBinding(ctx, &existing); err != nil {
					t.Fatalf("existing additive ensure rejected: %v", err)
				}
			}
			r.Phase = terminal
			if err := s.UpdateRestore(ctx, &r); err != nil {
				t.Fatal(err)
			}
			fresh := domain.PostgresBinding{ID: "fresh", SourceCluster: c.Name, ConsumerKind: "keycloak", ConsumerName: "fresh", State: "pending"}
			if _, err := s.EnsurePostgresBinding(ctx, &fresh); err != nil {
				t.Fatalf("terminal restore still blocks binding: %v", err)
			}
		})
	}
}
