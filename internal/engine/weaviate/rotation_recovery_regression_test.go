// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

type recoveryClock struct{ at *time.Time }

func (c recoveryClock) Now() time.Time { return *c.at }

type rotationFailureBackend struct {
	*fake.Backend
	target string
	failed bool
}

func (b *rotationFailureBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	if name == b.target && !b.failed {
		b.failed = true
		return infra.Op{}, errors.New("injected member start failure")
	}
	return b.Backend.Deploy(ctx, name, d)
}
func TestInternalRotationFailureRestartRecovery(t *testing.T) {
	ctx := context.Background()
	f := coreSetup(t, 3)
	s := coreConverge(t, f)
	old, err := secret(ctx, f.rc, secretClusterPassword)
	if err != nil {
		t.Fatal(err)
	}
	b := &rotationFailureBackend{Backend: f.backend, target: s.Members[1].Name}
	f.rc.Backend = b
	clock := time.Now()
	f.rc.Clock = recoveryClock{&clock}
	op := &domain.Operation{ID: "rotation-failure", Kind: domain.OperationRotateSystemUser, Payload: json.RawMessage(`{"user":"karakoram-cluster"}`)}
	stages := map[string]bool{}
	sawFailure, done := false, false
	for range 240 {
		// Reconstruct the provider at every stop/config/start and pending-op boundary.
		f.p = New(Options{Store: f.rc.Store})
		r, e := f.p.RunOperation(ctx, f.rc, op)
		s, _ = load(f.rc.Cluster)
		if s.Rotation != nil {
			stages[s.Rotation.Stage] = true
		}
		next, _ := secret(ctx, f.rc, secretClusterPassword)
		if e != nil {
			if !strings.Contains(e.Error(), "injected member start failure") {
				t.Fatal(e)
			}
			sawFailure = true
			if string(next) == string(old) {
				t.Fatal("failed start rolled back committed credential")
			}
			// Every member, including already started survivors, must be recoverable
			// using the single committed credential after the controller disappears.
			for _, m := range s.Members {
				cfg, err := f.p.MemberConfig(ctx, f.rc, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				var spec wvspec.MemberSpec
				if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
					t.Fatal(err)
				}
				if spec.ClusterPassword != string(next) {
					t.Fatal("mixed internal credentials after failed restart")
				}
			}
			if s.Pending == nil || s.Pending.Action != "deploy" || s.Pending.Member != b.target {
				t.Fatal("failed start lost durable retry owner")
			}
		}
		visible := string(r.Checkpoint) + r.Message + string(f.rc.Cluster.Internal)
		if e != nil {
			visible += e.Error()
		}
		if strings.Contains(visible, string(old)) || len(next) > 0 && strings.Contains(visible, string(next)) {
			t.Fatal("credential leaked into operation output")
		}
		if len(r.Checkpoint) > 0 {
			op.Checkpoint = r.Checkpoint
		}
		clock = clock.Add(time.Minute)
		if r.Done {
			done = true
			break
		}
	}
	for _, stage := range []string{"stop-all", "cluster-switch", "cluster-config", "start-all", "cluster-ack", "finish"} {
		if !stages[stage] {
			t.Fatalf("missing restart boundary %s: state=%s stages=%v", stage, f.rc.Cluster.Internal, stages)
		}
	}
	if !done || !sawFailure {
		t.Fatal("rotation retry did not converge after injected failure")
	}
	s, _ = load(f.rc.Cluster)
	if s.Rotation != nil || s.Pending != nil || s.LastRotated[ClusterUser].IsZero() {
		t.Fatal("rotation owner not completed")
	}
	coreConverge(t, f)
}
