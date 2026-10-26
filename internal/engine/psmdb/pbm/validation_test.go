// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestPITRTargetChecksContinuousCoverage(t *testing.T) {
	at := func(second int64) time.Time { return time.Unix(second, 0).UTC() }
	windows := []domain.TimeWindow{{Start: at(100), End: at(200)}, {Start: at(300), End: at(400)}}
	for _, second := range []int64{100, 150, 200, 300, 400} {
		if err := ValidatePITRTarget(windows, at(second), at(500)); err != nil {
			t.Fatal(second, err)
		}
	}
	for _, second := range []int64{99, 201, 250, 401, 600} {
		var field *domain.FieldError
		if err := ValidatePITRTarget(windows, at(second), at(500)); !errors.As(err, &field) || field.Field != "source.pitrTime" || field.Reason != domain.ReasonOutOfRange {
			t.Fatal(second, err)
		}
	}
	if ValidatePITRTarget(nil, at(150), at(500)) == nil {
		t.Fatal("uncovered target accepted")
	}
	if ValidatePITRTarget(windows, at(400), at(350)) == nil {
		t.Fatal("future target accepted despite coverage")
	}
}

func TestInvalidPITRRestoreFailsTerminally(t *testing.T) {
	for _, mode := range []string{"future", "no-base", "lost-coverage"} {
		t.Run(mode, func(t *testing.T) {
			env, st, fake, _ := setup(t)
			target := time.Now().Add(time.Hour)
			if mode != "future" {
				target = time.Unix(1704453100, 0).UTC()
			}
			r := &domain.Restore{Name: "recovery", Cluster: env.RC.Cluster.Name, Source: domain.RestoreSource{PITRTime: &target}}
			if err := st.CreateRestore(context.Background(), r); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{ID: "recovery", Payload: json.RawMessage(`{"restore":"recovery"}`)}
			if mode == "lost-coverage" {
				fake.mu.Lock()
				fake.pitrDeleted = true
				fake.mu.Unlock()
				cp := checkpoint{Step: "disable-pitr", Member: "m0", TargetTime: &target, Snapshot: "base"}
				op.Checkpoint, _ = json.Marshal(cp)
			}
			terminal := false
			for range 15 {
				res, err := RunRestore(context.Background(), env, op)
				op.Checkpoint = res.Checkpoint
				if res.Done {
					if err == nil {
						t.Fatal("invalid recovery succeeded")
					}
					terminal = true
					break
				}
				if err != nil {
					t.Fatal("permanent error retried", res, err)
				}
			}
			if !terminal {
				t.Fatal("invalid recovery did not terminate")
			}
			stored, err := st.GetRestore(context.Background(), env.RC.Cluster.Ref(), r.Name)
			if err != nil || stored.Phase != domain.PhaseFailed || stored.Error == "" || stored.CompletedAt == nil {
				t.Fatal(stored, err)
			}
			if fake.startCount() != 0 {
				t.Fatal("invalid PITR started restore")
			}
			res, err := RunRestore(context.Background(), env, op)
			if !res.Done || err == nil {
				t.Fatal("terminal failure lost on resume", res, err)
			}
		})
	}
}
