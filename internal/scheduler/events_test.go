// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package scheduler

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type scheduleRecorder struct{ events []domain.Event }

func (r *scheduleRecorder) Record(_ context.Context, event domain.Event) error {
	r.events = append(r.events, event)
	return nil
}

func TestScheduleFailureUsesRecorderAndSafeMessage(t *testing.T) {
	key, err := secrets.New("0123456789abcdef0123456789abcdef-test-key")
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), key)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Unix(123, 0)
	recorder := &scheduleRecorder{}
	s, err := New(Options{Store: st, Events: recorder, Clock: fixedClock{now}})
	if err != nil {
		t.Fatal(err)
	}
	ref := domain.ClusterRef{Namespace: "default", Name: "missing"}
	if err := s.Fire(t.Context(), ref, "daily"); err == nil {
		t.Fatal("missing cluster did not fail")
	}
	if err := s.catchUp(t.Context(), ref, "daily"); err == nil {
		t.Fatal("missing cluster catch-up did not fail")
	}
	if len(recorder.events) != 2 {
		t.Fatal(recorder.events)
	}
	for _, event := range recorder.events {
		if event.Type != "Warning" || event.Reason != "BackupScheduleFailed" || event.Namespace != ref.Namespace || event.Cluster != ref.Name || event.Actor != "scheduler" || !event.CreatedAt.Equal(now) || event.Message != "Scheduled backup could not be admitted: daily" {
			t.Fatal(event)
		}
	}
}
