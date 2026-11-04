// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"path/filepath"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestHealthTransitionSemantics(t *testing.T) {
	key, err := secrets.New("0123456789abcdef0123456789abcdef-test-key")
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), key)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	m := &Manager{opts: Options{Store: st}}
	for _, tc := range []struct {
		before, after domain.Phase
		reason, kind  string
	}{
		{domain.PhaseReady, domain.PhaseDegraded, "ClusterDegraded", "Warning"},
		{domain.PhaseDegraded, domain.PhaseError, "", ""},
		{domain.PhaseError, domain.PhaseReady, "ClusterRecovered", "Normal"},
		{domain.PhaseReady, domain.PhaseReady, "", ""},
		{domain.PhaseCreating, domain.PhaseError, "ClusterDegraded", "Warning"},
	} {
		before, err := st.ListEvents(t.Context(), store.EventFilter{})
		if err != nil {
			t.Fatal(err)
		}
		var cursor int64
		if len(before) > 0 {
			cursor = before[len(before)-1].ID
		}
		c := &domain.Cluster{Namespace: "default", Name: "db", Status: domain.ClusterStatus{Phase: tc.after}}
		if err := m.healthTransition(t.Context(), c, tc.before); err != nil {
			t.Fatal(err)
		}
		got, err := st.ListEvents(t.Context(), store.EventFilter{SinceID: cursor})
		if err != nil {
			t.Fatal(err)
		}
		if tc.reason == "" {
			if len(got) != 0 {
				t.Fatal(got)
			}
			continue
		}
		if len(got) != 1 || got[0].Reason != tc.reason || got[0].Type != tc.kind || got[0].Cluster != "db" || got[0].Namespace != "default" {
			t.Fatal(got)
		}
	}
}
