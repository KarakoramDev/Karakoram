// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestHAConfigOnlyReloadDoesNotRestartAdoptedPrimary(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	primary := s.data()[0]
	st, err := live(t.Context(), rc, primary.Name)
	if err != nil {
		t.Fatal(err)
	}
	// The agent reports Patroni's false/absent pending_restart rather than the
	// stale native recovery_target* flags left after postmaster adoption.
	st.PendingRestart = false
	b.statusOverrides = map[string]pgspec.StatusResponse{primary.Name: st}
	b.leader = primary.Name
	b.actions, b.reloads = nil, nil
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"parameters":{"work_mem":"8MB"}}`)
	rc.Cluster.Generation++
	s = converge(t, p, rc)
	if !slices.Contains(b.reloads, primary.Name) || s.find(primary.Name).Stage != "joined" {
		t.Fatal("config-only reload did not pass through readiness and converge")
	}
	if b.leader != primary.Name {
		t.Fatal("config-only reload switched the adopted primary", b.leader)
	}
	for _, action := range b.actions {
		if strings.Contains(action, "restart") || strings.HasPrefix(action, "deploy:") {
			t.Fatal("config-only reload disrupted a database", b.actions)
		}
	}
}

func TestHAGenuinePendingRestartSwitchesThenRestartsFormerPrimary(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	m := s.data()[0]
	st, err := live(t.Context(), rc, m.Name)
	if err != nil {
		t.Fatal(err)
	}
	// For example, Patroni reports a shared_buffers change requiring restart.
	st.PendingRestart = true
	b.statusOverrides = map[string]pgspec.StatusResponse{m.Name: st}
	b.actions = nil
	ok, err := ready(t.Context(), rc, m)
	if err != nil || ok {
		t.Fatalf("pending primary change reported ready: ready=%t err=%v", ok, err)
	}
	if b.leader == "" || b.leader == m.Name {
		t.Fatal("genuine restart did not move the writer", b.leader)
	}
	if len(b.actions) != 0 {
		t.Fatal("primary restarted before switchover", b.actions)
	}
	st.Role, st.Streaming, st.Leader = "replica", true, b.leader
	b.statusOverrides[m.Name] = st
	ok, err = ready(t.Context(), rc, m)
	if err != nil || ok {
		t.Fatalf("pending former-primary change reported ready: ready=%t err=%v", ok, err)
	}
	if !slices.Contains(b.actions, "/v1/postgres/restart:"+m.Name) {
		t.Fatal("former primary was not restarted after switchover", b.actions)
	}
	st.PendingRestart = false
	b.statusOverrides[m.Name] = st
	ok, err = ready(t.Context(), rc, m)
	if err != nil || !ok {
		t.Fatalf("restarted replica did not become ready: ready=%t err=%v", ok, err)
	}
}
