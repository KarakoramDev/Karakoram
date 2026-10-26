// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func healthCondition(status domain.ClusterStatus, kind string) domain.Condition {
	for _, c := range status.Conditions {
		if c.Type == kind {
			return c
		}
	}
	return domain.Condition{}
}

func TestHealthNativeObservationUnavailable(t *testing.T) {
	for _, all := range []bool{false, true} {
		t.Run(map[bool]string{false: "replica", true: "all"}[all], func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			s := converge(t, p, rc)
			baseline, err := p.Status(t.Context(), rc)
			if err != nil || baseline.Phase != domain.PhaseReady {
				t.Fatalf("baseline: %+v %v", baseline, err)
			}
			b.statusFailures = map[string]int{}
			failed := map[string]bool{}
			leader := b.leader
			if leader == "" {
				leader = "demop0"
			}
			for _, m := range s.data() {
				if all || m.Name != leader {
					b.statusFailures[m.Name] = 1
					failed[m.Name] = true
					if !all {
						break
					}
				}
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil || status.Phase == domain.PhaseReady || !strings.Contains(status.Message, "restore observations") {
				t.Fatalf("silent observation failure: %+v %v", status, err)
			}
			for _, member := range status.Members {
				if failed[member.Name] && (member.Health || !member.Agent.Healthy || !strings.Contains(member.Agent.Message, member.Name)) {
					t.Fatalf("native failure hidden: %+v", member)
				}
			}
			for _, kind := range []string{"Replication", "EtcdMembers", "Failover"} {
				c := healthCondition(status, kind)
				if c.Status != "Unknown" || c.Reason != "ObservationUnavailable" || c.Message == "" {
					t.Fatalf("%s fabricated healthy native evidence: %+v", kind, c)
				}
			}
			quorum := healthCondition(status, "EtcdQuorum")
			want := "True"
			if all {
				want = "Unknown"
			}
			if quorum.Status != want {
				t.Fatalf("quorum evidence: %+v", quorum)
			}
			recovered, err := p.Status(t.Context(), rc)
			if err != nil || recovered.Phase != domain.PhaseReady || strings.Contains(recovered.Message, "observation unavailable") {
				t.Fatalf("native recovery: %+v %v", recovered, err)
			}
		})
	}
}

func TestHealthUnexpectedPause(t *testing.T) {
	p, rc, b := setup(t, "ha")
	converge(t, p, rc)
	b.upgradePaused = true
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	c := healthCondition(status, "Failover")
	if status.Phase == domain.PhaseReady || c.Status != "False" || c.Reason != "UnexpectedPatroniPause" {
		t.Fatalf("unexpected pause hidden: phase=%s condition=%+v", status.Phase, c)
	}
	for _, condition := range status.Conditions {
		if condition.Reason == "ProcessPaused" {
			t.Fatalf("native Patroni pause misreported as supervisor pause: %+v", condition)
		}
	}
}

func TestHealthLocalEtcdFailure(t *testing.T) {
	p, rc, b := setup(t, "ha")
	converge(t, p, rc)
	st, err := live(t.Context(), rc, "demop1")
	if err != nil {
		t.Fatal(err)
	}
	st.EtcdHealthy = false
	b.statusOverrides = map[string]pgspec.StatusResponse{"demop1": st}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	c := healthCondition(status, "EtcdMembers")
	if status.Phase == domain.PhaseReady || healthCondition(status, "EtcdQuorum").Status != "True" || c.Status != "False" || !strings.Contains(c.Message, "demop1") {
		t.Fatalf("local voter loss hidden: phase=%s condition=%+v", status.Phase, c)
	}
}

func TestHealthArchiveFailure(t *testing.T) {
	p, rc, b := setup(t, "single")
	converge(t, p, rc)
	rc.Cluster.Spec.Backup.Storage = "archive-store"
	st, err := live(t.Context(), rc, "demop0")
	if err != nil {
		t.Fatal(err)
	}
	st.ArchiveFailing = true
	b.statusOverrides = map[string]pgspec.StatusResponse{"demop0": st}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	c := healthCondition(status, "WALArchive")
	if status.Phase == domain.PhaseReady || c.Status != "False" || !strings.Contains(c.Message, "archive-store") {
		t.Fatalf("archive failure hidden: phase=%s condition=%+v", status.Phase, c)
	}
}

func TestHealthPITRErrorPreservesOutage(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	rc.Cluster.Spec.Backup.PITR.Enabled = true
	b.statusOverrides = map[string]pgspec.StatusResponse{}
	for _, m := range s.data() {
		b.statusOverrides[m.Name] = pgspec.StatusResponse{
			Role: "replica", Running: true, Streaming: true,
			HealthCapabilities: &pgspec.HealthCapabilities{LocalEtcd: true},
		}
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatalf("backup observation discarded current health: %v", err)
	}
	if status.Phase == domain.PhaseReady || healthCondition(status, "EtcdQuorum").Status != "False" || healthCondition(status, "PITR").Status != "Unknown" {
		t.Fatalf("outage snapshot lost: %+v", status)
	}
}

func TestHealthTransportLag(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	b.statusOverrides = map[string]pgspec.StatusResponse{}
	for _, m := range s.data() {
		st, err := live(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		st.SystemID = "123"
		st.WALPosition = "1/100"
		if m.Name == "demop0" {
			st.WALPosition = "1/2000100"
		}
		b.statusOverrides[m.Name] = st
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	c := healthCondition(status, "Replication")
	if status.Phase == domain.PhaseReady || c.Status != "False" || !strings.Contains(c.Message, "demop1") {
		t.Fatalf("transport lag hidden by zero replay backlog: phase=%s condition=%+v", status.Phase, c)
	}
}

func TestHealthSlotRetention(t *testing.T) {
	for _, topology := range []string{"single", "ha"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, b := setup(t, topology)
			converge(t, p, rc)
			writer := b.leader
			if writer == "" {
				writer = "demop0"
			}
			baseline, err := live(t.Context(), rc, writer)
			if err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				name     string
				retained int64
				wal      string
				ready    bool
			}{
				{"limit", 1 << 30, "extended", true},
				{"over-limit", (1 << 30) + 1, "extended", false},
				{"lost", 0, "lost", false},
				{"unreserved", 0, "unreserved", false},
				{"recovered", 0, "reserved", true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					native := baseline
					native.SlotStatusKnown = true
					native.ReplicationSlots = []pgspec.ReplicationSlot{{Name: "test_consumer", Type: "logical", RetainedBytes: tc.retained, WALStatus: tc.wal}}
					b.statusOverrides = map[string]pgspec.StatusResponse{writer: native}
					status, err := p.Status(t.Context(), rc)
					if err != nil {
						t.Fatal(err)
					}
					ready, slots := healthCondition(status, "Ready"), healthCondition(status, "ReplicationSlots")
					if tc.ready {
						if ready.Status != "True" || ready.Reason != "MembersReady" || slots.Status != "True" || status.Phase != domain.PhaseReady {
							t.Fatalf("healthy slot blocked readiness: phase=%s Ready=%+v slots=%+v", status.Phase, ready, slots)
						}
					} else if ready.Status != "False" || ready.Reason != "SlotWALRetention" || slots.Status != "False" || slots.Reason != ready.Reason || !strings.Contains(slots.Message, "test_consumer") || status.Phase == domain.PhaseReady {
						t.Fatalf("slot blocker misreported: phase=%s Ready=%+v slots=%+v", status.Phase, ready, slots)
					}
					for _, member := range status.Members {
						if !member.Health {
							t.Fatalf("slot-only blocker changed serving member health: %+v", member)
						}
					}
				})
			}
		})
	}
	if _, err := ParseOptions([]byte(`{"parameters":{"max_slot_wal_keep_size":"4GB"}}`)); err != nil {
		t.Fatal(err)
	}
}

// A native pause has no ownership receipt. Absence of a Karakoram operation
// cannot establish that an operator did not intentionally disable elections.
func TestHealthPauseResumeOwnership(t *testing.T) {
	p, rc, b := setup(t, "ha")
	converge(t, p, rc)
	b.upgradePaused = true
	for range 3 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	if !b.upgradePaused {
		t.Fatal("ordinary reconciliation resumed an intentional native pause")
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil || healthCondition(status, "Failover").Reason != "UnexpectedPatroniPause" {
		t.Fatalf("unattributed pause must remain visible for operator recovery: %+v %v", status, err)
	}
}

func TestHealthReplicaDistanceUnknown(t *testing.T) {
	primary := pgspec.StatusResponse{Role: "primary", Running: true, SystemID: "123", Timeline: 2, WALPosition: "2/100"}
	replica := pgspec.StatusResponse{Role: "replica", Running: true, SystemID: "123", Timeline: 2, WALPosition: "1/100"}
	if distance, known := replicaDistance(primary, replica); !known || distance != 1<<32 {
		t.Fatalf("wrong transport distance %d %t", distance, known)
	}
	for _, change := range []func(*pgspec.StatusResponse){func(s *pgspec.StatusResponse) { s.SystemID = "" }, func(s *pgspec.StatusResponse) { s.SystemID = "other" }, func(s *pgspec.StatusResponse) { s.Timeline = 1 }, func(s *pgspec.StatusResponse) { s.WALPosition = "" }, func(s *pgspec.StatusResponse) { s.WALPosition = "invalid" }} {
		st := replica
		change(&st)
		if _, known := replicaDistance(primary, st); known {
			t.Fatal("missing or incompatible sample reported zero authoritative lag")
		}
	}
}

func TestHealthEtcdReplacementCheckpoint(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	s := converge(t, p, rc)
	for _, stage := range []string{"remove-etcd", "promote"} {
		s.Replacement = &replacementState{Operation: "replace", Old: "demop1", New: "demop3", Stage: stage}
		if err := save(rc.Cluster, s); err != nil {
			t.Fatal(err)
		}
		status, err := p.Status(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		c := healthCondition(status, "EtcdReconfiguration")
		if c.Status != "False" || !strings.Contains(c.Message, stage) || !strings.Contains(c.Message, "quorum") || status.Phase == domain.PhaseReady {
			t.Fatalf("replacement vote boundary is opaque: %+v", c)
		}
	}
}
