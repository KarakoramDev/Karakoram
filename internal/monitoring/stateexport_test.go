// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package monitoring

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

func stateFixture() StateSnapshot {
	at := func(seconds int64) *time.Time { v := time.Unix(seconds, 250000000).UTC(); return &v }
	return StateSnapshot{
		Clusters: []ClusterState{{
			Cluster: domain.Cluster{Name: "prod", Spec: domain.ClusterSpec{Provider: "postgres", Version: "desired", Backup: domain.BackupConfig{Schedules: []domain.BackupSchedule{{Name: "daily", Enabled: true}, {Name: "never", Enabled: true}, {Name: "disabled"}}, Drill: &domain.BackupDrillPolicy{Enabled: true}}}, Status: domain.ClusterStatus{Phase: domain.PhaseDegraded, Version: "observed", Conditions: []domain.Condition{{Type: "Ready", Status: "False", Reason: "Waiting"}, {Type: "Ready", Status: "False", Reason: "Waiting"}, {Type: "Deployment/m0", Status: "False", Reason: "DeploymentRetriesExhausted"}}, Agents: &domain.AgentRolloutStatus{Outdated: []string{"m0"}, Unknown: []string{"m1"}}, Members: []domain.MemberStatus{{Name: "m0"}, {Name: "m1"}}, Placement: domain.Placement{Groups: map[string][]string{"best-effort:host": {"m0", "m1"}}}}},
			Backups: []domain.Backup{
				{Phase: domain.PhaseSucceeded, Schedule: "daily", CreatedAt: *at(900), CompletedAt: at(200), LastWriteTime: at(100)},
				{Phase: domain.PhaseSucceeded, Schedule: "daily", CreatedAt: *at(800), CompletedAt: at(300)},
				{Phase: domain.PhaseSucceeded, CompletedAt: at(400)},
				{Phase: domain.PhaseFailed, CompletedAt: at(500)},
				{Phase: domain.PhaseRunning, CompletedAt: at(600)},
				{Phase: domain.PhaseSucceeded},
			},
			Drills: []domain.BackupDrill{
				{Phase: "Succeeded", CreatedAt: *at(1000), CompletedAt: at(200), RestoreDurationSeconds: 90},
				{Phase: "Succeeded", CreatedAt: *at(100), CompletedAt: at(300), RestoreDurationSeconds: 12},
				{Phase: "Failed", CompletedAt: at(400), RestoreDurationSeconds: 999},
				{Phase: "Restoring", CompletedAt: at(500), RestoreDurationSeconds: 888},
				{Phase: "Succeeded"},
			},
		}},
		Certificates:              []CertificateExpiry{{Namespace: "default", Cluster: "prod", Certificate: "database", Expiry: *at(700)}, {Certificate: "server", Expiry: *at(800)}},
		Installation:              true,
		ControlPlaneBackupEnabled: true,
		ControlPlaneBackups:       []domain.ControlPlaneBackup{{Phase: domain.PhaseSucceeded, CompletedAt: at(100)}, {Phase: domain.PhaseSucceeded, CompletedAt: at(200)}, {Phase: domain.PhaseFailed, CompletedAt: at(300)}, {Phase: domain.PhaseRunning, CompletedAt: at(900)}, {Phase: domain.PhaseSucceeded}},
		NotificationFailures:      map[string]int64{"chat": 3, "email": 0},
	}
}

func parseState(t *testing.T, snapshot StateSnapshot) map[string]*dto.MetricFamily {
	t.Helper()
	var out bytes.Buffer
	if err := WriteStatePrometheus(&out, snapshot); err != nil {
		t.Fatal(err)
	}
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(&out)
	if err != nil {
		t.Fatal(err)
	}
	return families
}

func stateValue(t *testing.T, families map[string]*dto.MetricFamily, name string, labels map[string]string) float64 {
	t.Helper()
	for _, metric := range families[name].GetMetric() {
		match := true
		for key, value := range labels {
			found := false
			for _, label := range metric.Label {
				if label.GetName() == key && label.GetValue() == value {
					found = true
				}
			}
			if !found {
				match = false
			}
		}
		if match {
			if families[name].GetType() == dto.MetricType_COUNTER {
				return metric.GetCounter().GetValue()
			}
			return metric.GetGauge().GetValue()
		}
	}
	t.Fatalf("missing %s %v", name, labels)
	return 0
}

func TestStateExpositionCompleteVocabulary(t *testing.T) {
	families := parseState(t, stateFixture())
	want := strings.Fields(`karakoram_cluster_info karakoram_cluster_ready karakoram_cluster_degraded karakoram_cluster_condition karakoram_cluster_members_co_located karakoram_member_agent_outdated karakoram_backup_last_success_timestamp_seconds karakoram_backup_last_failure_timestamp_seconds karakoram_certificate_expiry_timestamp_seconds karakoram_backup_drill_enabled karakoram_backup_drill_last_failure_timestamp_seconds karakoram_backup_drill_last_success_timestamp_seconds karakoram_backup_drill_last_restore_duration_seconds karakoram_deployment_retries_exhausted karakoram_control_plane_backup_enabled karakoram_control_plane_backup_last_success_timestamp_seconds karakoram_control_plane_backup_last_failure_timestamp_seconds karakoram_notification_deliveries_failed_total`)
	got := make([]string, 0, len(families))
	for name := range families {
		got = append(got, name)
	}
	sort.Strings(want)
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("families=%v want=%v", got, want)
	}
	if len(StateMetrics) != len(want) {
		t.Fatal("vocabulary size", len(StateMetrics))
	}
	for _, descriptor := range StateMetrics {
		family := families[descriptor.Name]
		if family == nil || family.GetHelp() != descriptor.Help || strings.ToLower(family.GetType().String()) != descriptor.Type {
			t.Fatalf("descriptor mismatch: %+v", descriptor)
		}
		for _, metric := range family.Metric {
			labels := make([]string, 0, len(metric.Label))
			for _, label := range metric.Label {
				labels = append(labels, label.GetName())
			}
			wantLabels := append([]string(nil), descriptor.Labels...)
			sort.Strings(labels)
			sort.Strings(wantLabels)
			if !reflect.DeepEqual(labels, wantLabels) && len(labels)+len(wantLabels) != 0 {
				t.Fatalf("%s labels=%v want=%v", descriptor.Name, labels, wantLabels)
			}
		}
	}
}

func TestStateExpositionTimestampsAndAggregation(t *testing.T) {
	families := parseState(t, stateFixture())
	for _, tc := range []struct {
		name   string
		labels map[string]string
		want   float64
	}{
		{"karakoram_cluster_ready", nil, 0}, {"karakoram_cluster_degraded", nil, 1},
		{"karakoram_cluster_info", map[string]string{"version": "observed", "namespace": "default"}, 1},
		{"karakoram_cluster_members_co_located", nil, 1},
		{"karakoram_deployment_retries_exhausted", nil, 1},
		{"karakoram_member_agent_outdated", map[string]string{"member": "m0"}, 1},
		{"karakoram_member_agent_outdated", map[string]string{"member": "m1"}, 0},
		{"karakoram_backup_last_success_timestamp_seconds", map[string]string{"schedule": "daily"}, 300.25},
		{"karakoram_backup_last_success_timestamp_seconds", map[string]string{"schedule": ""}, 400.25},
		{"karakoram_backup_last_success_timestamp_seconds", map[string]string{"schedule": "never"}, 0},
		{"karakoram_backup_last_failure_timestamp_seconds", nil, 500.25},
		{"karakoram_backup_drill_enabled", nil, 1},
		{"karakoram_backup_drill_last_success_timestamp_seconds", nil, 300.25},
		{"karakoram_backup_drill_last_failure_timestamp_seconds", nil, 400.25},
		{"karakoram_backup_drill_last_restore_duration_seconds", nil, 12},
		{"karakoram_certificate_expiry_timestamp_seconds", map[string]string{"certificate": "database"}, 700.25},
		{"karakoram_control_plane_backup_enabled", nil, 1},
		{"karakoram_control_plane_backup_last_success_timestamp_seconds", nil, 200.25},
		{"karakoram_control_plane_backup_last_failure_timestamp_seconds", nil, 300.25},
		{"karakoram_notification_deliveries_failed_total", map[string]string{"channel": "chat"}, 3},
	} {
		if got := stateValue(t, families, tc.name, tc.labels); got != tc.want {
			t.Errorf("%s %v = %v want %v", tc.name, tc.labels, got, tc.want)
		}
	}
	if len(families["karakoram_cluster_condition"].Metric) != 2 {
		t.Fatal("duplicate condition series")
	}
	if len(families["karakoram_backup_last_success_timestamp_seconds"].Metric) != 3 {
		t.Fatal("disabled schedule emitted")
	}
}

func TestStateExpositionZeroesEscapingAndDeterminism(t *testing.T) {
	snapshot := StateSnapshot{Clusters: []ClusterState{{Cluster: domain.Cluster{Name: "prod", Status: domain.ClusterStatus{Phase: domain.PhaseReady, Members: []domain.MemberStatus{{Name: "m0"}}, Placement: domain.Placement{Groups: map[string][]string{"unknown": {"m0", "m1"}}}, Conditions: []domain.Condition{{Type: "custom\tcondition", Status: "Unknown", Reason: "quote\"slash\\newline\n"}}}}}}, Installation: true}
	families := parseState(t, snapshot)
	for _, name := range []string{"karakoram_backup_last_failure_timestamp_seconds", "karakoram_backup_drill_last_success_timestamp_seconds", "karakoram_backup_drill_last_failure_timestamp_seconds", "karakoram_backup_drill_last_restore_duration_seconds", "karakoram_cluster_members_co_located", "karakoram_control_plane_backup_enabled", "karakoram_control_plane_backup_last_success_timestamp_seconds", "karakoram_control_plane_backup_last_failure_timestamp_seconds"} {
		if stateValue(t, families, name, nil) != 0 {
			t.Error(name)
		}
	}
	if stateValue(t, families, "karakoram_cluster_ready", nil) != 1 {
		t.Fatal("ready")
	}
	if stateValue(t, families, "karakoram_cluster_condition", map[string]string{"type": "custom\tcondition", "reason": "quote\"slash\\newline\n"}) != 1 {
		t.Fatal("escaping")
	}
	var first, second bytes.Buffer
	fixture := stateFixture()
	if err := WriteStatePrometheus(&first, fixture); err != nil {
		t.Fatal(err)
	}
	for left, right := 0, len(fixture.Clusters[0].Backups)-1; left < right; left, right = left+1, right-1 {
		fixture.Clusters[0].Backups[left], fixture.Clusters[0].Backups[right] = fixture.Clusters[0].Backups[right], fixture.Clusters[0].Backups[left]
	}
	if err := WriteStatePrometheus(&second, fixture); err != nil {
		t.Fatal(err)
	}
	if first.String() != second.String() {
		t.Fatal("unstable exposition")
	}
}

type stateErrorWriter struct{ remaining int }

func (w *stateErrorWriter) Write(p []byte) (int, error) {
	if w.remaining == 0 {
		return 0, io.ErrClosedPipe
	}
	w.remaining--
	return len(p), nil
}
func TestStateExpositionPropagatesWriterErrors(t *testing.T) {
	for _, remaining := range []int{0, 1, 5} {
		if err := WriteStatePrometheus(&stateErrorWriter{remaining: remaining}, stateFixture()); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("remaining=%d err=%v", remaining, err)
		}
	}
}
