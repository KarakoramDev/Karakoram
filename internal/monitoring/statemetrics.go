// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package monitoring

// StateMetricDescriptor describes a control-plane series, including every label.
// Type is either gauge or counter.
type StateMetricDescriptor struct {
	Name   string
	Help   string
	Type   string
	Labels []string
}

// StateMetrics is the canonical control-plane exposition vocabulary. Cluster
// series are independent of member monitoring and require monitoring:read;
// installation series require settings:read. Timestamp gauges use Unix seconds,
// with zero meaning no recorded result. A nonempty backup schedule identifies an
// enabled schedule; the empty schedule identifies the aggregate/manual result.
var StateMetrics = []StateMetricDescriptor{
	{"karakoram_cluster_info", "Cluster identity and current phase; value is one.", "gauge", []string{"namespace", "cluster", "provider", "version", "phase"}},
	{"karakoram_cluster_ready", "Whether the cluster is ready.", "gauge", []string{"namespace", "cluster"}},
	{"karakoram_cluster_degraded", "Whether the cluster is degraded.", "gauge", []string{"namespace", "cluster"}},
	{"karakoram_cluster_condition", "Current condition identity and status; value is one.", "gauge", []string{"namespace", "cluster", "type", "status", "reason"}},
	{"karakoram_cluster_members_co_located", "Whether members share an observed placement group; not an anti-affinity guarantee.", "gauge", []string{"namespace", "cluster"}},
	{"karakoram_member_agent_outdated", "Whether the member agent differs from the target build.", "gauge", []string{"namespace", "cluster", "member"}},
	{"karakoram_backup_last_success_timestamp_seconds", "Unix timestamp of the last successful backup for the schedule.", "gauge", []string{"namespace", "cluster", "schedule"}},
	{"karakoram_backup_last_failure_timestamp_seconds", "Unix timestamp of the last failed backup.", "gauge", []string{"namespace", "cluster"}},
	{"karakoram_certificate_expiry_timestamp_seconds", "Unix timestamp of certificate expiry.", "gauge", []string{"namespace", "cluster", "certificate"}},
	{"karakoram_backup_drill_enabled", "Whether scheduled backup drills are enabled.", "gauge", []string{"namespace", "cluster"}},
	{"karakoram_backup_drill_last_failure_timestamp_seconds", "Unix timestamp of the last failed backup drill.", "gauge", []string{"namespace", "cluster"}},
	{"karakoram_backup_drill_last_success_timestamp_seconds", "Unix timestamp of the last successful backup drill.", "gauge", []string{"namespace", "cluster"}},
	{"karakoram_backup_drill_last_restore_duration_seconds", "Restore duration of the last successful backup drill in seconds.", "gauge", []string{"namespace", "cluster"}},
	{"karakoram_deployment_retries_exhausted", "Whether deployment retries are exhausted.", "gauge", []string{"namespace", "cluster"}},
	{"karakoram_control_plane_backup_enabled", "Whether scheduled control-plane backups are enabled.", "gauge", nil},
	{"karakoram_control_plane_backup_last_success_timestamp_seconds", "Unix timestamp of the last successful control-plane backup.", "gauge", nil},
	{"karakoram_control_plane_backup_last_failure_timestamp_seconds", "Unix timestamp of the last failed control-plane backup.", "gauge", nil},
	{"karakoram_notification_deliveries_failed_total", "Total notification deliveries that exhausted retries.", "counter", []string{"channel"}},
}
