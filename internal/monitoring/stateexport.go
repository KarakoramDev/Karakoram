// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package monitoring

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// ClusterState is the durable inventory used for one authorized cluster scrape.
type ClusterState struct {
	Cluster domain.Cluster
	Backups []domain.Backup
	Drills  []domain.BackupDrill
}

// CertificateExpiry is public certificate identity, never certificate/key material.
// Empty namespace and cluster designate an installation certificate.
type CertificateExpiry struct {
	Namespace   string
	Cluster     string
	Certificate string
	Expiry      time.Time
}

// StateSnapshot contains only records authorized by the caller. Installation is
// explicitly separate: a cluster monitoring grant never reveals installation data.
type StateSnapshot struct {
	Clusters                  []ClusterState
	Certificates              []CertificateExpiry
	Installation              bool
	ControlPlaneBackupEnabled bool
	ControlPlaneBackups       []domain.ControlPlaneBackup
	NotificationFailures      map[string]int64
}

var stateLabelEscaper = strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\"", "\\\"")

// WriteStatePrometheus writes current durable state, independent of agent scraping.
// Missing terminal results are zero; timestamps are completion times, not creation
// times or data-recovery points. No observation timestamp is synthesized.
func WriteStatePrometheus(w io.Writer, snapshot StateSnapshot) error {
	values := make(map[string][]string, len(StateMetrics))
	add := func(name string, value float64, labels ...string) {
		var b strings.Builder
		b.WriteString(name)
		if len(labels) != 0 {
			b.WriteByte('{')
			for i := 0; i < len(labels); i += 2 {
				if i != 0 {
					b.WriteByte(',')
				}
				b.WriteString(labels[i])
				b.WriteString("=\"")
				b.WriteString(stateLabelEscaper.Replace(labels[i+1]))
				b.WriteByte('"')
			}
			b.WriteByte('}')
		}
		b.WriteByte(' ')
		b.WriteString(strconv.FormatFloat(value, 'g', -1, 64))
		b.WriteByte('\n')
		values[name] = append(values[name], b.String())
	}
	for _, state := range snapshot.Clusters {
		c := state.Cluster
		base := []string{"namespace", domain.NormalizedNamespace(c.Namespace), "cluster", c.Name}
		emit := func(name string, value float64, labels ...string) { add(name, value, append(base, labels...)...) }
		emit("karakoram_cluster_info", 1, "provider", c.Spec.Provider, "version", c.Status.Version, "phase", string(c.Status.Phase))
		emit("karakoram_cluster_ready", boolean(c.Status.Phase == domain.PhaseReady))
		emit("karakoram_cluster_degraded", boolean(c.Status.Phase == domain.PhaseDegraded || c.Status.Phase == domain.PhaseError))
		conditions := map[string]bool{}
		exhausted := len(c.Status.DeploymentFailures) > 0
		for _, condition := range c.Status.Conditions {
			key := condition.Type + "\x00" + condition.Status + "\x00" + condition.Reason
			if !conditions[key] {
				emit("karakoram_cluster_condition", 1, "type", condition.Type, "status", condition.Status, "reason", condition.Reason)
				conditions[key] = true
			}
			if condition.Reason == "DeploymentRetriesExhausted" {
				exhausted = true
			}
		}
		coLocated := false
		for group, members := range c.Status.Placement.Groups {
			switch strings.ToLower(group) {
			case "", "unknown", "unavailable", "pending":
				continue
			}
			if len(members) > 1 {
				coLocated = true
			}
		}
		emit("karakoram_cluster_members_co_located", boolean(coLocated))
		outdated := map[string]bool{}
		if c.Status.Agents != nil {
			for _, member := range c.Status.Agents.Outdated {
				outdated[member] = true
			}
		}
		for _, member := range c.Status.Members {
			emit("karakoram_member_agent_outdated", boolean(outdated[member.Name]), "member", member.Name)
		}
		successes := map[string]time.Time{"": {}}
		for _, schedule := range c.Spec.Backup.Schedules {
			if schedule.Enabled {
				successes[schedule.Name] = time.Time{}
			}
		}
		var failure time.Time
		for _, backup := range state.Backups {
			if backup.CompletedAt == nil {
				continue
			}
			if backup.Phase == domain.PhaseSucceeded {
				if backup.CompletedAt.After(successes[""]) {
					successes[""] = *backup.CompletedAt
				}
				if _, enabled := successes[backup.Schedule]; enabled && backup.CompletedAt.After(successes[backup.Schedule]) {
					successes[backup.Schedule] = *backup.CompletedAt
				}
			}
			if backup.Phase == domain.PhaseFailed && backup.CompletedAt.After(failure) {
				failure = *backup.CompletedAt
			}
		}
		for schedule, completed := range successes {
			emit("karakoram_backup_last_success_timestamp_seconds", unixSeconds(completed), "schedule", schedule)
		}
		emit("karakoram_backup_last_failure_timestamp_seconds", unixSeconds(failure))
		var drillSuccess, drillFailure time.Time
		var duration float64
		for _, drill := range state.Drills {
			if drill.CompletedAt == nil {
				continue
			}
			if drill.Phase == "Succeeded" && drill.CompletedAt.After(drillSuccess) {
				drillSuccess = *drill.CompletedAt
				duration = float64(drill.RestoreDurationSeconds)
			}
			if drill.Phase == "Failed" && drill.CompletedAt.After(drillFailure) {
				drillFailure = *drill.CompletedAt
			}
		}
		emit("karakoram_backup_drill_enabled", boolean(c.Spec.Backup.Drill != nil && c.Spec.Backup.Drill.Enabled))
		emit("karakoram_backup_drill_last_success_timestamp_seconds", unixSeconds(drillSuccess))
		emit("karakoram_backup_drill_last_failure_timestamp_seconds", unixSeconds(drillFailure))
		emit("karakoram_backup_drill_last_restore_duration_seconds", duration)
		emit("karakoram_deployment_retries_exhausted", boolean(exhausted))
	}
	for _, cert := range snapshot.Certificates {
		add("karakoram_certificate_expiry_timestamp_seconds", unixSeconds(cert.Expiry), "namespace", cert.Namespace, "cluster", cert.Cluster, "certificate", cert.Certificate)
	}
	if snapshot.Installation {
		var success, failure time.Time
		for _, backup := range snapshot.ControlPlaneBackups {
			if backup.CompletedAt == nil {
				continue
			}
			if backup.Phase == domain.PhaseSucceeded && backup.CompletedAt.After(success) {
				success = *backup.CompletedAt
			}
			if backup.Phase == domain.PhaseFailed && backup.CompletedAt.After(failure) {
				failure = *backup.CompletedAt
			}
		}
		add("karakoram_control_plane_backup_enabled", boolean(snapshot.ControlPlaneBackupEnabled))
		add("karakoram_control_plane_backup_last_success_timestamp_seconds", unixSeconds(success))
		add("karakoram_control_plane_backup_last_failure_timestamp_seconds", unixSeconds(failure))
		for channel, count := range snapshot.NotificationFailures {
			add("karakoram_notification_deliveries_failed_total", float64(count), "channel", channel)
		}
	}
	for _, descriptor := range StateMetrics {
		series := values[descriptor.Name]
		if len(series) == 0 {
			continue
		}
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", descriptor.Name, descriptor.Help, descriptor.Name, descriptor.Type); err != nil {
			return err
		}
		sort.Strings(series)
		for _, line := range series {
			if _, err := io.WriteString(w, line); err != nil {
				return err
			}
		}
	}
	return nil
}

func boolean(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
func unixSeconds(value time.Time) float64 {
	if value.IsZero() {
		return 0
	}
	return float64(value.Unix()) + float64(value.Nanosecond())/1e9
}
