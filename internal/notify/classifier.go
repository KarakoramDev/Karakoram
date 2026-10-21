// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package notify

import "github.com/KarakoramDev/Karakoram/internal/domain"

// Classify uses durable reasons and operation targets, never message parsing.
func Classify(event domain.Event, operation *domain.Operation) domain.NotificationCategory {
	if event.Type != "Warning" && event.Type != "Normal" {
		return ""
	}
	if event.Reason == "ClusterRecovered" {
		if event.Type == "Normal" {
			return domain.NotificationClusterRecovered
		}
		return ""
	}
	if event.Reason == "OrphanedVolumeRecordRemoved" {
		return domain.NotificationVolumeDeletionRefused
	}
	if event.Type != "Warning" {
		return ""
	}
	switch event.Reason {
	case "BackupFailed", "BackupScheduleFailed":
		return domain.NotificationBackupFailed
	case "ClusterDegraded":
		return domain.NotificationClusterDegraded
	case "VolumeDeletionRefused", "OrphanedVolumeDeleteStuck":
		return domain.NotificationVolumeDeletionRefused
	case "CertificateExpiring":
		return domain.NotificationCertificateExpiring
	case "DeploymentRetriesExhausted":
		return domain.NotificationDeploymentRetriesExhausted
	case "OperationFailed":
		if operation != nil && operation.Target.Kind == "backup" {
			return domain.NotificationBackupFailed
		}
		return domain.NotificationOperationFailed
	case "MembersCoLocated":
		return domain.NotificationMembersCoLocated
	case "BackupDrillFailed":
		return domain.NotificationBackupDrillFailed
	case "ControlPlaneBackupFailed":
		return domain.NotificationControlPlaneBackupFailed
	case "AgentsOutdated":
		return domain.NotificationAgentsOutdated
	}
	return ""
}

func Matches(channel domain.NotificationChannel, event domain.Event, category domain.NotificationCategory) bool {
	if !channel.Enabled || category == "" {
		return false
	}
	if len(channel.Categories) > 0 {
		found := false
		for _, c := range channel.Categories {
			if c == category {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if len(channel.Namespaces) > 0 {
		for _, ns := range channel.Namespaces {
			if ns == event.Namespace {
				return true
			}
		}
		return false
	}
	return true
}
