// SPDX-License-Identifier: Apache-2.0
package notify

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"testing"
)

func TestClassifier(t *testing.T) {
	cases := []struct {
		reason, kind string
		category     domain.NotificationCategory
	}{
		{"BackupFailed", "", domain.NotificationBackupFailed}, {"BackupScheduleFailed", "", domain.NotificationBackupFailed},
		{"ClusterDegraded", "", domain.NotificationClusterDegraded}, {"ClusterRecovered", "", domain.NotificationClusterRecovered},
		{"VolumeDeletionRefused", "", domain.NotificationVolumeDeletionRefused}, {"OrphanedVolumeDeleteStuck", "", domain.NotificationVolumeDeletionRefused}, {"OrphanedVolumeRecordRemoved", "", domain.NotificationVolumeDeletionRefused},
		{"CertificateExpiring", "", domain.NotificationCertificateExpiring}, {"DeploymentRetriesExhausted", "", domain.NotificationDeploymentRetriesExhausted},
		{"OperationFailed", "backup", domain.NotificationBackupFailed}, {"OperationFailed", "cluster", domain.NotificationOperationFailed},
		{"MembersCoLocated", "", domain.NotificationMembersCoLocated}, {"BackupDrillFailed", "", domain.NotificationBackupDrillFailed},
		{"ControlPlaneBackupFailed", "", domain.NotificationControlPlaneBackupFailed}, {"AgentsOutdated", "", domain.NotificationAgentsOutdated}, {"BackupCompleted", "", ""}, {"Unknown", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.reason+tc.kind, func(t *testing.T) {
			typ := "Warning"
			if tc.reason == "ClusterRecovered" || tc.reason == "OrphanedVolumeRecordRemoved" {
				typ = "Normal"
			}
			event := domain.Event{Type: typ, Reason: tc.reason}
			op := &domain.Operation{Target: domain.OperationTarget{Kind: tc.kind}}
			if got := Classify(event, op); got != tc.category {
				t.Fatalf("got %s, want %s", got, tc.category)
			}
			event.Type = "Audit"
			if got := Classify(event, op); got != "" {
				t.Fatalf("audit delivered: %s", got)
			}
		})
	}
}
func TestNotificationFilters(t *testing.T) {
	c := domain.NotificationChannel{Enabled: true}
	event := domain.Event{}
	if !Matches(c, event, domain.NotificationAgentsOutdated) {
		t.Fatal("empty filters exclude installation events")
	}
	c.Namespaces = []string{"default"}
	if Matches(c, event, domain.NotificationAgentsOutdated) {
		t.Fatal("namespace filter includes installation event")
	}
	event.Namespace = "default"
	c.Categories = []domain.NotificationCategory{domain.NotificationBackupFailed}
	if Matches(c, event, domain.NotificationAgentsOutdated) {
		t.Fatal("category filter ignored")
	}
	if !Matches(c, event, domain.NotificationBackupFailed) {
		t.Fatal("matching category excluded")
	}
}
