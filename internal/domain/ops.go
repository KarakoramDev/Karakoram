// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package domain

import "time"

type AgentBuild struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Build   string `json:"build"`
}

type AgentRolloutSettings struct {
	Automatic             bool        `json:"automatic"`
	MaxConcurrentClusters int         `json:"maxConcurrentClusters"`
	CurrentBuild          *AgentBuild `json:"currentBuild,omitempty"`
}

type AgentRolloutStatus struct {
	TargetBuild   string   `json:"targetBuild"`
	TargetVersion string   `json:"targetVersion"`
	Outdated      []string `json:"outdated"`
	Unknown       []string `json:"unknown"`
}

type MaintenancePolicy struct {
	TimeZone string              `json:"timeZone,omitempty"`
	Windows  []MaintenanceWindow `json:"windows,omitempty"`
}

type MaintenanceWindow struct {
	Schedule string `json:"schedule"`
	Duration string `json:"duration"`
}

type MaintenanceStatus struct {
	Open             bool       `json:"open"`
	CurrentWindowEnd *time.Time `json:"currentWindowEnd,omitempty"`
	NextWindowStart  *time.Time `json:"nextWindowStart,omitempty"`
}

type ManagedBy struct {
	Kind    string `json:"kind"`
	Cluster string `json:"cluster"`
	ID      string `json:"id"`
}

type BackupDrillPolicy struct {
	Enabled        bool   `json:"enabled"`
	Schedule       string `json:"schedule"`
	BackupSchedule string `json:"backupSchedule,omitempty"`
	Timeout        string `json:"timeout,omitempty"`
}

type ControlPlaneBackupStorage struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type ControlPlaneBackupPolicy struct {
	Enabled     bool                      `json:"enabled"`
	Storage     ControlPlaneBackupStorage `json:"storage"`
	Schedule    string                    `json:"schedule"`
	RetainCount int                       `json:"retainCount"`
}

type ControlPlaneBackup struct {
	ID               string                    `json:"id"`
	Trigger          string                    `json:"trigger"`
	Phase            Phase                     `json:"phase"`
	Storage          ControlPlaneBackupStorage `json:"storage"`
	ObjectKey        string                    `json:"objectKey,omitempty"`
	SizeBytes        int64                     `json:"sizeBytes,omitempty"`
	SHA256           string                    `json:"sha256,omitempty"`
	SchemaVersion    string                    `json:"schemaVersion,omitempty"`
	KarakoramVersion string                    `json:"karakoramVersion,omitempty"`
	KeyFingerprint   string                    `json:"keyFingerprint,omitempty"`
	CreatedAt        time.Time                 `json:"createdAt"`
	StartedAt        *time.Time                `json:"startedAt,omitempty"`
	CompletedAt      *time.Time                `json:"completedAt,omitempty"`
	Error            string                    `json:"error,omitempty"`
}

type NotificationCategory string

const (
	NotificationBackupFailed               NotificationCategory = "backup-failed"
	NotificationClusterDegraded            NotificationCategory = "cluster-degraded"
	NotificationClusterRecovered           NotificationCategory = "cluster-recovered"
	NotificationVolumeDeletionRefused      NotificationCategory = "volume-deletion-refused"
	NotificationCertificateExpiring        NotificationCategory = "certificate-expiring"
	NotificationDeploymentRetriesExhausted NotificationCategory = "deployment-retries-exhausted"
	NotificationOperationFailed            NotificationCategory = "operation-failed"
	NotificationMembersCoLocated           NotificationCategory = "members-co-located"
	NotificationBackupDrillFailed          NotificationCategory = "backup-drill-failed"
	NotificationControlPlaneBackupFailed   NotificationCategory = "control-plane-backup-failed"
	NotificationAgentsOutdated             NotificationCategory = "agents-outdated"
)

type NotificationChannel struct {
	Name       string                     `json:"name"`
	Type       string                     `json:"type"`
	Enabled    bool                       `json:"enabled"`
	Categories []NotificationCategory     `json:"categories,omitempty"`
	Namespaces []string                   `json:"namespaces,omitempty"`
	Webhook    *NotificationWebhook       `json:"webhook,omitempty"`
	Slack      *NotificationSlack         `json:"slack,omitempty"`
	Email      *NotificationEmail         `json:"email,omitempty"`
	Status     NotificationChannelStatus  `json:"status"`
	CreatedAt  time.Time                  `json:"createdAt"`
	UpdatedAt  time.Time                  `json:"updatedAt"`
	Secrets    NotificationChannelSecrets `json:"-"`
}

// NotificationChannelSecrets must be sealed independently of public channel records.
type NotificationChannelSecrets struct {
	WebhookURL           string
	WebhookSigningSecret string
	SlackWebhookURL      string
	EmailPassword        string
}

type NotificationWebhook struct {
	URLConfigured           bool   `json:"urlConfigured"`
	SigningSecretConfigured bool   `json:"signingSecretConfigured"`
	Host                    string `json:"host,omitempty"`
}

type NotificationSlack struct {
	WebhookURLConfigured bool `json:"webhookUrlConfigured"`
}

type NotificationEmail struct {
	Host               string   `json:"host"`
	Port               int      `json:"port"`
	Security           string   `json:"security"`
	Username           string   `json:"username,omitempty"`
	PasswordConfigured bool     `json:"passwordConfigured"`
	From               string   `json:"from"`
	To                 []string `json:"to"`
}

type NotificationChannelStatus struct {
	LastDeliveryAt *time.Time `json:"lastDeliveryAt,omitempty"`
	LastErrorAt    *time.Time `json:"lastErrorAt,omitempty"`
	LastError      string     `json:"lastError,omitempty"`
	Pending        int64      `json:"pending,omitempty"`
	Failed         int64      `json:"failed,omitempty"`
}

type BackupDrill struct {
	ID                     string             `json:"id"`
	Namespace              string             `json:"namespace"`
	Cluster                string             `json:"cluster"`
	Backup                 string             `json:"backup,omitempty"`
	BackupType             string             `json:"backupType,omitempty"`
	Trigger                string             `json:"trigger"`
	Phase                  string             `json:"phase"`
	TargetCluster          string             `json:"targetCluster,omitempty"`
	CreatedAt              time.Time          `json:"createdAt"`
	StartedAt              *time.Time         `json:"startedAt,omitempty"`
	ReadyAt                *time.Time         `json:"readyAt,omitempty"`
	CompletedAt            *time.Time         `json:"completedAt,omitempty"`
	RestoreDurationSeconds int64              `json:"restoreDurationSeconds,omitempty"`
	TotalDurationSeconds   int64              `json:"totalDurationSeconds,omitempty"`
	DataAgeSeconds         int64              `json:"dataAgeSeconds,omitempty"`
	Checks                 []BackupDrillCheck `json:"checks,omitempty"`
	Limitations            string             `json:"limitations,omitempty"`
	Cleanup                BackupDrillCleanup `json:"cleanup"`
	Error                  string             `json:"error,omitempty"`
}

type BackupDrillCheck struct {
	Name    string `json:"name"`
	Passed  bool   `json:"passed"`
	Message string `json:"message,omitempty"`
}

type BackupDrillCleanup struct {
	Phase           string   `json:"phase"`
	OrphanedVolumes []string `json:"orphanedVolumes"`
}
