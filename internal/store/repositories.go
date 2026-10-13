// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// BackupStorageRepository provides encrypted destination persistence.
type BackupStorageRepository interface {
	CreateBackupStorage(context.Context, *domain.BackupStorage) error
	UpdateBackupStorage(context.Context, *domain.BackupStorage) error
	GetBackupStorage(context.Context, string) (*domain.BackupStorage, error)
	ListBackupStorages(context.Context) ([]domain.BackupStorage, error)
	DeleteBackupStorage(context.Context, string) error
}

// BackupRepository provides backup inventory and atomic operation enqueueing.
type BackupRepository interface {
	CreateBackup(context.Context, *domain.Backup) error
	UpdateBackup(context.Context, *domain.Backup) error
	GetBackup(context.Context, string, string) (*domain.Backup, error)
	ListBackups(context.Context, string) ([]domain.Backup, error)
	DeleteBackup(context.Context, string, string) error
	EnqueueBackup(context.Context, *domain.Backup, *domain.Operation) error
	EnqueueBackupDeletion(context.Context, *domain.Backup, *domain.Operation) error
}

// RestoreRepository provides durable restore inventory.
type RestoreRepository interface {
	CreateRestore(context.Context, *domain.Restore) error
	UpdateRestore(context.Context, *domain.Restore) error
	GetRestore(context.Context, string, string) (*domain.Restore, error)
	ListRestores(context.Context, string) ([]domain.Restore, error)
	DeleteRestore(context.Context, string, string) error
}

// AccountRepository provides local account identity and password hash storage.
type AccountRepository interface {
	CreateAccount(context.Context, *domain.Account) error
	UpdateAccount(context.Context, *domain.Account) error
	GetAccount(context.Context, string) (*domain.Account, error)
	ListAccounts(context.Context) ([]domain.Account, error)
	DeleteAccount(context.Context, string) error
}

// ApiTokenRepository provides hashed API token lookup and usage tracking.
type ApiTokenRepository interface {
	CreateApiToken(context.Context, *domain.ApiToken) error
	UpdateApiToken(context.Context, *domain.ApiToken) error
	GetApiToken(context.Context, string) (*domain.ApiToken, error)
	FindApiTokenByHash(context.Context, string) (*domain.ApiToken, error)
	ListApiTokens(context.Context) ([]domain.ApiToken, error)
	TouchApiToken(context.Context, string, time.Time) error
	DeleteApiToken(context.Context, string) error
}

// SessionRepository provides refresh token storage and atomic rotation.
type SessionRepository interface {
	CreateSession(context.Context, *domain.Session) error
	UpdateSession(context.Context, *domain.Session) error
	GetSession(context.Context, string) (*domain.Session, error)
	FindSessionByHash(context.Context, string) (*domain.Session, error)
	ListSessions(context.Context) ([]domain.Session, error)
	RotateSession(context.Context, string, string, string, time.Time) error
	DeleteSession(context.Context, string) error
}

// PolicyRepository persists ordered RBAC rules as an atomic policy set.
type PolicyRepository interface {
	ListPolicyRules(context.Context) ([]domain.PolicyRule, error)
	ReplacePolicyRules(context.Context, []domain.PolicyRule) error
}

// SettingsRepository persists JSON-valued control-plane settings.
type SettingsRepository interface {
	SetSetting(context.Context, string, json.RawMessage) error
	GetSetting(context.Context, string) (json.RawMessage, error)
	ListSettings(context.Context) (map[string]json.RawMessage, error)
	DeleteSetting(context.Context, string) error
}

// SecretRepository persists identity-bound encrypted cluster values.
type SecretRepository interface {
	SetClusterSecret(context.Context, string, string, []byte) error
	GetClusterSecret(context.Context, string, string) ([]byte, error)
	DeleteClusterSecret(context.Context, string, string) error
}

// AgentTokenRepository persists both encrypted and hashed member tokens.
type AgentTokenRepository interface {
	SetAgentToken(context.Context, *domain.AgentToken) error
	GetAgentToken(context.Context, string, string) (*domain.AgentToken, error)
	FindAgentTokenByHash(context.Context, string) (*domain.AgentToken, error)
	DeleteAgentToken(context.Context, string, string) error
}

// MetricRepository provides transactional ingestion, range reads, and retention.
type MetricRepository interface {
	InsertMetricSamples(context.Context, []domain.MetricSample) error
	QueryMetricSamples(context.Context, MetricQuery) ([]domain.MetricSample, error)
	PruneMetricSamples(context.Context, time.Time) (int64, error)
}

// RetainedVolumeRepository preserves mappings needed to resume suspended clusters.
type RetainedVolumeRepository interface {
	CreateRetainedVolume(context.Context, *domain.RetainedVolume) error
	UpdateRetainedVolume(context.Context, *domain.RetainedVolume) error
	GetRetainedVolume(context.Context, string, string) (*domain.RetainedVolume, error)
	ListRetainedVolumes(context.Context, string) ([]domain.RetainedVolume, error)
	DeleteRetainedVolume(context.Context, string, string) error
}
