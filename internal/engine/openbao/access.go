// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// Access is the OpenBao-specific API implemented by the provider. Server
// handlers type-assert it; callers enforce RBAC (including the target
// cluster's database-users:create for connections) and audit.
type Access interface {
	// RevealUnsealKeys returns the stored initialization material. It fails
	// with engine.ErrUnavailable before initialization completed.
	RevealUnsealKeys(ctx context.Context, rc *engine.Context) (UnsealKeys, error)
	ListDatabaseConnections(ctx context.Context, rc *engine.Context) ([]DatabaseConnection, error)
	// CreateDatabaseConnection is idempotent for the same name and request;
	// a different request for an existing name is engine.ErrConflict.
	CreateDatabaseConnection(ctx context.Context, rc *engine.Context, target Target, req DatabaseConnectionWrite) (DatabaseConnection, error)
	// DeleteDatabaseConnection removes the OpenBao configuration, then the
	// target identity. A missing connection is engine.ErrNotFound.
	DeleteDatabaseConnection(ctx context.Context, rc *engine.Context, target Target, name string) error
	// ConnectionTargets lists the clusters this OpenBao cluster holds
	// connections to; their deletion is refused while connections exist.
	ConnectionTargets(ctx context.Context, rc *engine.Context) ([]domain.ClusterRef, error)
}

// Target is a resolved database-connection target: the target cluster's own
// provider and namespace-resolved context, built by the server.
type Target struct {
	Provider engine.Provider
	Context  *engine.Context
}

// UnsealKeys is the revealed initialization material.
type UnsealKeys struct {
	// SealType is shamir, transit or awskms; for auto-unseal seals the shares
	// are recovery shares.
	SealType       string `json:"sealType"`
	RecoveryShares bool   `json:"recoveryShares"`
	Threshold      int    `json:"threshold"`
	// Shares are base64-encoded shares, as accepted by `bao operator unseal`.
	Shares           []string  `json:"shares"`
	InitialRootToken string    `json:"initialRootToken"`
	InitializedAt    time.Time `json:"initializedAt"`
}

// Database-connection access levels.
const (
	AccessReadOnly  = "readonly"
	AccessReadWrite = "readwrite"
)

// Database-connection phases.
const (
	ConnectionPending  = "pending"
	ConnectionReady    = "ready"
	ConnectionDeleting = "deleting"
	ConnectionFailed   = "failed"
)

// DatabaseConnectionWrite requests a connection to a Karakoram PostgreSQL,
// PSMDB replica-set or RabbitMQ cluster in the same namespace.
type DatabaseConnectionWrite struct {
	// Name is ^[a-z][a-z0-9-]{0,30}$ and unique per OpenBao cluster.
	Name          string              `json:"name"`
	TargetCluster string              `json:"targetCluster"`
	Roles         []DatabaseRoleWrite `json:"roles"`
}

// DatabaseRoleWrite defines one dynamic-credential role.
type DatabaseRoleWrite struct {
	// Name is ^[a-z][a-z0-9-]{0,30}$, unique per connection.
	Name string `json:"name"`
	// Access is readonly or readwrite.
	Access string `json:"access"`
	// Database scopes MongoDB roles to one database or RabbitMQ roles to one
	// vhost (default "/"); empty means every database. Not used for PostgreSQL.
	Database string `json:"database,omitempty"`
	// DefaultTTLSeconds and MaxTTLSeconds bound issued credentials (defaults
	// 3600 and 86400; MaxTTLSeconds >= DefaultTTLSeconds).
	// RabbitMQ configures leases per mount; all roles in one RabbitMQ
	// connection must use the same default/maximum pair.
	DefaultTTLSeconds int64 `json:"defaultTtlSeconds,omitempty"`
	MaxTTLSeconds     int64 `json:"maxTtlSeconds,omitempty"`
}

// DatabaseConnection is the public, secret-free connection record.
type DatabaseConnection struct {
	Name           string `json:"name"`
	TargetCluster  string `json:"targetCluster"`
	TargetProvider string `json:"targetProvider"`
	// MountPath is database (PostgreSQL, MongoDB) or rabbitmq-<name>.
	MountPath string         `json:"mountPath"`
	Phase     string         `json:"phase"`
	Message   string         `json:"message,omitempty"`
	Roles     []DatabaseRole `json:"roles"`
	CreatedAt time.Time      `json:"createdAt"`
}

// DatabaseRole is a configured role and the path that issues credentials.
type DatabaseRole struct {
	DatabaseRoleWrite
	// CredentialsPath is the OpenBao read path, e.g. database/creds/<conn>_<role>.
	CredentialsPath string `json:"credentialsPath"`
}
