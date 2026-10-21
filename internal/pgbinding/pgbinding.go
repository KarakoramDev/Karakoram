// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package pgbinding defines how a consumer service binds to an existing
// Karakoram PostgreSQL cluster: the control plane creates the consumer's
// databases and one least-privilege owning login role on the source, seals the
// role password, and blocks source deletion while the binding exists.
// See docs/adr/0045-postgres-bindings.md.
package pgbinding

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// MaxDatabases bounds the databases one binding may own.
const MaxDatabases = 8

// TLS mode values reported in Connection.TLS.Mode.
const (
	TLSVerifyFull = "verify-full"
	TLSVerifyCA   = "verify-ca"
)

// ErrSourceUnavailable reports a source that does not exist, is being deleted,
// is not a PostgreSQL provider, or is outside a consumer's major-version range.
var ErrSourceUnavailable = errors.New("postgres binding source unavailable")

// Request asks for a binding of one consumer to one source cluster. Ensure is
// idempotent for the same (consumer, source) identity; changing Databases or
// Extensions of an existing binding is additive.
type Request struct {
	// ConsumerKind is the consumer provider, e.g. "keycloak", "temporal", "openbao".
	ConsumerKind      string
	ConsumerNamespace string
	ConsumerName      string
	SourceNamespace   string
	SourceCluster     string
	// Databases are created on the source and owned by the binding role (>=1, same cluster).
	Databases []string
	// Extensions are created in every bound database; each must be available on the source.
	Extensions []string
	// MinMajor and MaxMajor bound the PostgreSQL majors the consumer supports;
	// zero means unbounded. The source's major upgrades honour MaxMajor.
	MinMajor, MaxMajor int
}

// BindingID returns the stable, URL-safe identity of a consumer/source pair.
// Empty namespaces mean "default". Database, extension and version requests do
// not affect identity. Cleanup must Release this ID even if Ensure never succeeded.
func BindingID(r Request) string {
	consumerNamespace, sourceNamespace := r.ConsumerNamespace, r.SourceNamespace
	if consumerNamespace == "" {
		consumerNamespace = "default"
	}
	if sourceNamespace == "" {
		sourceNamespace = "default"
	}
	identity := strings.Join([]string{r.ConsumerKind, consumerNamespace, r.ConsumerName, sourceNamespace, r.SourceCluster}, "\x00")
	hash := sha256.Sum256([]byte(identity))
	return "pgb-" + hex.EncodeToString(hash[:16])
}

// TLS carries the trust material a consumer needs to verify the source endpoint.
type TLS struct {
	Mode  string
	CAPEM []byte
}

// Connection is the resolved, non-secret connection material of a binding.
type Connection struct {
	BindingID string
	// Host and Port address the source's stable writer endpoint.
	Host      string
	Port      int
	Databases []string
	Username  string
	// PasswordSecretRef names the sealed password; resolve it only in-process
	// through Service.Password and never return it through an API read.
	PasswordSecretRef string
	TLS               TLS
	// MaxConnections is the role's connection limit; zero means unlimited.
	MaxConnections int
	// CredentialGeneration increases on every password rotation. Consumers
	// compare it in Sync and re-render/roll their configuration on change.
	CredentialGeneration int64
	// Shared reports other consumers bound to the same source; ConsumerCount
	// includes this binding. In-place restores and major upgrades of the source
	// affect every bound consumer.
	Shared        bool
	ConsumerCount int
}

// Service manages bindings. Implementations are safe for concurrent use.
type Service interface {
	Ensure(context.Context, Request) (Connection, error)
	Resolve(ctx context.Context, bindingID string) (Connection, error)
	// Release removes the binding. Unless keepData, the bound databases and role
	// are dropped on the source after the consumer's runtimes are gone; with
	// keepData the databases remain and the role is disabled (NOLOGIN).
	Release(ctx context.Context, bindingID string, keepData bool) error
	Password(ctx context.Context, ref string) (string, error)
}

var (
	kindPattern       = regexp.MustCompile(`^[a-z][a-z0-9-]{0,30}$`)
	identifierPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
)

// Validate checks a request's syntax; source existence and extension
// availability are checked by the Service.
func (r Request) Validate() error {
	if !kindPattern.MatchString(r.ConsumerKind) {
		return fmt.Errorf("consumer kind %q is invalid", r.ConsumerKind)
	}
	if r.ConsumerName == "" || r.SourceCluster == "" {
		return errors.New("consumer name and source cluster are required")
	}
	if len(r.Databases) == 0 || len(r.Databases) > MaxDatabases {
		return fmt.Errorf("a binding needs 1-%d databases", MaxDatabases)
	}
	seen := map[string]bool{}
	for _, name := range r.Databases {
		if !identifierPattern.MatchString(name) || name == "postgres" || name == "template0" || name == "template1" {
			return fmt.Errorf("database name %q is invalid", name)
		}
		if strings.HasPrefix(name, "_karakoram") {
			return fmt.Errorf("database name %q is reserved", name)
		}
		if seen[name] {
			return fmt.Errorf("database %q is listed twice", name)
		}
		seen[name] = true
	}
	for _, name := range r.Extensions {
		if !identifierPattern.MatchString(name) {
			return fmt.Errorf("extension name %q is invalid", name)
		}
	}
	if r.MinMajor < 0 || r.MaxMajor < 0 || (r.MaxMajor != 0 && r.MinMajor > r.MaxMajor) {
		return errors.New("major-version range is invalid")
	}
	return nil
}

// Admits reports whether a source major satisfies the request's range.
func (r Request) Admits(major int) bool {
	return (r.MinMajor == 0 || major >= r.MinMajor) && (r.MaxMajor == 0 || major <= r.MaxMajor)
}

// Backup and operation phases reported by BackupBinder.
const (
	PhasePending   = "pending"
	PhaseRunning   = "running"
	PhaseCompleted = "completed"
	PhaseFailed    = "failed"
)

// ErrSharedSource rejects a restore through a binding whose source serves
// other consumers unless the caller explicitly acknowledges the impact.
var ErrSharedSource = errors.New("postgres binding source is shared by other consumers")

// BackupRef identifies a source backup created through a binding.
type BackupRef struct{ ID string }

// OperationRef identifies a source operation started through a binding.
type OperationRef struct{ ID string }

// State is the observed progress of a backup or operation.
type State struct {
	Phase       string
	CompletedAt time.Time
	Error       string
}

// BackupBinder is an optional Service extension that lets a consumer request
// and observe backups and in-place restores of its source without owning it.
// Backup is idempotent by idempotencyKey. Restore returns ErrSharedSource for
// a shared source unless acknowledgeShared is true; it is destructive for every
// consumer of the source.
type BackupBinder interface {
	Backup(ctx context.Context, bindingID, backupType, idempotencyKey string) (BackupRef, error)
	BackupStatus(ctx context.Context, bindingID, backupID string) (State, error)
	Restore(ctx context.Context, bindingID, backupID, idempotencyKey string, acknowledgeShared bool) (OperationRef, error)
	OperationStatus(ctx context.Context, bindingID, operationID string) (State, error)
}
