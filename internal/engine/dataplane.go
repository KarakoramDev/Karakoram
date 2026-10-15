// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// Sentinel errors providers return so the API layer can map them to HTTP
// status codes (ErrNotFound → 404, ErrNotSupported and ErrConflict → 409,
// ErrInvalid → 400, ErrUnavailable → 503). Wrap them with fmt.Errorf("...: %w", err) for context.
var (
	ErrNotFound     = errors.New("not found")
	ErrNotSupported = errors.New("not supported for this cluster")
	ErrConflict     = errors.New("conflict")
	ErrInvalid      = errors.New("invalid request")
	ErrUnavailable  = errors.New("temporarily unavailable")
)

// ValidationError is an ErrInvalid whose Message is safe to show to the API
// caller: it explains which part of the request was rejected and why. Field
// (JSON path such as spec.components.engine.replicas) and Reason (one of the
// domain.Reason* constants) are optional and become the API error details.
type ValidationError struct{ Field, Reason, Message string }

func (e *ValidationError) Error() string { return ErrInvalid.Error() + ": " + e.Message }

// Is makes errors.Is(err, ErrInvalid) hold for validation errors.
func (e *ValidationError) Is(target error) bool { return target == ErrInvalid }

// Invalid returns a ValidationError with a formatted, caller-facing message.
func Invalid(format string, args ...any) error {
	return &ValidationError{Message: fmt.Sprintf(format, args...)}
}

// InvalidField returns a ValidationError for one request field. The message
// should name the field so it reads well without the details object.
func InvalidField(field, reason, format string, args ...any) error {
	return &ValidationError{Field: field, Reason: reason, Message: fmt.Sprintf(format, args...)}
}

// DataPlane is implemented by providers for synchronous requests that the
// API serves directly against a running cluster (as opposed to declarative
// spec changes, which go through Sync, and long-running actions, which go
// through RunOperation). Every method receives a fully populated Context for
// the target cluster.
type DataPlane interface {
	// Connection returns connection details including credentials. Callers
	// enforce the cluster-credentials read permission.
	Connection(ctx context.Context, rc *Context) (domain.ConnectionDetails, error)
	// SystemUsers lists the provider-managed system users (no passwords).
	SystemUsers(ctx context.Context, rc *Context) ([]domain.SystemUser, error)

	ListDatabaseUsers(ctx context.Context, rc *Context) ([]domain.DatabaseUser, error)
	GetDatabaseUser(ctx context.Context, rc *Context, db, name string) (*domain.DatabaseUser, error)
	// CreateDatabaseUser creates an application user. An empty password makes
	// the provider generate one; the effective password is returned once.
	CreateDatabaseUser(ctx context.Context, rc *Context, user domain.DatabaseUser, password string) (string, error)
	// UpdateDatabaseUser replaces roles and, when password is non-empty, the password.
	UpdateDatabaseUser(ctx context.Context, rc *Context, user domain.DatabaseUser, password string) error
	DeleteDatabaseUser(ctx context.Context, rc *Context, db, name string) error

	ListDatabaseRoles(ctx context.Context, rc *Context) ([]domain.DatabaseRole, error)
	GetDatabaseRole(ctx context.Context, rc *Context, db, name string) (*domain.DatabaseRole, error)
	CreateDatabaseRole(ctx context.Context, rc *Context, role domain.DatabaseRole) error
	UpdateDatabaseRole(ctx context.Context, rc *Context, role domain.DatabaseRole) error
	DeleteDatabaseRole(ctx context.Context, rc *Context, db, name string) error

	// Balancer and SetBalancer are only valid for sharded clusters; providers
	// return ErrNotSupported for other topologies.
	Balancer(ctx context.Context, rc *Context) (domain.BalancerStatus, error)
	SetBalancer(ctx context.Context, rc *Context, enabled bool) error

	// PITRWindow returns the current restorable point-in-time windows.
	PITRWindow(ctx context.Context, rc *Context) ([]domain.TimeWindow, error)
	// Placement returns best-effort host placement of the members.
	Placement(ctx context.Context, rc *Context) (domain.Placement, error)
	// MemberLogs returns the last lines of a member process log.
	MemberLogs(ctx context.Context, rc *Context, member, process string, lines int) ([]domain.MemberLogLine, error)
}

// AgentConfigSource is implemented by providers to render the configuration a
// node agent pulls from the control plane. member is the member hostname the
// authenticated agent token belongs to.
type AgentConfigSource interface {
	MemberConfig(ctx context.Context, rc *Context, member string) (agentapi.MemberConfig, error)
}
