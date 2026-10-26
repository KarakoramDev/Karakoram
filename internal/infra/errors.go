// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra

import (
	"errors"
	"fmt"
	"time"
)

// Neutral error classes. Backends return errors that match these through
// errors.Is (directly, by wrapping, or with an Is method) and keep platform
// detail behind SafeDetail.
var (
	// ErrNotFound reports an absent service, route or operation target where
	// absence is not a normal (nil, nil) lookup result.
	ErrNotFound = errors.New("infra: not found")
	// ErrConflict reports a state conflict, e.g. a fenced delete whose
	// hostname now has a different ID.
	ErrConflict = errors.New("infra: conflict")
	// ErrRejected reports a request the backend refused as invalid; it is not
	// retried unchanged. RejectedError carries details.
	ErrRejected = errors.New("infra: request rejected")
	// ErrConfiguration reports backend configuration that must be corrected.
	// It does not imply that a resource mutation was definitively rejected.
	ErrConfiguration = errors.New("infra: backend configuration invalid")
	// ErrUnauthorized reports credentials that were rejected or lack the
	// required permissions for the target.
	ErrUnauthorized = errors.New("infra: credentials rejected")
	// ErrUnavailable reports a transient backend outage, rate limit or
	// network failure; the request may be retried.
	ErrUnavailable = errors.New("infra: backend unavailable")
	// ErrUnsupported reports a feature this backend does not provide
	// (see Capabilities).
	ErrUnsupported = errors.New("infra: not supported by backend")
	// ErrResourceChangeCooldown reports that the backend refuses another
	// resource-mode change for a while; callers wait before retrying.
	ErrResourceChangeCooldown = errors.New("infra: resource change cooling down")
	// ErrDeploymentRetriesExhausted requires an explicit operator re-arm or new intent.
	ErrDeploymentRetriesExhausted = errors.New("infra: platform deployment retries exhausted")
)

// RejectedError is a refused request with a stable backend code and a safe,
// bounded message. It matches ErrRejected.
type RejectedError struct {
	Code    string
	Message string
}

func (e *RejectedError) Error() string {
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

// Is matches ErrRejected.
func (e *RejectedError) Is(target error) bool { return target == ErrRejected }

// PublicPortConflictError reports a refused publication, not an uncertain
// delivery. The backend has preserved the previously applied service.
type PublicPortConflictError struct {
	Port int
}

func (e *PublicPortConflictError) Error() string {
	return fmt.Sprintf("public port %d is already published by another service on this Swarm; disable public exposure there or keep this cluster private", e.Port)
}
func (e *PublicPortConflictError) Is(target error) bool { return target == ErrRejected }
func (e *PublicPortConflictError) SafeDetail() string   { return e.Error() }

// PublicPortPublishError reports a failed publication whose underlying reason
// the platform hid, after proving and preserving the existing private service.
type PublicPortPublishError struct {
	Port int
}

func (e *PublicPortPublishError) Error() string {
	return fmt.Sprintf("Dokploy could not publish public port %d; check Swarm-wide port ownership in Dokploy and free any conflicting publication, or keep this cluster private; the private endpoint is unchanged", e.Port)
}
func (e *PublicPortPublishError) Is(target error) bool { return target == ErrRejected }
func (e *PublicPortPublishError) SafeDetail() string   { return e.Error() }

// EndpointRepairExhaustedError reports an unreachable published logical endpoint
// after the backend's durable repair budget has been consumed.
type EndpointRepairExhaustedError struct {
	Service  string
	Attempts int
}

func (e *EndpointRepairExhaustedError) Error() string {
	return fmt.Sprintf("PublishedEndpointRepairExhausted: service %s remains unreachable after %d repair attempts; repair the Swarm private DNS alias/VIP and overlay connectivity, or disable public exposure to restore private-only service; automatic repair is stopped", e.Service, e.Attempts)
}
func (e *EndpointRepairExhaustedError) SafeDetail() string { return e.Error() }

// DeploymentExhaustedError is a durable, non-retryable deployment failure.
// Backends must sanitize LastError before constructing it.
type DeploymentExhaustedError struct {
	Service, Version string
	Attempts         int
	LastError        string
	// PreviousResources restores provider-applied bounds after a conditional
	// initial-deployment resize was refused before replacement registration.
	PreviousResources *Resources
}

func (e *DeploymentExhaustedError) Error() string {
	message := fmt.Sprintf("DeploymentRetriesExhausted: service %s, app version %s, %d attempts; explicit operator re-arm required", e.Service, e.Version, e.Attempts)
	if e.LastError != "" {
		message += "; last platform error: " + e.LastError
	}
	return message
}

func (e *DeploymentExhaustedError) Unwrap() error      { return ErrDeploymentRetriesExhausted }
func (e *DeploymentExhaustedError) SafeDetail() string { return e.Error() }

// DeploymentWaitError reports platform backpressure: no mutation was submitted;
// retry at RetryAt. Key identifies the blocking state without ages or timestamps.
type DeploymentWaitError struct {
	Reason  string
	Key     string
	Detail  string
	RetryAt time.Time
}

func (e *DeploymentWaitError) Error() string           { return e.Reason + ": " + e.Detail }
func (e *DeploymentWaitError) DeploymentWaiting() bool { return true }
func (e *DeploymentWaitError) SafeDetail() string      { return e.Error() }

// SafeDetailer is implemented by backend errors whose Error text may contain
// platform data that must not reach users or logs verbatim.
type SafeDetailer interface {
	SafeDetail() string
}

// SafeDetail returns a short diagnostic suitable for API responses: the
// backend's safe detail when available, otherwise the neutral class.
func SafeDetail(err error) string {
	if err == nil {
		return ""
	}
	var d SafeDetailer
	if errors.As(err, &d) {
		return d.SafeDetail()
	}
	for _, class := range []error{ErrUnauthorized, ErrNotFound, ErrConflict, ErrUnavailable, ErrUnsupported, ErrResourceChangeCooldown, ErrRejected, ErrConfiguration} {
		if errors.Is(err, class) {
			return class.Error()
		}
	}
	return "network request or platform response failed"
}

// RetryAfterError carries the backend's earliest permitted retry time through
// wrapped errors. The deadline is not capped by local exponential backoff.
type RetryAfterError struct {
	Cause error
	At    time.Time
}

func (e *RetryAfterError) Error() string {
	return SafeDetail(e.Cause) + "; retry after " + e.At.UTC().Format(time.RFC3339)
}
func (e *RetryAfterError) Unwrap() error      { return e.Cause }
func (e *RetryAfterError) SafeDetail() string { return e.Error() }

// RetryAt returns the latest retry constraint in an error tree, including
// joined errors; a zero value means the backend supplied no retry constraint.
func RetryAt(err error) time.Time {
	var at time.Time
	if e, ok := err.(*RetryAfterError); ok {
		at = e.At
	}
	if e, ok := err.(*DeploymentWaitError); ok && e.RetryAt.After(at) {
		at = e.RetryAt
	}
	switch e := err.(type) {
	case interface{ Unwrap() []error }:
		for _, child := range e.Unwrap() {
			if t := RetryAt(child); t.After(at) {
				at = t
			}
		}
	case interface{ Unwrap() error }:
		if t := RetryAt(e.Unwrap()); t.After(at) {
			at = t
		}
	}
	return at
}
