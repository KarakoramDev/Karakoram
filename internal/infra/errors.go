// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra

import (
	"errors"
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
	for _, class := range []error{ErrUnauthorized, ErrNotFound, ErrConflict, ErrUnavailable, ErrUnsupported, ErrResourceChangeCooldown, ErrRejected} {
		if errors.Is(err, class) {
			return class.Error()
		}
	}
	return "network request or platform response failed"
}
