// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package domain

import "fmt"

// Stable machine-readable reasons reported in API error details. Clients may
// branch on them; messages are for humans and may change.
const (
	ReasonRequired      = "required"       // a mandatory field or parameter is absent
	ReasonUnknownField  = "unknown_field"  // the request names a field the API does not define
	ReasonInvalid       = "invalid"        // malformed value or wrong type
	ReasonOutOfRange    = "out_of_range"   // number, length or time outside the accepted range
	ReasonUnsupported   = "unsupported"    // valid in general, not supported here (engine, topology, licence)
	ReasonConflict      = "conflict"       // fields that cannot be combined
	ReasonReused        = "reused"         // a new secret equals the one it replaces
	ReasonNotFound      = "not_found"      // a referenced resource does not exist
	ReasonAlreadyExists = "already_exists" // a resource with this name already exists
	ReasonInvalidState  = "invalid_state"  // the resource's current state does not allow the request
	ReasonProtected     = "protected"      // the resource is managed by Karakoram and cannot be changed
)

// FieldError rejects one request field or parameter. Field is the JSON path
// (for example spec.components.engine.replicas) or the query/path parameter
// name; Message is caller-safe and names the field. The HTTP layer maps it to
// 400 invalid_request with details {field, reason}.
type FieldError struct {
	Field   string
	Reason  string
	Message string
	// Err optionally classifies the error for errors.Is (for example a
	// package's ErrInvalid sentinel); it is never shown to callers.
	Err error
}

func (e *FieldError) Error() string { return e.Message }
func (e *FieldError) Unwrap() error { return e.Err }

// InvalidField returns a FieldError whose message is the formatted text.
// Messages should start with the field name so they are useful on their own.
func InvalidField(field, reason, format string, args ...any) *FieldError {
	return &FieldError{Field: field, Reason: reason, Message: fmt.Sprintf(format, args...)}
}

// ResourceError reports a missing or conflicting named resource. The HTTP
// layer maps Reason not_found to 404 and every other reason to 409, with
// details {resource, name, reason} plus State when known.
type ResourceError struct {
	Resource string // API resource kind, for example "backup", "backupStorage"
	Name     string
	Reason   string
	State    string // current state for invalid_state, for example "Pending"
	Message  string
	// Err optionally preserves a sentinel (store.ErrNotFound, engine.ErrConflict)
	// so existing errors.Is checks keep working.
	Err error
}

func (e *ResourceError) Error() string { return e.Message }
func (e *ResourceError) Unwrap() error { return e.Err }

// NotFound reports that the named resource does not exist; sentinel may be nil.
func NotFound(resource, name string, sentinel error) *ResourceError {
	return &ResourceError{Resource: resource, Name: name, Reason: ReasonNotFound, Err: sentinel,
		Message: fmt.Sprintf("%s %q not found", resource, name)}
}

// Conflict reports that the request conflicts with the named resource; sentinel may be nil.
func Conflict(resource, name, reason string, sentinel error, format string, args ...any) *ResourceError {
	return &ResourceError{Resource: resource, Name: name, Reason: reason, Err: sentinel,
		Message: fmt.Sprintf(format, args...)}
}
