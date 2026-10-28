// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
)

type queryFailureKind uint8

const (
	queryFailureUnknown queryFailureKind = iota
	queryFailureInvalid
	queryFailureExists
	queryFailureMissing
)

// queryFailure keeps only a recognized status code and a closed classification.
// Native messages can contain query inputs and must never escape the decoder.
type queryFailure struct {
	code              string
	kind              queryFailureKind
	passwordUnchanged bool
}

func (*queryFailure) Error() string { return "Neo4j query failed" }

func classifyQueryError(code, message string) error {
	failure := &queryFailure{code: nativeStatusCode(code)}
	switch code {
	case "Neo.ClientError.General.InvalidArguments", "Neo.ClientError.Statement.ArgumentError":
		failure.code = code
		failure.kind = queryFailureInvalid
		// Neo4j rejects setting an administrator password to its current value.
		// Only this exact native diagnostic proves the recovery postcondition;
		// other argument errors must still fail closed.
		failure.passwordUnchanged = code == "Neo.ClientError.Statement.ArgumentError" &&
			message == "Failed to alter the specified user 'neo4j': Old password and new password cannot be the same."
		lower := strings.ToLower(message)
		if strings.Contains(lower, "already exists") {
			failure.kind = queryFailureExists
		} else if strings.Contains(lower, "does not exist") || strings.Contains(lower, "not found") {
			failure.kind = queryFailureMissing
		}
	case "Neo.ClientError.Security.UserAlreadyExists":
		failure.code, failure.kind = code, queryFailureExists
	case "Neo.ClientError.Security.UserNotFound":
		failure.code, failure.kind = code, queryFailureMissing
	case "Neo.ClientError.Statement.SyntaxError", "Neo.ClientError.Statement.SemanticError":
		failure.code, failure.kind = code, queryFailureInvalid
	}
	return failure
}

// Keep a closed set: even a syntactically plausible remote code can contain
// application data. Unknown codes and all native message text stay private.
func nativeStatusCode(code string) string {
	switch code {
	case "Neo.ClientError.General.InvalidArguments",
		"Neo.ClientError.Statement.ArgumentError",
		"Neo.ClientError.Statement.SyntaxError",
		"Neo.ClientError.Statement.SemanticError",
		"Neo.ClientError.Security.UserAlreadyExists",
		"Neo.ClientError.Security.UserNotFound",
		"Neo.ClientError.Security.Unauthorized",
		"Neo.ClientError.Security.Forbidden",
		"Neo.ClientError.Security.CredentialsExpired",
		"Neo.ClientError.Security.AuthenticationRateLimit",
		"Neo.ClientError.Database.DatabaseNotFound",
		"Neo.ClientError.Database.DatabaseNotAvailable",
		"Neo.TransientError.General.DatabaseUnavailable",
		"Neo.TransientError.Database.DatabaseUnavailable",
		"Neo.ClientError.Cluster.NotALeader",
		"Neo.ClientError.General.ForbiddenOnReadOnlyDatabase",
		"Neo.TransientError.General.OutOfMemoryError",
		"Neo.DatabaseError.General.UnknownError":
		return code
	default:
		return "unrecognized"
	}
}

type requestFailure struct {
	class  string
	status int
}

func (e *requestFailure) Error() string { return "Neo4j request failed" }

func classifyTransportError(err error) error {
	class := "transport"
	var timeout net.Error
	if errors.Is(err, context.Canceled) {
		class = "canceled"
	} else if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout()) {
		class = "timeout"
	}
	return &requestFailure{class: class}
}

// recoveryFailureDiagnostic is the only recovery diagnostic escaping to the
// agent log. Neither the original error string nor native message is included.
func recoveryFailureDiagnostic(err error) error {
	const prefix = "Neo4j credential recovery failed"
	var native *queryFailure
	if errors.As(err, &native) {
		class := "native-error"
		switch native.kind {
		case queryFailureInvalid:
			class = "invalid-argument"
		case queryFailureExists:
			class = "user-exists"
		case queryFailureMissing:
			class = "user-missing"
		}
		switch native.code {
		case "Neo.ClientError.Cluster.NotALeader":
			class = "not-leader"
		case "Neo.ClientError.Database.DatabaseNotAvailable", "Neo.TransientError.General.DatabaseUnavailable", "Neo.TransientError.Database.DatabaseUnavailable":
			class = "database-unavailable"
		case "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase":
			class = "read-only"
		case "Neo.ClientError.Security.CredentialsExpired":
			class = "password-change-required"
		}
		return fmt.Errorf("%s: category=native code=%s class=%s", prefix, native.code, class)
	}
	var request *requestFailure
	if errors.As(err, &request) {
		return fmt.Errorf("%s: category=request class=%s httpStatus=%d", prefix, request.class, request.status)
	}
	if errors.Is(err, errCredentials) {
		return fmt.Errorf("%s: category=request class=authentication httpStatus=401", prefix)
	}
	return fmt.Errorf("%s: category=unknown", prefix)
}
func userErrorResponse(err error) (int, string) {
	var failure *queryFailure
	if errors.As(err, &failure) {
		switch failure.kind {
		case queryFailureExists:
			return http.StatusConflict, "Neo4j user already exists"
		case queryFailureMissing:
			return http.StatusNotFound, "Neo4j user does not exist"
		case queryFailureInvalid:
			return http.StatusBadRequest, "invalid Neo4j user request"
		}
	}
	return http.StatusServiceUnavailable, "Neo4j user action failed"
}
