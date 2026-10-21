// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"errors"
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
	code string
	kind queryFailureKind
}

func (*queryFailure) Error() string { return "Neo4j query failed" }

func classifyQueryError(code, message string) error {
	failure := &queryFailure{}
	switch code {
	case "Neo.ClientError.General.InvalidArguments", "Neo.ClientError.Statement.ArgumentError":
		failure.code = code
		failure.kind = queryFailureInvalid
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
