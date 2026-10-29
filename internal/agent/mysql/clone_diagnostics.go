// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"errors"
	"fmt"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// Preserve the original error chain for local recovery decisions, while exposing
// only a trusted stage and native error number across the agent/CP boundary.
type cloneActionError struct {
	stage string
	cause error
}

func (e *cloneActionError) Error() string { return e.cause.Error() }
func (e *cloneActionError) Unwrap() error { return e.cause }

func cloneActionDiagnostic(err error) string {
	stage := "request" // Strict JSON decoding can fail before CloneGroup runs.
	var action *cloneActionError
	if errors.As(err, &action) {
		switch action.stage {
		case "request", "authorization", "receipt", "receipt-recovery", "recipient-preflight", "donor-probe", "native-baseline", "donor-configuration", "receipt-write", "native-clone", "completion":
			stage = action.stage
		default:
			stage = "unknown"
		}
	}
	message := "MySQL clone failed: stage=" + stage
	var native *mysqldriver.MySQLError
	if errors.As(err, &native) {
		return fmt.Sprintf("%s; MySQL %d", message, native.Number)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return message + "; deadline exceeded"
	}
	if errors.Is(err, context.Canceled) {
		return message + "; canceled"
	}
	// Do not include driver messages, SQL, receipt paths, peer addresses or log
	// tails: any can contain credentials, including obsolete/unknown passwords.
	return message
}
