// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra

import (
	"errors"
	"strings"
	"testing"
)

func TestDeploymentExhaustedErrorIsNonRetryable(t *testing.T) {
	err := &DeploymentExhaustedError{Service: "member", Version: "version", Attempts: 4, LastError: "sanitized platform failure"}
	if !errors.Is(err, ErrDeploymentRetriesExhausted) || errors.Is(err, ErrConflict) || errors.Is(err, ErrUnavailable) || !RetryAt(err).IsZero() {
		t.Fatalf("terminal deployment error classified as retryable: %v", err)
	}
	for _, detail := range []string{"DeploymentRetriesExhausted", "member", "version", "4 attempts", "sanitized platform failure", "operator re-arm"} {
		if !strings.Contains(SafeDetail(err), detail) {
			t.Fatalf("terminal diagnostic omitted %q: %v", detail, err)
		}
	}
}
