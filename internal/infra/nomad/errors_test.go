// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestEnforceIndexConflictClassification(t *testing.T) {
	for _, tt := range []struct {
		name    string
		status  int
		message string
		want    error
	}{
		{"existing job", 500, "Enforcing job modify index 0: job already exists\n", infra.ErrConflict},
		{"changed index", 500, "Enforcing job modify index 17: job exists with conflicting job modify index: 19\n", infra.ErrConflict},
		{"missing job", 500, "Enforcing job modify index 17: job does not exist\n", infra.ErrConflict},
		{"ordinary server failure", 500, "internal server error", infra.ErrUnavailable},
		{"unrecognized suffix", 500, "Enforcing job modify index 17: server unavailable", infra.ErrUnavailable},
		{"extra diagnostics", 500, "Enforcing job modify index 0: job already exists; unrelated failure", infra.ErrUnavailable},
		{"not numeric", 500, "Enforcing job modify index unknown: job already exists", infra.ErrUnavailable},
		{"other server status", 503, "Enforcing job modify index 0: job already exists", infra.ErrUnavailable},
		{"bad request unchanged", 400, "Enforcing job modify index 0: job already exists", infra.ErrRejected},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := responseError(tt.status, "PUT", []byte(tt.message), "")
			if !errors.Is(err, tt.want) {
				t.Fatalf("classification = %v; want %v", err, tt.want)
			}
		})
	}
}
