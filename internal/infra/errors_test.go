// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type detailedError struct{}

func (detailedError) Error() string      { return "sensitive diagnostic" }
func (detailedError) SafeDetail() string { return "safe diagnostic" }
func (detailedError) Unwrap() error      { return infra.ErrUnavailable }

func TestSafeDetail(t *testing.T) {
	if got := infra.SafeDetail(nil); got != "" {
		t.Fatal(got)
	}
	if got := infra.SafeDetail(errors.New("secret credential")); got != "network request or platform response failed" {
		t.Fatal(got)
	}
	for _, class := range []error{infra.ErrNotFound, infra.ErrConflict, infra.ErrRejected, infra.ErrUnauthorized, infra.ErrUnavailable, infra.ErrUnsupported, infra.ErrResourceChangeCooldown} {
		if got := infra.SafeDetail(fmt.Errorf("private context: %w", class)); got != class.Error() {
			t.Fatalf("%v: %q", class, got)
		}
	}
	if got := infra.SafeDetail(fmt.Errorf("wrapped: %w", detailedError{})); got != "safe diagnostic" {
		t.Fatal(got)
	}
	if got := infra.SafeDetail(&infra.RejectedError{Code: "private", Message: "private"}); got != infra.ErrRejected.Error() {
		t.Fatal(got)
	}
}
func TestRejectedError(t *testing.T) {
	for _, tc := range []struct {
		err  *infra.RejectedError
		want string
	}{{&infra.RejectedError{Message: "message"}, "message"}, {&infra.RejectedError{Code: "code", Message: "message"}, "code: message"}} {
		if tc.err.Error() != tc.want || !errors.Is(tc.err, infra.ErrRejected) || errors.Is(tc.err, infra.ErrConflict) {
			t.Fatalf("rejected error: %v", tc.err)
		}
	}
}
