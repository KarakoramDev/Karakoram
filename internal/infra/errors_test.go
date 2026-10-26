// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

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

func TestRetryAtPreservesWrappedJoinedConstraints(t *testing.T) {
	now := time.Now()
	early := &infra.RetryAfterError{Cause: detailedError{}, At: now.Add(time.Minute)}
	late := &infra.RetryAfterError{Cause: infra.ErrUnavailable, At: now.Add(time.Hour)}
	err := fmt.Errorf("private context: %w", errors.Join(early, errors.New("private secret"), late))
	if got := infra.RetryAt(err); !got.Equal(late.At) {
		t.Fatalf("reset %v want %v", got, late.At)
	}
	if !errors.Is(err, infra.ErrUnavailable) {
		t.Fatal("lost unavailable classification")
	}
	detail := infra.SafeDetail(early)
	if strings.Contains(detail, "sensitive") || !strings.Contains(detail, early.At.UTC().Format(time.RFC3339)) {
		t.Fatalf("unsafe or unactionable detail: %s", detail)
	}
	if !infra.RetryAt(nil).IsZero() || !infra.RetryAt(infra.ErrUnavailable).IsZero() {
		t.Fatal("invented retry deadline")
	}
}

func TestDeploymentWaitPreservesJoinedRetryDeadline(t *testing.T) {
	at := time.Now().Add(time.Hour)
	wait := &infra.DeploymentWaitError{Reason: "DeploymentSlot", Key: "service/process/RUNNING", Detail: "process age 15s", RetryAt: at}
	err := fmt.Errorf("deploy: %w", errors.Join(wait, &infra.RetryAfterError{Cause: infra.ErrUnavailable, At: at.Add(-time.Minute)}))
	if !wait.DeploymentWaiting() || infra.SafeDetail(wait) != wait.Error() || !infra.RetryAt(err).Equal(at) {
		t.Fatalf("typed wait contract lost: %v", err)
	}
}
