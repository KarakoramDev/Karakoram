// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package infra

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestVolumeLostError(t *testing.T) {
	lost := &VolumeLostError{Volume: "member-volume", ServiceID: "logical-id", Message: "original physical volume is absent; recover retained data or replace from healthy replicas"}
	wrapped := fmt.Errorf("observe runtime: %w", lost)
	var observed *VolumeLostError
	if !errors.Is(wrapped, ErrConflict) || !errors.As(wrapped, &observed) || observed.ServiceID != "logical-id" {
		t.Fatal("physical loss identity/class lost")
	}
	if detail := SafeDetail(wrapped); !strings.Contains(detail, "member-volume") || !strings.Contains(detail, "healthy replicas") {
		t.Fatal("actionable storage detail lost", detail)
	}
}
