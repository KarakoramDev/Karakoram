// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package orphanvolumes

import (
	"strings"
	"testing"
	"time"
)

func TestPendingDeleteReportsStuckWithoutReissuing(t *testing.T) {
	j, s, b, _, at := fixture()
	if err := j.Tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	*at = at.Add(16 * time.Minute)
	j = New(j.Options)
	if err := j.Tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	v := s.items["demom0v"]
	if !v.ProcessPending || b.deletes != 1 || !strings.Contains(v.LastError, "new-process") || !strings.Contains(v.LastError, "backend") {
		t.Fatalf("missing safe pending diagnostic: pending=%v deletes=%d error=%q", v.ProcessPending, b.deletes, v.LastError)
	}
	b.service = nil
	*at = v.NextAttemptAt
	if err := j.Tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(s.items) != 0 {
		t.Fatal("confirmed absence did not clear pending record")
	}
}
