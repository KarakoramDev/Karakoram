// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestRestoreSourceInventoryAbsenceIsPermanentOnlyAfterSuccessfulRead(t *testing.T) {
	for _, unavailable := range []bool{false, true} {
		env, agent, _, op := prepareRestoreStartup(t, false)
		cp, err := loadCheckpoint(op)
		if err != nil {
			t.Fatal(err)
		}
		cp.Step, cp.Snapshot = "source-inventory", "2024-01-05T11:10:54Z"
		op.Checkpoint, err = json.Marshal(cp)
		if err != nil {
			t.Fatal(err)
		}
		agent.mu.Lock()
		agent.deleted = true
		agent.mu.Unlock()
		if unavailable {
			agent.requireConfig = true
		}
		result, err := RunRestore(t.Context(), env, op)
		var field *engine.ValidationError
		if err == nil || errors.As(err, &field) == unavailable {
			t.Fatalf("incorrect source absence classification: %+v %v", result, err)
		}
		if !unavailable && !strings.Contains(err.Error(), "select another successful backup") {
			t.Fatal("missing remediation", err)
		}
		starts := agent.startCount()
		if starts != 0 {
			t.Fatal("restore started without source", starts)
		}
	}
}
