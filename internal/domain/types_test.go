// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package domain

import (
	"testing"
	"time"
)

func TestNames(t *testing.T) {
	for _, name := range []string{"ab", "mongo1234567"} {
		if err := ValidateClusterName(name); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"a", "A1", "aa-b", "mongo12345678", "1abc", ""} {
		if err := ValidateClusterName(name); err == nil {
			t.Errorf("accepted %q", name)
		}
	}
	if ValidateAccountName("admin@example.org") != nil || ValidateAccountName("a b") == nil {
		t.Fatal("account validation")
	}
	if ValidateName("archive-one") != nil || ValidateName("../archive") == nil {
		t.Fatal("resource validation")
	}
	if ValidateDatabaseUserName("app-user") != nil || ValidateDatabaseUserName("bad\x00user") == nil {
		t.Fatal("database name validation")
	}
}

func TestOperationDefersPITRReanchorLifecycle(t *testing.T) {
	at := time.Now().UTC()
	for _, kind := range []string{OperationUpgradeAgents, OperationReplaceMember} {
		for _, phase := range []Phase{PhasePending, PhaseRunning, PhaseSucceeded, PhaseFailed, PhaseCancelled} {
			t.Run(kind+"/"+string(phase), func(t *testing.T) {
				active := phase == PhasePending || phase == PhaseRunning
				completed := at.Add(-PITRReanchorStabilityPeriod + time.Second)
				op := Operation{Kind: kind, Phase: phase, CompletedAt: &completed}
				if !OperationDefersPITRReanchor(op, at) {
					t.Fatal("maintenance did not defer reanchor")
				}
				completed = at.Add(-PITRReanchorStabilityPeriod)
				if got := OperationDefersPITRReanchor(op, at); got != active {
					t.Fatalf("at stability boundary got %v, want %v", got, active)
				}
			})
		}
	}
}
