// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestRecoveryAccountPlanPreservesDualPasswordsAndDrops(t *testing.T) {
	users := []myspec.RecoveryUser{{Name: "app", Current: "current-password", Secondary: "previous-password"}, {Name: "dropped", Current: "dropped-password"}, {Name: "single", Current: "single-password"}}
	plan, err := recoveryAccountPlan(users, map[string]bool{"app": true, "single": false})
	if err != nil || len(plan) != 2 {
		t.Fatal(plan, err)
	}
	if plan[0].Name != "app" || len(plan[0].Statements) != 3 || !strings.Contains(plan[0].Statements[0].prefix, "DISCARD OLD PASSWORD") || *plan[0].Statements[1].password != "previous-password" || *plan[0].Statements[2].password != "current-password" || !strings.Contains(plan[0].Statements[2].suffix, "RETAIN CURRENT PASSWORD") {
		t.Fatalf("dual passwords not reconstructed: %+v", plan[0])
	}
	if plan[1].Name != "single" || len(plan[1].Statements) != 1 || *plan[1].Statements[0].password != "single-password" {
		t.Fatal("single password reconstruction", plan[1])
	}
	for _, entry := range plan {
		if entry.Name == "dropped" {
			t.Fatal("PITR-dropped account recreated")
		}
		for _, statement := range entry.Statements {
			if strings.Contains(statement.prefix, "CREATE USER") || strings.Contains(statement.prefix, "password") {
				t.Fatal("credential leaked to SQL prefix or account recreated")
			}
		}
	}
}
func TestRecoveryAccountPlanRejectsSystemAndInvalidUsers(t *testing.T) {
	for _, user := range []myspec.RecoveryUser{{Name: "root", Current: "valid-password"}, {Name: "kk_agent", Current: "valid-password"}, {Name: "app", Current: "short"}, {Name: "app", Current: "valid-password", Secondary: "bad\npassword"}} {
		if _, err := recoveryAccountPlan([]myspec.RecoveryUser{user}, map[string]bool{user.Name: false}); err == nil {
			t.Fatal("unsafe recovery account accepted", user.Name)
		}
	}
	user := myspec.RecoveryUser{Name: "app", Current: "valid-password"}
	if _, err := recoveryAccountPlan([]myspec.RecoveryUser{user, user}, map[string]bool{"app": false}); err == nil {
		t.Fatal("duplicate account accepted")
	}
}
