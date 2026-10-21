// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rbac

import "testing"

func TestDataMcpDefaultPermissions(t *testing.T) {
	e, _ := fixture(t)
	for _, role := range []string{"role:viewer", "role:operator", "role:admin", "user:admin"} {
		for _, action := range []string{"read", "write"} {
			want := role == "role:admin" || role == "user:admin"
			if got := e.Can([]string{role}, "cluster-data", action, "prod/orders"); got != want {
				t.Errorf("%s cluster-data:%s = %v, want %v", role, action, got, want)
			}
		}
	}
	for _, action := range []string{"read", "write"} {
		if _, err := parse("p, user:alice, cluster-data, " + action + ", prod/orders, allow"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := parse("p, user:alice, cluster-data, update, prod/orders, allow"); err == nil {
		t.Fatal("accepted cluster lifecycle action for data")
	}
}
