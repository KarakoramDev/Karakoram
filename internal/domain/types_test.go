// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package domain

import "testing"

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
