// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mysql

import (
	"strings"
	"testing"
)

func TestAccessNames(t *testing.T) {
	for _, name := range []string{"app", "_app", "A1", strings.Repeat("a", 32)} {
		if err := ValidateUserName(name); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	for _, name := range []string{"", "root", "ROOT", "kk_user", "KK_user", "mysql", "sys", "information_schema", "performance_schema", "1user", "user-name", "a$", strings.Repeat("a", 33)} {
		if ValidateUserName(name) == nil {
			t.Errorf("accepted user %q", name)
		}
	}
	for _, name := range []string{"app_db$", strings.Repeat("a", 64)} {
		if err := ValidateDatabaseName(name); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"mysql", "_karakoram", "_karakoram_extra", "kk_data", strings.Repeat("a", 65), "app`x", "app%"} {
		if ValidateDatabaseName(name) == nil {
			t.Errorf("accepted database %q", name)
		}
	}
}
func TestAccessPassword(t *testing.T) {
	for _, password := range []string{"short", strings.Repeat("a", 1025), "password\x00withnul", string([]byte{0xff}) + "longpasswordhere"} {
		if ValidatePassword(password) == nil {
			t.Fatal("accepted invalid password")
		}
	}
	if err := ValidatePassword("quoted'\\utf8你好-password"); err != nil {
		t.Fatal(err)
	}
	password, err := GeneratePassword()
	if err != nil {
		t.Fatal(err)
	}
	generatedPasswordContract.Assert(t, "standalone generated SQL password", password)
	if len(password) != 40 || ValidatePassword(password) != nil {
		t.Fatal("invalid generated password")
	}
}
func TestAccessGrants(t *testing.T) {
	grant := Grant{User: "app", Database: "app_db", Privileges: []string{"SELECT", "CREATE_VIEW"}}
	if err := ValidateGrant(grant); err != nil {
		t.Fatal(err)
	}
	for _, privileges := range [][]string{nil, {"ALL", "DROP"}, {"SELECT", "SELECT"}, {"SUPER"}} {
		grant.Privileges = privileges
		if ValidateGrant(grant) == nil {
			t.Fatal("accepted unsafe grant")
		}
	}
}
