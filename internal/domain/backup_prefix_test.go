// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package domain

import "testing"

func TestBackupStoragePrefix(t *testing.T) {
	for _, test := range []struct {
		prefix, want string
		valid        bool
	}{
		{"", "", true}, {"/", "", true}, {"///", "", true},
		{"x/y", "x/y", true}, {"/x/y", "x/y", true},
		{"x/y/", "x/y", true}, {"/x/y/", "x/y", true},
		{"a//b", "a//b", false}, {"..", "..", false},
		{"a/../b", "a/../b", false}, {"a/./b", "a/./b", false},
		{"a\\b", "a\\b", false},
	} {
		t.Run(test.prefix, func(t *testing.T) {
			if got := BackupStoragePrefix(test.prefix); got != test.want {
				t.Fatalf("namespace = %q, want %q", got, test.want)
			}
			if err := ValidateBackupStoragePrefix(test.prefix); (err == nil) != test.valid {
				t.Fatalf("validation = %v, valid = %t", err, test.valid)
			}
			want := "karakoram/demo-uid"
			if test.want != "" {
				want = test.want + "/" + want
			}
			if got := BackupObjectPrefix(test.prefix, "karakoram/demo-uid"); got != want {
				t.Fatalf("object prefix = %q, want %q", got, want)
			}
		})
	}
}

func TestPersistedEnginePrefixMappings(t *testing.T) {
	for _, test := range []struct{ prefix, postgres, elasticsearch string }{
		{"a//b", "a/b", "a/b"},
		{"a/./b", "a/b", "a/./b"},
		{"a/../b", "b", "a/../b"},
		{"/backups///nested/", "backups/nested", "backups/nested"},
	} {
		if got := PostgresObjectPrefix(test.prefix, "postgres/ns/demo-uid"); got != test.postgres+"/postgres/ns/demo-uid" {
			t.Fatal("PostgreSQL historical namespace changed", test.prefix, got)
		}
		if got := ElasticsearchObjectPrefix(test.prefix, "ns/demo/uid"); got != test.elasticsearch+"/ns/demo/uid" {
			t.Fatal("Elasticsearch historical namespace changed", test.prefix, got)
		}
		if err := ValidateBackupStoragePrefix(test.prefix); err == nil {
			t.Fatal("new registration accepted an ambiguous legacy prefix", test.prefix)
		}
	}
}
