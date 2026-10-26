// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import "testing"

func TestDefinitionsBindingPrefixNormalization(t *testing.T) {
	for _, test := range []struct{ prefix, base string }{
		{"x/y", "x/y/"}, {"/x/y", "x/y/"}, {"x/y/", "x/y/"}, {"/x/y/", "x/y/"}, {"/", ""}, {"", ""},
	} {
		key, err := definitionsKey(test.prefix, "uid", "daily")
		if err != nil || key != test.base+"uid/definitions/daily.json.gz" {
			t.Fatal(test.prefix, key, err)
		}
	}
	for _, prefix := range []string{"a//b", "..", "a/../b", "a/./b", "a\\b"} {
		if _, err := definitionsKey(prefix, "uid", "daily"); err == nil {
			t.Fatal("unsafe prefix accepted", prefix)
		}
	}
}
