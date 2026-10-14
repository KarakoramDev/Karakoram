// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package apispec

import "testing"

func TestEmbeddedSpecLoadsAndValidates(t *testing.T) {
	doc, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if doc.Paths.Find("/clusters") == nil {
		t.Fatal("embedded spec has no /clusters path")
	}
}
