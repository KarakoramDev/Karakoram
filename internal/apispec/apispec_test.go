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

func TestRequestIDRemainsATransportHeader(t *testing.T) {
	doc, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if doc.Components.Headers["RequestID"] == nil {
		t.Fatal("request correlation response header is undocumented")
	}
	for path, item := range doc.Paths.Map() {
		for _, parameter := range item.Parameters {
			if parameter.Value.Name == "X-Request-ID" {
				t.Errorf("%s declares transport RequestID as a path-wide parameter", path)
			}
		}
		for method, operation := range item.Operations() {
			for _, parameter := range operation.Parameters {
				if parameter.Value.Name == "X-Request-ID" {
					t.Errorf("%s %s declares transport RequestID as an operation parameter", method, path)
				}
			}
		}
	}
}
