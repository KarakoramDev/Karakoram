// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package nativeartifacts

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

// These components have no artifact delta in the v0.4.5 to v0.4.6 CP upgrade.
// Compare entire genuine released entries, not just their version strings.
func TestCPUpgradeUnchangedProfileProxyAndKeycloakLocks(t *testing.T) {
	raw, err := os.ReadFile("testdata/v045-unchanged-components.json")
	if err != nil {
		t.Fatal(err)
	}
	var legacy []Artifact
	if err := json.Unmarshal(raw, &legacy); err != nil {
		t.Fatal(err)
	}
	if len(legacy) != 8 {
		t.Fatal("incomplete unchanged-lock fixture", len(legacy))
	}
	for _, before := range legacy {
		t.Run(before.Name, func(t *testing.T) {
			after, err := Get(before.Name)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("component is no longer an unchanged-lock sanity case: %s", before.Name)
			}
		})
	}
}
