// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package myspec

import (
	"errors"
	"testing"
)

func TestCheckGTIDQuorum(t *testing.T) {
	const sid = "11111111-2222-3333-4444-555555555555"
	const restored = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	const inverse = "eeeeeeee-dddd-cccc-bbbb-aaaaaaaaaaaa"
	for _, tc := range []struct {
		name        string
		sets        []string
		unavailable bool
		diverged    bool
	}{
		{name: "equal", sets: []string{sid + ":1-7", sid + ":1-7", sid + ":1-7"}},
		{name: "active write lag", sets: []string{sid + ":1-6", sid + ":1-7", sid + ":1-8"}},
		{name: "interior hole is not lag", sets: []string{sid + ":1-7", sid + ":1-7", sid + ":1-4:6-7"}, diverged: true},
		{name: "shared hole with prefix lag", sets: []string{sid + ":1-4:6-7", sid + ":1-4:6-8", sid + ":1-4:6-9"}},
		{name: "inverse mapping", sets: []string{sid + ":1-7", sid + ":1-7", sid + ":1-6," + inverse + ":1"}, diverged: true},
		{name: "inverse only", sets: []string{sid + ":1-7", sid + ":1-7", inverse + ":1"}, diverged: true},
		{name: "legitimate restored namespaces", sets: []string{restored + ":1-9," + inverse + ":1," + sid + ":1-6", restored + ":1-9," + inverse + ":1," + sid + ":1-7", restored + ":1-9," + inverse + ":1," + sid + ":1-8"}},
		{name: "different restored history", sets: []string{restored + ":1-9," + sid + ":1-7", restored + ":1-9," + sid + ":1-7", sid + ":1-8"}, diverged: true},
		{name: "no majority chain", sets: []string{sid + ":1", restored + ":1", inverse + ":1"}, unavailable: true},
		{name: "singleton baseline", sets: []string{restored + ":1-9," + inverse + ":1"}},
		{name: "invalid GTIDs", sets: []string{"invalid", "invalid", sid + ":1"}, unavailable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observed := map[string]GaleraStatusResponse{}
			for i, set := range tc.sets {
				observed[string(rune('a'+i))] = GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, StateUUID: sid, GTIDExecuted: set}
			}
			err := CheckGTIDQuorum(observed)
			if !tc.unavailable && !tc.diverged {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var conflict *GTIDQuorumError
			if !errors.As(err, &conflict) || conflict.Diverged != tc.diverged || len(conflict.Members) == 0 {
				t.Fatalf("unexpected comparison: %#v", err)
			}
			if tc.diverged && (len(conflict.Members) != 1 || conflict.Members[0] != "c") {
				t.Fatalf("affected members: %v", conflict.Members)
			}
		})
	}
}

func TestCheckGTIDQuorumUnavailableAndForeignComponent(t *testing.T) {
	const sid = "11111111-2222-3333-4444-555555555555"
	healthy := GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, StateUUID: sid, GTIDExecuted: sid + ":1-7"}
	observed := map[string]GaleraStatusResponse{"a": healthy, "b": {}, "c": {}}
	if err := CheckGTIDQuorum(observed); err == nil {
		t.Fatal("minority became baseline")
	}
	observed["b"] = healthy
	if err := CheckGTIDQuorum(observed); err != nil {
		t.Fatal(err)
	}
	foreign := healthy
	foreign.StateUUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	observed["c"] = foreign
	var conflict *GTIDQuorumError
	if err := CheckGTIDQuorum(observed); !errors.As(err, &conflict) || !conflict.Diverged {
		t.Fatalf("foreign component: %v", err)
	}
}

func TestCheckGTIDQuorumRejectsNodeLocalSuperset(t *testing.T) {
	const current = "11111111-2222-3333-4444-555555555555"
	const local = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	for _, extra := range []string{local + ":1", "eeeeeeee-dddd-cccc-bbbb-aaaaaaaaaaaa:1"} {
		healthy := GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, StateUUID: current, GTIDExecuted: current + ":1-7"}
		localExtra := healthy
		localExtra.GTIDExecuted += "," + extra
		var conflict *GTIDQuorumError
		err := CheckGTIDQuorum(map[string]GaleraStatusResponse{"a": healthy, "b": healthy, "c": localExtra})
		if !errors.As(err, &conflict) || !conflict.Diverged || len(conflict.Members) != 1 || conflict.Members[0] != "c" {
			t.Fatalf("local superset not rejected: %v", err)
		}
	}
}
