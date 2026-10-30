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
		{name: "ambiguous direct and inverse lag", sets: []string{restored + ":1-9," + inverse + ":1," + sid + ":1-6", restored + ":1-9," + inverse + ":1," + sid + ":1-7", restored + ":1-9," + inverse + ":1," + sid + ":1-8"}, unavailable: true},
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

func TestRestoredInverseSIDLag(t *testing.T) {
	const state = "60dbb8ee-d39a-11f1-928e-f3cac64ca389"
	const active = "9f244711-2c65-ee0e-6d71-0c3539b35c76"
	const frozen = "de66497c-d38f-11f1-b461-b39f3845b42b"
	for _, last := range []string{"205", "207"} {
		healthy := GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, StateUUID: state, GTIDExecuted: active + ":1-206," + frozen + ":1-165"}
		lag := healthy
		lag.GTIDExecuted = active + ":1-" + last + "," + frozen + ":1-165"
		observations := map[string]GaleraStatusResponse{"a": healthy, "b": healthy, "c": lag}
		if err := CheckGTIDQuorum(observations); err != nil {
			t.Fatalf("nested inverse prefix %s: %v", last, err)
		}
		for _, bad := range []string{active + ":1-200:202-207," + frozen + ":1-165", active + ":1-207," + frozen + ":1-166", healthy.GTIDExecuted + ",aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1"} {
			lag.GTIDExecuted = bad
			observations["c"] = lag
			if err := CheckGTIDQuorum(observations); err == nil {
				t.Fatalf("unsafe history accepted: %s", bad)
			}
		}
	}
}

func TestNativePostBootstrapGTIDMappings(t *testing.T) {
	t.Run("consistent inverse mapping", func(t *testing.T) {
		const state = "8248544b-d3a9-11f1-86a2-b7f81642a5d4"
		const active = "7db7abb4-2c56-ee0e-795d-4807e9bd5a2b"
		const frozen = "90685df7-d39f-11f1-9568-8a2644f1f160:1-11"
		healthy := GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, StateUUID: state, GTIDExecuted: active + ":1-300," + frozen}
		observed := map[string]GaleraStatusResponse{"a": healthy, "b": healthy, "c": healthy}
		if err := CheckGTIDQuorum(observed); err != nil {
			t.Fatalf("identical native inverse histories rejected: %v", err)
		}
		healthy.GTIDExecuted = active + ":1-301," + frozen
		observed["c"] = healthy
		if err := CheckGTIDQuorum(observed); err != nil {
			t.Fatalf("nested native inverse prefixes rejected: %v", err)
		}
	})
	t.Run("mixed mapping is real divergence", func(t *testing.T) {
		const state = "d5aeb882-d447-11f1-96e2-f732d65208d9"
		const inverse = "2a51477d-2bb8-ee0e-691d-08cd29adf726"
		const frozen = "e419e04e-d43f-11f1-835a-d377c0fbcfd8:1-227"
		direct := GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, StateUUID: state, GTIDExecuted: state + ":1-366," + frozen}
		complemented := direct
		complemented.GTIDExecuted = inverse + ":1-368," + frozen
		observed := map[string]GaleraStatusResponse{"a": direct, "b": direct, "c": complemented}
		var conflict *GTIDQuorumError
		if err := CheckGTIDQuorum(observed); !errors.As(err, &conflict) || !conflict.Diverged || len(conflict.Members) != 1 || conflict.Members[0] != "c" {
			t.Fatalf("mixed native accounting not fenced: %v", err)
		}
		acknowledged := map[string]GaleraStatusResponse{"a": direct, "b": direct, "c": complemented}
		if err := CheckGTIDQuorumBaseline(observed, acknowledged); err != nil {
			t.Fatalf("unchanged acknowledged mixed accounting rejected: %v", err)
		}
		complemented.GTIDExecuted = inverse + ":1-369," + frozen
		observed["c"] = complemented
		if err := CheckGTIDQuorumBaseline(observed, acknowledged); !errors.As(err, &conflict) || !conflict.Diverged {
			t.Fatalf("new mixed accounting silently acknowledged: %v", err)
		}
	})
}
