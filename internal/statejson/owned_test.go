// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package statejson

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestReplaceOwnedClearsEmptyFields(t *testing.T) {
	type state struct {
		Text    string            `json:"text,omitempty"`
		Flag    bool              `json:"flag,omitempty"`
		Count   int               `json:"count,omitempty"`
		Pointer *int              `json:"pointer,omitempty"`
		List    []string          `json:"list,omitempty"`
		Map     map[string]string `json:"map,omitempty"`
		Nested  map[string]string `json:"nested,omitempty"`
		Ignored string            `json:"-"`
	}
	one := 1
	for _, tc := range []struct {
		name string
		set  state
	}{
		{"string", state{Text: "error"}},
		{"bool", state{Flag: true}},
		{"number", state{Count: 1}},
		{"pointer", state{Pointer: &one}},
		{"slice", state{List: []string{"a"}}},
		{"map", state{Map: map[string]string{"a": "b"}}},
		{"nested", state{Nested: map[string]string{"old": "value"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := map[string]json.RawMessage{"foreign": json.RawMessage(`{"keep":true}`)}
			root, err := ReplaceOwned(root, &tc.set)
			if err != nil {
				t.Fatal(err)
			}
			var loaded state
			data, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(data, &loaded); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(loaded, tc.set) {
				t.Fatalf("initial load = %+v", loaded)
			}
			root, err = ReplaceOwned(root, &state{})
			if err != nil {
				t.Fatal(err)
			}
			data, err = json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			loaded = state{}
			if err = json.Unmarshal(data, &loaded); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(loaded, state{}) || len(root) != 1 || string(root["foreign"]) != `{"keep":true}` {
				t.Fatalf("stale or foreign state: %s", data)
			}
		})
	}
}

func TestReplaceOwnedRejectsUnsupportedStateWithoutMutation(t *testing.T) {
	for _, input := range []any{nil, (*struct {
		Value string `json:"value"`
	})(nil), map[string]string{}, struct{ Value string }{}, struct {
		Value chan int `json:"value"`
	}{make(chan int)}} {
		root := map[string]json.RawMessage{"value": json.RawMessage(`"old"`)}
		if _, err := ReplaceOwned(root, input); err == nil {
			t.Fatalf("accepted unsupported state %T", input)
		}
		if string(root["value"]) != `"old"` {
			t.Fatal("failed replacement modified state")
		}
	}
}
