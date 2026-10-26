// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package statejson preserves foreign workflow keys while replacing owned state.
package statejson

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

var ownedKeys sync.Map // reflect.Type -> []string

// ReplaceOwned replaces all JSON fields owned by a flat, explicitly tagged
// struct, including fields omitted by omitempty or omitzero. Other root keys
// are preserved. Nested objects are replaced whole, not recursively merged.
// The input map is modified only after serialization and ownership validation.
func ReplaceOwned(existing map[string]json.RawMessage, state any) (map[string]json.RawMessage, error) {
	t := reflect.TypeOf(state)
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nil || t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("owned JSON state must be a struct")
	}
	cached, ok := ownedKeys.Load(t)
	if !ok {
		keys := make([]string, 0, t.NumField())
		seen := make(map[string]bool, t.NumField())
		for i := range t.NumField() {
			field := t.Field(i)
			if field.PkgPath != "" {
				continue
			}
			name := strings.SplitN(field.Tag.Get("json"), ",", 2)[0]
			if name == "-" {
				continue
			}
			if name == "" || seen[name] {
				return nil, fmt.Errorf("owned JSON state %s requires unique explicit field tags", t)
			}
			seen[name] = true
			keys = append(keys, name)
		}
		cached, _ = ownedKeys.LoadOrStore(t, keys)
	}
	data, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	var fresh map[string]json.RawMessage
	if err = json.Unmarshal(data, &fresh); err != nil {
		return nil, err
	}
	if fresh == nil {
		return nil, fmt.Errorf("owned JSON state must not be null")
	}
	if existing == nil {
		existing = make(map[string]json.RawMessage, len(fresh))
	}
	for _, key := range cached.([]string) {
		delete(existing, key)
	}
	for key, value := range fresh {
		existing[key] = value
	}
	return existing, nil
}
