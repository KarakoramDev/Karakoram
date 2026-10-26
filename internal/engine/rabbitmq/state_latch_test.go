// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestStateSaveClearsOmittedOwnedFields(t *testing.T) {
	for _, tc := range []struct {
		name  string
		set   func(*internalState)
		clear func(*internalState)
	}{
		{"pending", func(s *internalState) { s.Pending = &pending{Action: "deploy"} }, func(s *internalState) { s.Pending = nil }},
		{"retry", func(s *internalState) { s.Retry = &pending{Action: "deploy"} }, func(s *internalState) { s.Retry = nil }},
		{"platformRetries", func(s *internalState) { s.PlatformRetries = map[string]*platformRetry{"deploy": {Failures: 1}} }, func(s *internalState) { s.PlatformRetries = nil }},
		{"cloneImported", func(s *internalState) { s.CloneImported = true }, func(s *internalState) { s.CloneImported = false }},
		{"rotationSecrets", func(s *internalState) { s.RotationSecrets = []string{"old-secret-key"} }, func(s *internalState) { s.RotationSecrets = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &domain.Cluster{Internal: json.RawMessage(`{"foreign":{"checkpoint":"keep"}}`)}
			s := &internalState{ConfigVersion: 1, DistributionSet: true}
			tc.set(s)
			if err := s.save(c); err != nil {
				t.Fatal(err)
			}
			loaded, err := loadState(c)
			if err != nil {
				t.Fatal(err)
			}
			tc.clear(loaded)
			if err = loaded.save(c); err != nil {
				t.Fatal(err)
			}
			got, err := loadState(c)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, loaded) {
				t.Fatalf("cleared state resurrected: got %+v, want %+v", got, loaded)
			}
			var root map[string]json.RawMessage
			if err = json.Unmarshal(c.Internal, &root); err != nil {
				t.Fatal(err)
			}
			if _, exists := root[tc.name]; exists {
				t.Fatalf("omitted key %s survived: %s", tc.name, c.Internal)
			}
			if string(root["foreign"]) != `{"checkpoint":"keep"}` {
				t.Fatalf("foreign state changed: %s", c.Internal)
			}
		})
	}
}
