// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestPatchPreservesOptionalCollections(t *testing.T) {
	for _, tc := range []struct {
		name      string
		populated bool
		empty     bool
		patch     string
		changed   bool
		clear     bool
	}{
		{name: "absent no-op", patch: `{"spec":{}}`},
		{name: "absent parameter change", patch: `{"spec":{"engineOptions":{"parameters":{"max_connections":"101"}}}}`, changed: true},
		{name: "populated parameter change", populated: true, patch: `{"spec":{"engineOptions":{"parameters":{"max_connections":"101"}}}}`, changed: true},
		{name: "explicit empty clears populated", populated: true, patch: `{"spec":{"backup":{"schedules":[]},"exposure":{"allowlist":[]}}}`, changed: true, clear: true},
		{name: "empty no-op", empty: true, patch: `{"spec":{}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, token := testServer(t)
			before, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
			if err != nil {
				t.Fatal(err)
			}
			before.Spec.EngineOptions = json.RawMessage(`{"parameters":{"max_connections":"100"}}`)
			if tc.populated {
				storage := domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: "https://s3.example.test", Region: "region", Bucket: "objects"}
				if err = s.Store.CreateBackupStorage(t.Context(), &storage); err != nil {
					t.Fatal(err)
				}
				before.Spec.Backup.Schedules = []domain.BackupSchedule{{Name: "daily", Cron: "0 0 * * *", Type: "logical", Storage: "archive", RetainCount: 2, Enabled: false}}
				before.Spec.Exposure.Allowlist = []string{"2001:db8::/32"}
			} else if tc.empty {
				before.Spec.Backup.Schedules = []domain.BackupSchedule{}
				before.Spec.Exposure.Allowlist = []string{}
			}
			if err = s.Store.UpdateClusterSpec(t.Context(), before.Ref(), before.Spec); err != nil {
				t.Fatal(err)
			}
			before, err = s.Store.GetCluster(t.Context(), before.Ref())
			if err != nil {
				t.Fatal(err)
			}
			w := call(s, "PATCH", "/api/v1/clusters/prod", tc.patch, token)
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			after, err := s.Store.GetCluster(t.Context(), before.Ref())
			if err != nil {
				t.Fatal(err)
			}
			want := before.Spec
			if tc.clear {
				want.Backup.Schedules = []domain.BackupSchedule{}
				want.Exposure.Allowlist = []string{}
			} else if tc.changed {
				want.EngineOptions = json.RawMessage(`{"parameters":{"max_connections":"101"}}`)
			}
			if !reflect.DeepEqual(after.Spec, want) {
				t.Errorf("PATCH changed omitted spec fields: got %+v, want %+v", after.Spec, want)
			}
			generation := before.Generation
			if tc.changed {
				generation++
			}
			if after.Generation != generation {
				t.Errorf("generation = %d, want %d", after.Generation, generation)
			}
			events := auditEvents(t, s, "ClusterSpecChangeRequested")
			if len(events) != 1 {
				t.Fatalf("audit events = %d, want 1", len(events))
			}
			var details specDetails
			if err = json.Unmarshal(events[0].Details, &details); err != nil {
				t.Fatal(err)
			}
			if !tc.clear {
				if tc.changed {
					if len(details.Changes) != 1 || details.Changes[0].Path != "engineOptions.parameters.max_connections" || details.Changes[0].Before != "100" || details.Changes[0].After != "101" {
						t.Fatal("unrelated collection change in audit", string(events[0].Details))
					}
				} else if len(details.Changes) != 0 {
					t.Fatal("no-op PATCH reported changes", string(events[0].Details))
				}
			}
			// Response normalization must not alter the persisted presence semantics.
			w = call(s, "GET", "/api/v1/clusters/prod", "", token)
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			var out struct {
				Spec struct {
					Backup struct {
						Schedules []any `json:"schedules"`
					} `json:"backup"`
					Exposure struct {
						Allowlist []string `json:"allowlist"`
					} `json:"exposure"`
				} `json:"spec"`
			}
			if err = json.Unmarshal(w.Body.Bytes(), &out); err != nil {
				t.Fatal(err)
			}
			if out.Spec.Backup.Schedules == nil || out.Spec.Exposure.Allowlist == nil {
				t.Fatal("GET omitted normalized arrays", w.Body.String())
			}
		})
	}
}
