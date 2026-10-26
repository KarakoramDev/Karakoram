// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/apispec"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/getkin/kin-openapi/openapi3"
)

func TestSpecDiffFieldsAndArrays(t *testing.T) {
	for _, tc := range []struct {
		name, before, after string
		paths, summaries    []string
	}{
		{"scalar", `{"version":"7"}`, `{"version":"8"}`, []string{"version"}, []string{"version: 7 → 8"}},
		{"nested", `{"components":{"engine":{"resources":{"maxDisk":3}}}}`, `{"components":{"engine":{"resources":{"maxDisk":2}}}}`, []string{"components.engine.resources.maxDisk"}, []string{"components.engine.resources.maxDisk: 3 → 2"}},
		{"named", `{"backup":{"schedules":[{"name":"nightly","retainCount":7},{"name":"old"}]}}`, `{"backup":{"schedules":[{"name":"hourly"},{"name":"nightly","retainCount":14}]}}`, []string{"backup.schedules[hourly]", "backup.schedules[nightly].retainCount", "backup.schedules[old]"}, []string{"backup.schedules[hourly]: added", "backup.schedules[nightly].retainCount: 7 → 14", "backup.schedules[old]: removed"}},
		{"scalar array", `{"exposure":{"allowlist":["192.0.2.1/32"]}}`, `{"exposure":{"allowlist":["192.0.2.2/32","192.0.2.3/32"]}}`, []string{"exposure.allowlist"}, []string{`exposure.allowlist: ["192.0.2.1/32"] → ["192.0.2.2/32","192.0.2.3/32"]`}},
		{"presence", `{"removed":false}`, `{"added":0}`, []string{"added", "removed"}, []string{"added: (unset) → 0", "removed: false → (unset)"}},
		{"null array", `{"exposure":{"allowlist":null}}`, `{"exposure":{"allowlist":[]}}`, []string{"exposure.allowlist"}, []string{"exposure.allowlist: (unset) → []"}},
		{"empty array removed", `{"exposure":{"allowlist":[]}}`, `{"exposure":{"allowlist":null}}`, []string{"exposure.allowlist"}, []string{"exposure.allowlist: [] → (unset)"}},
		{"null object", `{"options":null}`, `{"options":{}}`, []string{"options"}, []string{"options: (unset) → {}"}},
		{"named identity", `{"backup":{"schedules":[{"name":"secret-daily","enabled":false}]}}`, `{"backup":{"schedules":[{"name":"secret-daily","enabled":true}]}}`, []string{"backup.schedules[secret-daily].enabled"}, []string{"backup.schedules[secret-daily].enabled: false → true"}},
		{"component identity", `{"components":{"extra":{"tokenizer":{"replicas":1}}}}`, `{"components":{"extra":{"tokenizer":{"replicas":2}}}}`, []string{"components.extra.tokenizer.replicas"}, []string{"components.extra.tokenizer.replicas: 1 → 2"}},
		{"no op", `{"version":"7"}`, `{"version":"7"}`, nil, nil},
		{"named reorder", `{"schedules":[{"name":"a"},{"name":"b"}]}`, `{"schedules":[{"name":"b"},{"name":"a"}]}`, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var before, after any
			if err := json.Unmarshal([]byte(tc.before), &before); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(tc.after), &after); err != nil {
				t.Fatal(err)
			}
			var changes []specChange
			diffSpecValues("", before, after, &changes)
			var paths, summaries []string
			for _, c := range changes {
				paths = append(paths, c.Path)
				summaries = append(summaries, specChangeText(c))
			}
			if !reflect.DeepEqual(paths, tc.paths) || !reflect.DeepEqual(summaries, tc.summaries) {
				t.Fatalf("paths=%v summaries=%v", paths, summaries)
			}
		})
	}
}

func writeOnlySpecPaths(t *testing.T) []string {
	t.Helper()
	doc, err := apispec.Load()
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	var walk func(*openapi3.Schema, string, map[*openapi3.Schema]bool)
	walk = func(schema *openapi3.Schema, path string, ancestors map[*openapi3.Schema]bool) {
		if schema == nil || ancestors[schema] {
			return
		}
		if schema.WriteOnly || schema.Format == "password" {
			found[path] = true
			return
		}
		ancestors[schema] = true
		defer delete(ancestors, schema)
		for name, ref := range schema.Properties {
			walk(ref.Value, joinSpecPath(path, name), ancestors)
		}
		for _, refs := range []openapi3.SchemaRefs{schema.AllOf, schema.AnyOf, schema.OneOf} {
			for _, ref := range refs {
				walk(ref.Value, path, ancestors)
			}
		}
		if schema.Items != nil {
			walk(schema.Items.Value, path+"[]", ancestors)
		}
		if schema.AdditionalProperties.Schema != nil {
			walk(schema.AdditionalProperties.Schema.Value, path+".*", ancestors)
		}
	}
	for _, name := range []string{"ClusterSpec", "ClusterInputSpec", "ClusterPatchSpec"} {
		ref, ok := doc.Components.Schemas[name]
		if !ok {
			t.Fatalf("missing schema %s", name)
		}
		walk(ref.Value, "", map[*openapi3.Schema]bool{})
	}
	if len(found) == 0 {
		t.Fatal("schema walker found no write-only spec fields")
	}
	return sortedSpecKeys(found)
}

func TestSpecDiffCoversWriteOnlySchemaAndMasksSecrets(t *testing.T) {
	for _, path := range writeOnlySpecPaths(t) {
		t.Run(path, func(t *testing.T) {
			if !sensitiveSpecPaths[path] {
				t.Fatalf("write-only schema path %s needs explicit audit masking", path)
			}
			for _, values := range [][2]any{{nil, "new-confidential-value"}, {"old-confidential-value", "new-confidential-value"}, {"old-confidential-value", nil}} {
				var changes []specChange
				diffSpecValues(path, values[0], values[1], &changes)
				data, err := json.Marshal(changes)
				if err != nil {
					t.Fatal(err)
				}
				if len(changes) != 1 || !changes[0].Sensitive || strings.Contains(string(data), "confidential") || strings.Contains(specChangeText(changes[0]), "confidential") {
					t.Fatal("unmasked write-only field", string(data))
				}
			}
		})
	}
	before := domain.ClusterSpec{EngineConfig: "old-confidential-config", EngineOptions: json.RawMessage(`{"password":"old-confidential-password","nested":{"private_key":"old-confidential-key"}}`)}
	after := domain.ClusterSpec{EngineConfig: "new-confidential-config", EngineOptions: json.RawMessage(`{"password":"new-confidential-password","nested":{"private_key":"new-confidential-key"},"users":[{"name":"app","credentials":{"token":"new-confidential-token"}}]}`)}
	event := clusterSpecAuditEvent("demo", 2, before, after, nil)
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "confidential") {
		t.Fatal("secret in message or structured details", string(data))
	}
	if !strings.Contains(string(event.Details), `"sensitive":true`) {
		t.Fatal("sensitivity not indicated")
	}
}

func TestSpecDiffSummaryBoundedAndNoOp(t *testing.T) {
	before := domain.ClusterSpec{}
	event := clusterSpecAuditEvent("demo", 1, before, before, nil)
	if event.Message != "Cluster spec change requested for demo: no changes" || string(event.Details) != `{"changes":[]}` {
		t.Fatal(event)
	}
	after := domain.ClusterSpec{Version: strings.Repeat("界", 100), Provider: "psmdb", Topology: domain.Topology{Type: "replicaSet"}, Components: domain.Components{Engine: domain.ComponentSpec{Replicas: 3}}}
	event = clusterSpecAuditEvent("demo", 2, before, after, nil)
	if len(event.Message) > 512 || !utf8.ValidString(event.Message) || !strings.Contains(event.Message, " and 1 more") {
		t.Fatal(event.Message)
	}
	if len(specValueText(after.Version)) > 64 {
		t.Fatal("unbounded value")
	}
	huge := strings.Repeat("界", 500)
	event = clusterSpecAuditEvent(huge, 2, before, after, nil)
	if len(event.Message) > 512 || !utf8.ValidString(event.Message) {
		t.Fatal("unbounded message")
	}
}

func TestSpecChangeAuditPatchAndPut(t *testing.T) {
	for _, method := range []string{"PATCH", "PUT"} {
		t.Run(method, func(t *testing.T) {
			s, _, token := testServer(t)
			c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
			if err != nil {
				t.Fatal(err)
			}
			old := c.Spec.Components.Engine.Resources.MaxDisk
			next := old + 1
			body := fmt.Sprintf(`{"spec":{"components":{"engine":{"resources":{"maxDisk":%g}}}}}`, next)
			if method == "PUT" {
				c.Spec.Components.Engine.Resources.MaxDisk = next
				data, err := json.Marshal(map[string]any{"spec": specAPI(c.Spec)})
				if err != nil {
					t.Fatal(err)
				}
				body = string(data)
			}
			w := call(s, method, "/api/v1/clusters/prod", body, token)
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			events := auditEvents(t, s, "ClusterSpecChangeRequested")
			if len(events) != 1 {
				t.Fatal(events)
			}
			expected := fmt.Sprintf("components.engine.resources.maxDisk: %g → %g", old, next)
			if !strings.Contains(events[0].Message, expected) || events[0].Operation == "" {
				t.Fatal(events[0])
			}
			var details specDetails
			if err = json.Unmarshal(events[0].Details, &details); err != nil {
				t.Fatal(err)
			}
			expectedChanges := 1
			if method == "PUT" && c.Spec.Backup.Schedules == nil {
				expectedChanges++
			}
			if method == "PUT" && c.Spec.Exposure.Allowlist == nil {
				expectedChanges++
			}
			if len(details.Changes) != expectedChanges {
				t.Fatal(string(events[0].Details))
			}
			found := false
			for _, change := range details.Changes {
				if change.Path == "components.engine.resources.maxDisk" {
					found = change.Before == old && change.After == next && !change.Sensitive
				}
			}
			if !found {
				t.Fatal("resource change missing", string(events[0].Details))
			}
			w = call(s, method, "/api/v1/clusters/prod", body, token)
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			events = auditEvents(t, s, "ClusterSpecChangeRequested")
			if len(events) != 2 || events[1].Message != "Cluster spec change requested for prod: no changes" {
				t.Fatal(events)
			}
		})
	}
}

func TestSpecAuditStoredWriteOnlyValuesMasked(t *testing.T) {
	s, _, _ := testServer(t)
	c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]string{"tls.certificate": "tls/user.crt", "tls.privateKey": "tls/user.key", "tls.caCertificate": "tls/ca.crt", "tls.caPrivateKey": "tls/ca.key"}
	for _, path := range writeOnlySpecPaths(t) {
		key, ok := keys[path]
		if !ok {
			t.Fatalf("add stored-event masking coverage for %s", path)
		}
		for i := range 2 {
			secret := fmt.Sprintf("confidential-%s-%d", path, i)
			audit := &store.SpecUpdateAudit{Build: func(before, after domain.ClusterSpec, generation int64, secrets []store.SpecSecretChange) domain.Event {
				return clusterSpecAuditEvent("prod", generation, before, after, secrets)
			}}
			ctx := store.WithSpecUpdateAudit(t.Context(), audit)
			if err := s.Store.UpdateClusterSpecWithSecrets(ctx, c.Ref(), c.Spec, map[string][]byte{key: []byte(secret)}); err != nil {
				t.Fatal(err)
			}
			if audit.Event == nil || strings.Contains(string(audit.Event.Details), "confidential") {
				t.Fatal("secret leaked before persistence")
			}
		}
	}
	events := auditEvents(t, s, "ClusterSpecChangeRequested")
	if len(events) != 2*len(keys) {
		t.Fatal("missing stored events", len(events))
	}
	for _, e := range events {
		data, err := json.Marshal(e)
		if err != nil || strings.Contains(string(data), "confidential") || !strings.Contains(string(e.Details), `"sensitive":true`) {
			t.Fatal("stored event leaked secret", err, string(data))
		}
	}
}

func TestEventDetailsListAndStreamVisibility(t *testing.T) {
	for _, auditRead := range []bool{false, true} {
		t.Run(fmt.Sprintf("audit=%t", auditRead), func(t *testing.T) {
			s, _, _ := testServer(t)
			roles := []string{"role:viewer"}
			if auditRead {
				roles = nil
			}
			if _, err := s.Auth.CreateAccount(t.Context(), domain.Account{Name: "reader", Roles: roles}, "reader-test-password"); err != nil {
				t.Fatal(err)
			}
			if auditRead {
				if err := s.RBAC.ReplacePolicy(t.Context(), s.RBAC.Policy()+"\np, user:reader, audit, read, default/prod, allow\n"); err != nil {
					t.Fatal(err)
				}
			}
			token, _, err := s.Auth.CreateToken(t.Context(), "reader", "events", nil)
			if err != nil {
				t.Fatal(err)
			}
			e := domain.Event{Cluster: "prod", Type: "Normal", Message: "details sentinel", Details: json.RawMessage(`{"changes":[{"path":"version","before":"7","after":"8"}]}`)}
			if err = s.Store.AppendEvent(t.Context(), &e); err != nil {
				t.Fatal(err)
			}
			w := call(s, "GET", "/api/v1/events?cluster=prod", "", token)
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			var list api.EventList
			if err = json.Unmarshal(w.Body.Bytes(), &list); err != nil || len(list.Items) != 1 {
				t.Fatal(err, w.Body.String())
			}
			if (list.Items[0].Details != nil) != auditRead {
				t.Fatal("incorrect list visibility", w.Body.String())
			}
			ctx, cancel := streamContext(t, s, token)
			defer cancel()
			writer := &callbackStreamWriter{ResponseRecorder: httptest.NewRecorder()}
			writer.flushed = func() {
				if strings.Contains(writer.Body.String(), "details sentinel") {
					cancel()
				}
			}
			_ = testStream(s, ctx, writer)
			if strings.Contains(writer.Body.String(), `"details":`) != auditRead {
				t.Fatal("incorrect SSE visibility", writer.Body.String())
			}
			// Response filtering must not mutate the durable event or hub payload.
			items, err := s.Store.ListEvents(context.Background(), store.EventFilter{Cluster: "prod"})
			if err != nil || len(items) != 1 || len(items[0].Details) == 0 {
				t.Fatal("response filtering changed stored details", err)
			}
		})
	}
}

func TestSpecDiffDetailsBounds(t *testing.T) {
	t.Run("large values", func(t *testing.T) {
		old, next := strings.Repeat("a", 64*1024), strings.Repeat("b", 64*1024)
		before := domain.ClusterSpec{EngineOptions: json.RawMessage(fmt.Sprintf(`{"description":%q}`, old))}
		after := domain.ClusterSpec{EngineOptions: json.RawMessage(fmt.Sprintf(`{"description":%q}`, next))}
		event := clusterSpecAuditEvent("demo", 2, before, after, nil)
		var details specDetails
		if err := json.Unmarshal(event.Details, &details); err != nil {
			t.Fatal(err)
		}
		if len(event.Details) > maxSpecDetailsBytes || !details.Truncated || len(details.Changes) != 1 || !details.Changes[0].Truncated || details.Changes[0].Before != "(65538 bytes)" || details.Changes[0].After != "(65538 bytes)" {
			t.Fatal("large values were not bounded", string(event.Details))
		}
	})
	t.Run("total values", func(t *testing.T) {
		beforeOptions, afterOptions := map[string]string{}, map[string]string{}
		for i := range 100 {
			beforeOptions[fmt.Sprintf("setting%03d", i)] = strings.Repeat("a", 500)
			afterOptions[fmt.Sprintf("setting%03d", i)] = strings.Repeat("b", 500)
		}
		old, _ := json.Marshal(beforeOptions)
		next, _ := json.Marshal(afterOptions)
		event := clusterSpecAuditEvent("demo", 2, domain.ClusterSpec{EngineOptions: old}, domain.ClusterSpec{EngineOptions: next}, nil)
		var details specDetails
		if err := json.Unmarshal(event.Details, &details); err != nil {
			t.Fatal(err)
		}
		if len(event.Details) > maxSpecDetailsBytes || !details.Truncated || len(details.Changes) != 100 {
			t.Fatal("total details did not preserve bounded paths", len(event.Details), len(details.Changes))
		}
		for _, change := range details.Changes {
			if !change.Truncated || change.Before != nil || change.After != nil {
				t.Fatal("values retained beyond total budget", change.Path)
			}
		}
	})
	t.Run("many paths", func(t *testing.T) {
		details := specDetails{Changes: make([]specChange, 1000)}
		for i := range details.Changes {
			details.Changes[i] = specChange{Path: fmt.Sprintf("engineOptions.setting%04d", i), Before: "old", After: "new"}
		}
		data := boundedSpecDetails(details)
		var bounded specDetails
		if err := json.Unmarshal(data, &bounded); err != nil {
			t.Fatal(err)
		}
		if len(data) > maxSpecDetailsBytes || !bounded.Truncated || len(bounded.Changes) == 0 || len(bounded.Changes) >= len(details.Changes) || bounded.Changes[0].Path != "engineOptions.setting0000" {
			t.Fatal("oversized path set was not bounded", len(data), len(bounded.Changes))
		}
	})
	t.Run("large path", func(t *testing.T) {
		data := boundedSpecDetails(specDetails{Changes: []specChange{{Path: strings.Repeat("界", 20000), After: true}}})
		var details specDetails
		if err := json.Unmarshal(data, &details); err != nil {
			t.Fatal(err)
		}
		if len(data) > maxSpecDetailsBytes || !details.Truncated || len(details.Changes) != 1 || !details.Changes[0].Truncated || len(details.Changes[0].Path) > maxSpecChangeValueBytes || !utf8.ValidString(details.Changes[0].Path) {
			t.Fatal("oversized path was not bounded", len(data))
		}
	})
	t.Run("sensitive sizes", func(t *testing.T) {
		before := domain.ClusterSpec{EngineConfig: strings.Repeat("x", 64000)}
		after := domain.ClusterSpec{EngineConfig: strings.Repeat("y", 65000)}
		event := clusterSpecAuditEvent("demo", 2, before, after, nil)
		if strings.Contains(string(event.Details), "bytes") || strings.Contains(string(event.Details), "64000") || strings.Contains(string(event.Details), "65000") {
			t.Fatal("sensitive input size leaked", string(event.Details))
		}
		var details specDetails
		if err := json.Unmarshal(event.Details, &details); err != nil || details.Truncated || details.Changes[0].Before != "(changed)" || details.Changes[0].After != "(changed)" {
			t.Fatal("sensitive change was not masked before bounding", err, string(event.Details))
		}
	})
}

func TestEventDetailsOmittedWhenAbsent(t *testing.T) {
	s, _, token := testServer(t)
	for _, details := range []json.RawMessage{nil, json.RawMessage("null")} {
		event := domain.Event{Cluster: "prod", Type: "Normal", Message: "no details", Details: details}
		if err := s.Store.AppendEvent(t.Context(), &event); err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(eventAPI(event))
		if err != nil || strings.Contains(string(data), `"details":`) {
			t.Fatal("absent details must be omitted", err, string(data))
		}
	}
	w := call(s, "GET", "/api/v1/events?cluster=prod", "", token)
	if w.Code != 200 || strings.Contains(w.Body.String(), `"details":`) {
		t.Fatal("list must omit absent details", w.Code, w.Body.String())
	}
	ctx, cancel := streamContext(t, s, token)
	defer cancel()
	writer := &callbackStreamWriter{ResponseRecorder: httptest.NewRecorder()}
	writer.flushed = func() {
		if strings.Count(writer.Body.String(), `"message":"no details"`) == 2 {
			cancel()
		}
	}
	_ = testStream(s, ctx, writer)
	if strings.Contains(writer.Body.String(), `"details":`) {
		t.Fatal("SSE must omit absent details", writer.Body.String())
	}
}

func TestSpecChangeTruncationSurvivesEventList(t *testing.T) {
	s, _, token := testServer(t)
	body, err := json.Marshal(map[string]any{"spec": map[string]any{"engineOptions": map[string]any{"description": strings.Repeat("a", 2048)}}})
	if err != nil {
		t.Fatal(err)
	}
	if w := call(s, "PATCH", "/api/v1/clusters/prod", string(body), token); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	w := call(s, "GET", "/api/v1/events?cluster=prod", "", token)
	var list api.EventList
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &list) != nil {
		t.Fatal(w.Code, w.Body.String())
	}
	found := false
	for _, event := range list.Items {
		if event.Details == nil || event.Details.Changes == nil {
			continue
		}
		for _, change := range *event.Details.Changes {
			if change.Path != "engineOptions.description" {
				continue
			}
			found = true
			if change.Truncated == nil || !*change.Truncated || event.Details.Truncated == nil || !*event.Details.Truncated || change.After == nil || *change.After != "(2050 bytes)" {
				t.Fatal("truncation flags or size marker lost during event serialization", w.Body.String())
			}
		}
	}
	if !found {
		t.Fatal("truncated spec change missing from events response", w.Body.String())
	}
}

func TestSpecChangeReportsNullToEmptyCollections(t *testing.T) {
	s, _, token := testServer(t)
	before, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	before.Spec.Exposure.Allowlist = nil
	before.Spec.Backup.Schedules = nil
	if err = s.Store.UpdateClusterSpec(t.Context(), before.Ref(), before.Spec); err != nil {
		t.Fatal(err)
	}
	before, err = s.Store.GetCluster(t.Context(), before.Ref())
	if err != nil {
		t.Fatal(err)
	}
	if w := call(s, "PATCH", "/api/v1/clusters/prod", `{"spec":{"backup":{"schedules":[]},"exposure":{"allowlist":[]}}}`, token); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	after, err := s.Store.GetCluster(t.Context(), before.Ref())
	if err != nil || after.Generation != before.Generation+1 {
		t.Fatal("expected persisted collection representation change", err, after)
	}
	events := auditEvents(t, s, "ClusterSpecChangeRequested")
	if len(events) != 1 || strings.Contains(events[0].Message, "no changes") {
		t.Fatal("generation change was incorrectly reported as a no-op", events)
	}
	var details specDetails
	if err = json.Unmarshal(events[0].Details, &details); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, change := range details.Changes {
		if change.Path == "exposure.allowlist" {
			list, ok := change.After.([]any)
			found = change.Before == nil && ok && len(list) == 0
		}
	}
	if !found {
		t.Fatal("null-to-empty array transition missing", string(events[0].Details))
	}
	if w := call(s, "PATCH", "/api/v1/clusters/prod", `{"spec":{}}`, token); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	repeated, err := s.Store.GetCluster(t.Context(), before.Ref())
	if err != nil || repeated.Generation != after.Generation {
		t.Fatal("repeated empty-array state advanced generation", err, repeated)
	}
	events = auditEvents(t, s, "ClusterSpecChangeRequested")
	if len(events) != 2 || events[1].Message != "Cluster spec change requested for prod: no changes" {
		t.Fatal("actual no-op was not reported", events)
	}
}
