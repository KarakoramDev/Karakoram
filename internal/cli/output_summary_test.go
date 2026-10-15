// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const summaryProvider = `{
  "name":"psmdb","displayName":"Percona Server for MongoDB",
  "versions":[{"version":"7.0","major":"7.0","default":false,"status":"supported"},{"version":"8.0","major":"8.0","default":true,"status":"supported"}],
  "backupTypes":["logical","physical"],
  "presets":[{"name":"dev","displayName":"Development","spec":{"components":{"engine":{"replicas":1}}}}],
  "topologies":[
    {"type":"replicaSet","displayName":"Replica set","defaults":{"version":"8.0","topology":{"type":"replicaSet"},"components":{"engine":{"replicas":3,"resources":{"minCpu":1,"maxCpu":2,"minRam":1,"maxRam":2}}},"tls":{"mode":"requireTLS"}},"limits":{"engine":{"allowedReplicas":[1,3,5,7],"minReplicas":1,"maxReplicas":7}},"componentSchemas":{},"uiSchema":{"schemaMarker":"full-schema-only"}},
    {"type":"sharded","displayName":"Sharded","defaults":{"topology":{"type":"sharded","shards":2},"components":{"engine":{"replicas":3},"configServer":{"replicas":3},"router":{"replicas":2}}},"limits":{"shards":{"min":1,"max":128}},"componentSchemas":{},"uiSchema":{}}
  ],
  "upgradePaths":[{"from":"7.0","to":"8.0","requiresFCV":"7.0","requiresBackup":true},{"from":"8.0","to":"8.1","requiresBackup":false}]
}`

func TestProviderHumanSummaryAndUnchangedDocuments(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/v1/providers" {
			fmt.Fprintf(w, `{"items":[%s]}`, summaryProvider)
		} else {
			fmt.Fprint(w, summaryProvider)
		}
	}))
	defer server.Close()
	for _, args := range [][]string{{"providers", "get", "psmdb"}, {"providers", "list"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			code, out, stderr := runCLI(t, server.URL, "", false, args...)
			if code != 0 {
				t.Fatalf("code=%d stderr=%q", code, stderr)
			}
			for _, want := range []string{"psmdb", "Percona Server for MongoDB", "7.0", "8.0", "replicaSet", "sharded", "dev", "-o json", "-o yaml"} {
				if !strings.Contains(out, want) {
					t.Errorf("missing %q: %q", want, out)
				}
			}
			for _, unwanted := range []string{"full-schema-only", "componentSchemas=", "defaults=", ";", "spec="} {
				if strings.Contains(out, unwanted) {
					t.Errorf("flattened content %q: %q", unwanted, out)
				}
			}
			if args[1] == "get" {
				for _, want := range []string{"Default version", "Topologies", "KEY DEFAULTS", "engine replicas=3", "router replicas=2", "shards=2", "Limits", "allowedReplicas", "1, 3, 5, 7", "Upgrade paths", "7.0 → 8.0 (requires FCV 7.0) (requires backup)", "8.0 → 8.1"} {
					if !strings.Contains(out, want) {
						t.Errorf("missing %q: %q", want, out)
					}
				}
			}
			var expected any
			body := summaryProvider
			if args[1] == "list" {
				body = `{"items":[` + body + `]}`
			}
			if err := json.Unmarshal([]byte(body), &expected); err != nil {
				t.Fatal(err)
			}
			for _, format := range []string{"json", "yaml"} {
				code, out, stderr := runCLI(t, server.URL, "", false, append(append([]string{}, args...), "-o", format)...)
				if code != 0 {
					t.Fatalf("%s: code=%d stderr=%q", format, code, stderr)
				}
				var actual any
				if format == "json" {
					if err := json.Unmarshal([]byte(out), &actual); err != nil {
						t.Fatal(err)
					}
				} else {
					var document map[string]any
					if err := yaml.Unmarshal([]byte(out), &document); err != nil {
						t.Fatal(err)
					}
					// Normalize YAML integer values to the JSON response's numeric representation.
					normalized, err := json.Marshal(document)
					if err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(normalized, &actual); err != nil {
						t.Fatal(err)
					}
				}
				if !reflect.DeepEqual(actual, expected) {
					t.Errorf("%s document changed: %s", format, out)
				}
			}
		})
	}
}

func TestNestedResourceTablesUseDetailSections(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		const resource = `{"name":"demo","spec":{"components":{"engine":{"replicas":3,"resources":{"maxCpu":2,"maxRam":4}}}},"status":{"phase":"Ready"}}`
		if r.URL.Path == "/api/v1/clusters" {
			fmt.Fprintf(w, `{"items":[%s],"nextCursor":"next-page"}`, resource)
		} else {
			fmt.Fprint(w, resource)
		}
	}))
	defer server.Close()
	for _, args := range [][]string{{"clusters", "get", "demo"}, {"clusters", "list"}} {
		code, out, stderr := runCLI(t, server.URL, "", false, args...)
		if code != 0 || strings.Contains(out, "components=") || strings.Contains(out, ";") {
			t.Fatalf("%v: code=%d stdout=%q stderr=%q", args, code, out, stderr)
		}
		if args[1] == "get" {
			for _, text := range []string{"spec:", "components:", "engine:", "resources:", "maxCpu", "maxRam", "replicas"} {
				if !strings.Contains(out, text) {
					t.Errorf("%v missing %q: %q", args, text, out)
				}
			}
		} else {
			if strings.Contains(out, "Details for") || strings.Contains(out, "maxCpu") || !strings.Contains(out, "-o json") {
				t.Errorf("list contains nested detail dumps or lacks guidance: %q", out)
			}
			if !strings.Contains(stderr, "--cursor next-page") {
				t.Errorf("missing pagination: stderr=%q", stderr)
			}
		}
	}
}

func TestOperationListSummarizesNestedResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"items":[{"id":"op-one","namespace":"default","cluster":"demo","type":"upgrade","phase":"Running","target":{"kind":"cluster","name":"demo"},"targetGeneration":4,"message":"Waiting for upgraded members","actor":"account:operator","createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:01Z","result":{"details":{"largeField":"nested-result-only"}}}]}`)
	}))
	defer server.Close()
	code, out, stderr := runCLI(t, server.URL, "", false, "operations", "list")
	if code != 0 || !strings.Contains(out, "op-one") || !strings.Contains(out, "Running") || !strings.Contains(out, "cluster/demo") || !strings.Contains(out, "-o json") || strings.Contains(out, "nested-result-only") || strings.Contains(out, "Details for") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, out, stderr)
	}
}

func TestCompactObjectCells(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   map[string]any
		want    string
		compact bool
	}{
		{"target", map[string]any{"kind": "cluster", "name": "demo"}, "cluster/demo", true},
		{"target with scope", map[string]any{"kind": "cluster", "name": "demo", "namespace": "dev"}, "cluster/demo,namespace=dev", true},
		{"scalar fields", map[string]any{"replicas": float64(3), "ready": true}, "ready=true,replicas=3", true},
		{"empty", map[string]any{}, "-", true},
		{"forty characters", map[string]any{"key": strings.Repeat("x", 36)}, "key=" + strings.Repeat("x", 36), true},
		{"long", map[string]any{"key": strings.Repeat("x", 37)}, "", false},
		{"integer fields", map[string]any{"replicas": 3}, "replicas=3", true},
		{"unicode characters", map[string]any{"key": strings.Repeat("é", 36)}, "key=" + strings.Repeat("é", 36), true},
		{"too many fields", map[string]any{"a": true, "b": true, "c": true, "d": true}, "", false},
		{"nested object", map[string]any{"nested": map[string]any{"name": "demo"}}, "", false},
		{"array", map[string]any{"values": []any{"one", "two"}}, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			summary, compact := compactObjectCell(tc.value)
			if compact != tc.compact || compact && summary != tc.want {
				t.Fatalf("summary=%q compact=%t, want %q compact=%t", summary, compact, tc.want, tc.compact)
			}
		})
	}
}
