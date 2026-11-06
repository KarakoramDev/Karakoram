// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/google/jsonschema-go/jsonschema"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func resolvedOutput(t *testing.T, value any) *jsonschema.Resolved {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var schema jsonschema.Schema
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	resolved, err := schema.Resolve(nil)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func TestParameterAndBodyMetadata(t *testing.T) {
	constraint := map[string]any{"type": "string", "default": "small", "enum": []any{"small", "large"}, "minLength": 1, "maxLength": 20, "pattern": "^[a-z]+$", "format": "hostname"}
	for _, ref := range []bool{false, true} {
		for _, nullable := range []bool{false, true} {
			t.Run(fmt.Sprintf("ref=%v/nullable=%v", ref, nullable), func(t *testing.T) {
				paramSchema := map[string]any{}
				if ref {
					paramSchema["$ref"] = "#/components/schemas/Choice"
				} else {
					for key, value := range constraint {
						paramSchema[key] = value
					}
				}
				paramSchema["nullable"] = nullable
				op := map[string]any{"operationId": "listClusters", "parameters": []any{map[string]any{"name": "choice", "in": "query", "description": "Choose a deployment size.", "example": "small", "deprecated": true, "schema": paramSchema}}, "requestBody": map[string]any{"description": "Desired settings supplied as JSON.", "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/Body"}}}}}
				doc := map[string]any{"paths": map[string]any{"/fixture": map[string]any{"get": op}}, "components": map[string]any{"schemas": map[string]any{"Choice": constraint, "Body": map[string]any{"type": "object", "description": "Fallback request settings."}}}}
				for _, explicit := range []bool{true, false} {
					if !explicit {
						delete(op["requestBody"].(map[string]any), "description")
					}
					operations, err := inventoryFromDocument(doc)
					if err != nil || len(operations) != 1 {
						t.Fatalf("inventory: %v %v", operations, err)
					}
					input := operations[0].Tool.InputSchema.(map[string]any)
					props := input["properties"].(map[string]any)
					property := props["choice"].(map[string]any)
					if property["description"] != "Choose a deployment size." || property["example"] != "small" || property["deprecated"] != true {
						t.Fatalf("parameter metadata lost: %v", property)
					}
					constrained := property
					if nullable {
						constrained = property["anyOf"].([]any)[0].(map[string]any)
					}
					if ref {
						defName := strings.TrimPrefix(constrained["$ref"].(string), "#/$defs/")
						constrained = input["$defs"].(map[string]any)[defName].(map[string]any)
					}
					for key, value := range constraint {
						got, _ := json.Marshal(constrained[key])
						want, _ := json.Marshal(value)
						if string(got) != string(want) {
							t.Errorf("%s: got %s want %s", key, got, want)
						}
					}
					expected := "Desired settings supplied as JSON."
					if !explicit {
						expected = "Fallback request settings."
					}
					if props["body"].(map[string]any)["description"] != expected {
						t.Fatalf("body metadata lost: %v", props["body"])
					}
					resolvedOutput(t, input)
				}
			})
		}
	}
}

func TestOutputSchemaInventory(t *testing.T) {
	operations, err := inventory()
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	noContent := false
	for _, op := range operations {
		if op.ID == "getCluster" || op.ID == "listClusters" {
			if op.Tool.OutputSchema == nil {
				t.Errorf("%s missing output schema", op.ID)
			}
			found[op.ID] = true
		}
		if op.Tool.OutputSchema != nil {
			resolvedOutput(t, op.Tool.OutputSchema)
		}
		if op.ID == "getCluster" {
			// A real REST Cluster envelope must retain its top-level contract
			// even when nested component definitions exceed the byte budget.
			var cluster map[string]any
			if err := json.Unmarshal([]byte(`{"namespace":"default","name":"example","generation":1,"spec":{"provider":"psmdb","version":"8.0","topology":{"type":"replicaSet"},"components":{"engine":{"replicas":1,"resources":{"cpuMode":"SHARED","minCpu":1,"maxCpu":1,"minRam":2,"maxRam":2,"minDisk":10,"maxDisk":10}}}},"status":{"phase":"Ready","observedGeneration":1,"conditions":[],"members":[]},"createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z","operationId":"op-example"}`), &cluster); err != nil {
				t.Fatal(err)
			}
			resolved := resolvedOutput(t, op.Tool.OutputSchema)
			if err := resolved.Validate(cluster); err != nil {
				t.Fatalf("cluster response rejected after depth reduction: %v", err)
			}
			cluster["name"] = 42
			if err := resolved.Validate(cluster); err == nil {
				t.Fatal("cluster top-level constraints were discarded")
			}
		}
		if op.ID == "deleteAPIToken" {
			noContent = true
			if op.Tool.OutputSchema != nil {
				t.Error("204 response has output schema")
			}
		}
	}
	if len(found) != 2 || !noContent {
		t.Fatalf("expected inventory fixtures missing: %v, 204=%v", found, noContent)
	}
}

func TestOutputSchemaEnvelopesAndCaps(t *testing.T) {
	object := map[string]any{"type": "object", "properties": map[string]any{"name": map[string]any{"type": "string"}}, "required": []any{"name"}, "additionalProperties": false}
	resolve := func(v map[string]any) map[string]any {
		if v["$ref"] != nil {
			return object
		}
		return v
	}
	for _, test := range []struct {
		name           string
		source         map[string]any
		valid, invalid any
	}{
		{"closed", object, map[string]any{"name": "test", "operationId": "op"}, map[string]any{"name": "test", "unexpected": true}},
		{"reference", map[string]any{"$ref": "#/components/schemas/Object"}, map[string]any{"name": "test", "operationId": "op"}, map[string]any{"name": 12}},
		{"allOf", map[string]any{"allOf": []any{object}}, map[string]any{"name": "test", "operationId": "op"}, map[string]any{"name": 12}},
		{"anyOf", map[string]any{"anyOf": []any{object, map[string]any{"type": "array", "items": map[string]any{"type": "string"}}}}, map[string]any{"result": []any{"x"}, "operationId": "op"}, map[string]any{"result": []any{12}}},
		{"oneOf", map[string]any{"oneOf": []any{object, map[string]any{"type": "string"}}}, map[string]any{"name": "test", "operationId": "op"}, map[string]any{"name": 12}},
		{"array", map[string]any{"type": "array", "items": map[string]any{"type": "string"}}, map[string]any{"result": []any{"x"}, "operationId": "op"}, map[string]any{"result": []any{12}}},
		{"nullable", map[string]any{"type": "string", "nullable": true}, map[string]any{"result": nil, "operationId": "op"}, map[string]any{"result": 12}},
		{"nullableReference", map[string]any{"$ref": "#/components/schemas/Object", "nullable": true}, map[string]any{"result": nil, "operationId": "op"}, map[string]any{"result": 12}},
	} {
		t.Run(test.name, func(t *testing.T) {
			op := map[string]any{"responses": map[string]any{"202": map[string]any{"content": map[string]any{"application/json": map[string]any{"schema": test.source}}}}}
			schema := responseSchema(op, resolve)
			if schema == nil {
				t.Fatal("missing output schema")
			}
			resolved := resolvedOutput(t, schema)
			if err := resolved.Validate(test.valid); err != nil {
				t.Fatalf("valid envelope rejected: %v", err)
			}
			if err := resolved.Validate(test.invalid); err == nil {
				t.Fatal("invalid envelope accepted")
			}
		})
	}
	oversized := map[string]any{"type": "object", "description": strings.Repeat("x", maxOutputSchemaBytes)}
	op := map[string]any{"responses": map[string]any{"200": map[string]any{"content": map[string]any{"application/json": map[string]any{"schema": oversized}}}}}
	raw, _ := json.Marshal(responseSchema(op, resolve))
	if len(raw) > maxOutputSchemaBytes || strings.Contains(string(raw), "description") {
		t.Fatalf("oversize schema not degraded: %d", len(raw))
	}
	recursive := map[string]any{"$ref": "#/components/schemas/Recursive"}
	c := schemaConverter{resolve: func(map[string]any) map[string]any { return recursive }, defs: map[string]any{}, maxDepth: maxSchemaRefDepth}
	root := c.structured(recursive, 0)
	if root["type"] != "object" {
		t.Fatal("reference-depth fallback missing")
	}
	chain := schemaConverter{resolve: func(v map[string]any) map[string]any { n := v["$ref"].(string); return map[string]any{"$ref": n + "x"} }, defs: map[string]any{}, maxDepth: maxSchemaRefDepth}
	chain.convert(recursive, 0)
	if len(chain.defs) != maxSchemaRefDepth {
		t.Fatalf("unbounded reference expansion: %d", len(chain.defs))
	}
}

func TestOutputSchemaClientCalls(t *testing.T) {
	for _, outputSchemas := range []bool{false, true} {
		for _, version := range []string{"2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25", "2026-07-28"} {
			t.Run(fmt.Sprintf("%s/outputSchemas=%t", version, outputSchemas), func(t *testing.T) {
				fail := false
				server, _ := testEndpoint(t, "readonly", "admin", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					if fail {
						w.WriteHeader(http.StatusNotFound)
						fmt.Fprint(w, `{"error":"fixture missing"}`)
						return
					}
					w.Header().Set("Karakoram-Operation-Id", "op-fixture")
					fmt.Fprint(w, `{"available":false,"intervals":[]}`)
				}), nil, outputSchemas)
				session := connectClient(t, server, version, nil)
				list, err := session.ListTools(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				var output any
				for _, tool := range list.Tools {
					if tool.Name == api.OperationAgent["getPITRWindow"].Tool {
						output = tool.OutputSchema
					}
				}
				if (output != nil) != outputSchemas {
					t.Fatalf("advertised output schema=%v, enabled=%t", output, outputSchemas)
				}
				args := &sdk.CallToolParams{Name: "pitr_window_get", Arguments: map[string]any{"cluster": "fixture"}}
				result, err := session.CallTool(context.Background(), args)
				if err != nil || result.IsError {
					t.Fatalf("successful call: %#v %v", result, err)
				}
				if result.StructuredContent.(map[string]any)["operationId"] != "op-fixture" {
					t.Fatal("operation header lost")
				}
				if outputSchemas {
					if err := resolvedOutput(t, output).Validate(result.StructuredContent); err != nil {
						t.Fatal(err)
					}
				}
				if !strings.Contains(result.Content[0].(*sdk.TextContent).Text, "op-fixture") {
					t.Fatal("success text lost")
				}
				fail = true
				result, err = session.CallTool(context.Background(), args)
				if err != nil || !result.IsError || result.StructuredContent != nil {
					t.Fatalf("error call: %#v %v", result, err)
				}
				if !strings.Contains(result.Content[0].(*sdk.TextContent).Text, "fixture missing") {
					t.Fatal("error text lost")
				}
			})
		}
	}
}

func TestSuccessResponseSelection(t *testing.T) {
	response := func(kind string) any {
		return map[string]any{"content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"type": kind}}}}
	}
	resolve := func(v map[string]any) map[string]any { return v }
	for _, test := range []struct {
		name      string
		responses map[string]any
		present   bool
	}{
		{"preferred200", map[string]any{"200": response("object"), "201": response("string")}, true},
		{"createdAndEmpty", map[string]any{"201": response("object"), "204": map[string]any{}}, true},
		{"ambiguous", map[string]any{"201": response("object"), "202": response("string")}, false},
		{"noContent", map[string]any{"204": map[string]any{}}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			schema := responseSchema(map[string]any{"responses": test.responses}, resolve)
			if (schema != nil) != test.present {
				t.Fatalf("outputSchema=%v, expected present=%v", schema, test.present)
			}
			if schema != nil {
				if err := resolvedOutput(t, schema).Validate(map[string]any{"operationId": "op"}); err != nil {
					t.Fatalf("selected wrong response: %v", err)
				}
			}
		})
	}
}

func TestOutputSchemaDepthBudget(t *testing.T) {
	ref := func(name string) map[string]any { return map[string]any{"$ref": "#/components/schemas/" + name} }
	components := map[string]map[string]any{
		"Response": {"type": "object", "properties": map[string]any{"name": map[string]any{"type": "string"}, "details": ref("Details")}, "required": []any{"name", "details"}, "additionalProperties": false},
		"Details":  {"type": "object", "properties": map[string]any{"payload": ref("Payload")}},
		"Payload":  {"type": "object", "description": strings.Repeat("large annotation ", 1200)},
	}
	resolve := func(value map[string]any) map[string]any {
		if name, ok := value["$ref"].(string); ok {
			return components[strings.TrimPrefix(name, "#/components/schemas/")]
		}
		return value
	}
	source := ref("Response")
	full, _ := json.Marshal(buildOutputSchema(source, resolve, maxSchemaRefDepth))
	if len(full) <= maxOutputSchemaBytes {
		t.Fatal("fixture does not exceed schema budget")
	}
	op := map[string]any{"responses": map[string]any{"200": map[string]any{"content": map[string]any{"application/json": map[string]any{"schema": source}}}}}
	bounded := responseSchema(op, resolve).(map[string]any)
	raw, _ := json.Marshal(bounded)
	if len(raw) > maxOutputSchemaBytes {
		t.Fatalf("schema budget exceeded: %d", len(raw))
	}
	if bounded["properties"].(map[string]any)["name"] == nil {
		t.Fatal("lost top-level response shape instead of reducing depth")
	}
	expected, _ := json.Marshal(buildOutputSchema(source, resolve, 2))
	if string(raw) != string(expected) {
		t.Fatalf("did not choose the deepest fitting budget: %s", raw)
	}
	resolved := resolvedOutput(t, bounded)
	if err := resolved.Validate(map[string]any{"name": "example", "details": map[string]any{"payload": map[string]any{"arbitrary": true}}, "operationId": "op"}); err != nil {
		t.Fatalf("real response shape rejected: %v", err)
	}
	if err := resolved.Validate(map[string]any{"name": 42, "details": map[string]any{}}); err == nil {
		t.Fatal("top-level type constraints lost")
	}
	if err := resolved.Validate(map[string]any{"name": "example"}); err == nil {
		t.Fatal("top-level required constraints lost")
	}
}

func TestSchemaDefinitionDepthIsolation(t *testing.T) {
	components := map[string]map[string]any{
		"Outer":  {"type": "object", "properties": map[string]any{"inner": map[string]any{"$ref": "#/components/schemas/Shared"}}},
		"Shared": {"type": "object", "properties": map[string]any{"value": map[string]any{"$ref": "#/components/schemas/Value"}}},
		"Value":  {"type": "string"},
	}
	resolve := func(value map[string]any) map[string]any {
		return components[strings.TrimPrefix(value["$ref"].(string), "#/components/schemas/")]
	}
	source := map[string]any{"type": "object", "properties": map[string]any{
		"aDeeper":    map[string]any{"$ref": "#/components/schemas/Outer"},
		"zShallower": map[string]any{"$ref": "#/components/schemas/Shared"},
	}}
	resolved := resolvedOutput(t, buildOutputSchema(source, resolve, 2))
	if err := resolved.Validate(map[string]any{"zShallower": map[string]any{"value": 42}}); err == nil {
		t.Fatal("shallower reference reused an under-specified deeper definition")
	}
	if err := resolved.Validate(map[string]any{"aDeeper": map[string]any{"inner": map[string]any{"value": 42}}, "zShallower": map[string]any{"value": "valid"}}); err != nil {
		t.Fatalf("deeper reference did not respect its smaller remaining budget: %v", err)
	}
}
