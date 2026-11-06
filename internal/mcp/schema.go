// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mcp

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/apispec"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"gopkg.in/yaml.v3"
)

type operation struct {
	Tags                                    []string
	ID, Method, Path, BasePath, ContentType string
	Parameters                              []parameter
	Rule                                    api.AgentRule
	Tool                                    *sdk.Tool
}
type parameter struct {
	Name, In string
	Required bool
	Schema   map[string]any
}

var inventory = sync.OnceValues(func() ([]operation, error) { return loadInventory(true) })
var inventoryWithoutOutputs = sync.OnceValues(func() ([]operation, error) { return loadInventory(false) })

func loadInventory(outputSchemas bool) ([]operation, error) {
	var doc map[string]any
	if err := yaml.Unmarshal(apispec.YAML(), &doc); err != nil {
		return nil, err
	}
	return inventoryFromDocumentWithOutputs(doc, outputSchemas)
}

func inventoryFromDocument(doc map[string]any) ([]operation, error) {
	return inventoryFromDocumentWithOutputs(doc, true)
}

func inventoryFromDocumentWithOutputs(doc map[string]any, outputSchemas bool) ([]operation, error) {
	resolve := func(v map[string]any) map[string]any {
		if ref, ok := v["$ref"].(string); ok {
			var item any = doc
			for _, part := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
				item = item.(map[string]any)[part]
			}
			return item.(map[string]any)
		}
		return v
	}
	var out []operation
	for path, pv := range doc["paths"].(map[string]any) {
		pi := pv.(map[string]any)
		for method, ov := range pi {
			op, ok := ov.(map[string]any)
			if !ok {
				continue
			}
			id, _ := op["operationId"].(string)
			rule, ok := api.OperationAgent[id]
			if !ok || !rule.Exposed {
				continue
			}
			o := operation{ID: id, Method: strings.ToUpper(method), Path: path, BasePath: "/api/v1", Rule: rule}
			if servers := anySlice(op["servers"]); len(servers) > 0 {
				server := servers[0].(map[string]any)
				if endpoint, ok := server["url"].(string); ok && strings.HasPrefix(endpoint, "/") {
					o.BasePath = strings.TrimSuffix(endpoint, "/")
				}
			}
			props := map[string]any{}
			required := []string{}
			defs := map[string]any{}
			converter := schemaConverter{resolve: resolve, defs: defs, maxDepth: maxSchemaRefDepth}
			schema := func(value any) any { return converter.convert(value, 0) }
			params, _ := pi["parameters"].([]any)
			own, _ := op["parameters"].([]any)
			params = append(append([]any(nil), params...), own...)
			for _, p := range params {
				v := resolve(p.(map[string]any))
				name, _ := v["name"].(string)
				in, _ := v["in"].(string)
				if in != "path" && in != "query" {
					continue
				}
				req, _ := v["required"].(bool)
				s, _ := v["schema"].(map[string]any)
				prop, _ := schema(s).(map[string]any)
				if prop == nil {
					prop = map[string]any{}
				}
				if description, _ := v["description"].(string); strings.TrimSpace(description) != "" {
					prop["description"] = strings.TrimSpace(description)
				}
				for _, key := range []string{"example", "deprecated"} {
					if value, exists := v[key]; exists {
						prop[key] = value
					}
				}
				props[name] = prop
				if req {
					required = append(required, name)
				}
				o.Parameters = append(o.Parameters, parameter{Name: name, In: in, Required: req, Schema: s})
			}
			for _, tag := range anySlice(op["tags"]) {
				o.Tags = append(o.Tags, fmt.Sprint(tag))
			}
			summary, _ := op["summary"].(string)
			description, _ := op["description"].(string)
			text := strings.TrimSpace(summary + "\n\n" + description)
			if len(text) > 6000 {
				text = text[:6000]
			}
			if body, ok := op["requestBody"].(map[string]any); ok {
				body = resolve(body)
				content, _ := body["content"].(map[string]any)
				for _, mime := range []string{"application/json", "application/merge-patch+json"} {
					if media, ok := content[mime].(map[string]any); ok {
						o.ContentType = mime
						prop, _ := schema(media["schema"]).(map[string]any)
						description, _ := body["description"].(string)
						if strings.TrimSpace(description) == "" {
							if source, ok := media["schema"].(map[string]any); ok {
								description, _ = resolve(source)["description"].(string)
							}
						}
						if strings.TrimSpace(description) != "" {
							prop["description"] = strings.TrimSpace(description)
						}
						props["body"] = prop
						if body["required"] == true {
							required = append(required, "body")
						}
						break
					}
				}
			}
			input := map[string]any{"type": "object", "properties": props, "required": required, "additionalProperties": false}
			if len(defs) > 0 {
				input["$defs"] = defs
			}
			destructive, open := rule.Risk == "destructive", false
			o.Tool = &sdk.Tool{Name: rule.Tool, Title: summary, Description: text, InputSchema: input, Annotations: &sdk.ToolAnnotations{ReadOnlyHint: rule.Risk == "read", DestructiveHint: &destructive, IdempotentHint: rule.Idempotent, OpenWorldHint: &open}}
			if outputSchemas {
				o.Tool.OutputSchema = responseSchema(op, resolve)
			}
			if _, err := json.Marshal(input); err != nil {
				return nil, err
			}
			out = append(out, o)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Tool.Name < out[j].Tool.Name })
	return out, nil
}
func anySlice(v any) []any { a, _ := v.([]any); return a }

// Bound reference traversal even for recursive component graphs. Definitions are
// shared, rather than expanded at every use.
const maxSchemaRefDepth = 16
const maxOutputSchemaBytes = 16 * 1024

type schemaConverter struct {
	resolve  func(map[string]any) map[string]any
	defs     map[string]any
	maxDepth int
}

func (c schemaConverter) convert(value any, depth int) any {
	switch v := value.(type) {
	case map[string]any:
		result := map[string]any{}
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, k := range keys {
			x := v[k]
			switch k {
			case "$ref":
				ref, _ := x.(string)
				const prefix = "#/components/schemas/"
				if !strings.HasPrefix(ref, prefix) || depth >= c.maxDepth {
					continue
				}
				name := strings.TrimPrefix(ref, prefix)
				// The same component can occur with different remaining budgets.
				// Never reuse a shallow conversion for a deeper occurrence.
				defName := fmt.Sprintf("%s_depth%d", name, c.maxDepth-depth)
				result[k] = "#/$defs/" + defName
				if _, exists := c.defs[defName]; !exists {
					c.defs[defName] = map[string]any{}
					c.defs[defName] = c.convert(c.resolve(v), depth+1)
				}
			case "nullable":
			default:
				result[k] = c.convert(x, depth)
			}
		}
		if v["nullable"] == true {
			return map[string]any{"anyOf": []any{result, map[string]any{"type": "null"}}}
		}
		return result
	case []any:
		result := make([]any, len(v))
		for i, x := range v {
			result[i] = c.convert(x, depth)
		}
		return result
	default:
		return value
	}
}

func successResponseSchema(op map[string]any, resolve func(map[string]any) map[string]any) map[string]any {
	responses, _ := op["responses"].(map[string]any)
	jsonSchema := func(value any) map[string]any {
		response, _ := value.(map[string]any)
		if response == nil {
			return nil
		}
		content, _ := resolve(response)["content"].(map[string]any)
		media, _ := content["application/json"].(map[string]any)
		source, _ := media["schema"].(map[string]any)
		return source
	}
	source := jsonSchema(responses["200"])
	if source == nil {
		count := 0
		for code, value := range responses {
			if len(code) == 3 && code[0] == '2' {
				if candidate := jsonSchema(value); candidate != nil {
					count++
					source = candidate
				}
			}
		}
		if count != 1 {
			return nil
		}
	}
	return source
}

func buildOutputSchema(source map[string]any, resolve func(map[string]any) map[string]any, maxDepth int) map[string]any {
	// Each build owns its cache; definitions from a previous depth attempt
	// cannot leak into a more restrictive attempt.
	defs := map[string]any{}
	c := schemaConverter{resolve: resolve, defs: defs, maxDepth: maxDepth}
	root := c.structured(source, 0)
	if len(defs) > 0 {
		root["$defs"] = defs
	}
	return root
}

func responseSchema(op map[string]any, resolve func(map[string]any) map[string]any) any {
	source := successResponseSchema(op, resolve)
	if source == nil {
		return nil
	}
	for depth := maxSchemaRefDepth; depth >= 1; depth-- {
		root := buildOutputSchema(source, resolve, depth)
		data, err := json.Marshal(root)
		if err == nil && len(data) <= maxOutputSchemaBytes {
			return root
		}
	}
	// Keep the tool and execute's envelope even if its inline schema cannot
	// fit at the shallowest reference budget.
	return map[string]any{"type": "object", "properties": map[string]any{"operationId": map[string]any{"type": "string"}}}
}

func (c schemaConverter) structured(source map[string]any, depth int) map[string]any {
	if depth > c.maxDepth {
		return map[string]any{"type": "object"}
	}
	if _, ok := source["$ref"]; ok {
		resolved := c.resolve(source)
		if len(source) == 1 {
			return c.structured(resolved, depth+1)
		}
		merged := make(map[string]any, len(resolved)+len(source))
		for key, value := range resolved {
			merged[key] = value
		}
		for key, value := range source {
			if key != "$ref" {
				merged[key] = value
			}
		}
		return c.structured(merged, depth+1)
	}
	if source["nullable"] == true {
		nonNull := make(map[string]any, len(source))
		for key, value := range source {
			if key != "nullable" {
				nonNull[key] = value
			}
		}
		return map[string]any{"type": "object", "anyOf": []any{c.structured(nonNull, depth+1), c.structured(map[string]any{"type": "null"}, depth+1)}}
	}
	result := c.convert(source, depth).(map[string]any)
	// Each union alternative describes the envelope actually returned for that
	// alternative, including mixed object/non-object response unions.
	for _, key := range []string{"anyOf", "oneOf"} {
		if branches := anySlice(source[key]); len(branches) > 0 {
			converted := make([]any, len(branches))
			for i, branch := range branches {
				converted[i] = c.structured(branch.(map[string]any), depth+1)
			}
			result[key] = converted
			result["type"] = "object"
			props, _ := result["properties"].(map[string]any)
			if props == nil {
				props = map[string]any{}
			}
			props["operationId"] = map[string]any{"type": "string"}
			result["properties"] = props
			return result
		}
	}
	_, hasProperties := source["properties"]
	isObject := source["type"] == "object" || source["type"] == nil || hasProperties || source["allOf"] != nil
	if !isObject {
		return map[string]any{"type": "object", "properties": map[string]any{"result": result, "operationId": map[string]any{"type": "string"}}, "required": []string{"result"}}
	}
	if branches := anySlice(source["allOf"]); len(branches) > 0 {
		converted := make([]any, len(branches))
		for i, branch := range branches {
			converted[i] = c.structured(branch.(map[string]any), depth+1)
		}
		result["allOf"] = converted
	}
	props, _ := result["properties"].(map[string]any)
	if props == nil {
		props = map[string]any{}
	}
	props["operationId"] = map[string]any{"type": "string"}
	result["properties"] = props
	result["type"] = "object"
	return result
}
