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

var inventory = sync.OnceValues(loadInventory)

func loadInventory() ([]operation, error) {
	var doc map[string]any
	if err := yaml.Unmarshal(apispec.YAML(), &doc); err != nil {
		return nil, err
	}
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
			var schema func(any) any
			schema = func(value any) any {
				switch v := value.(type) {
				case map[string]any:
					result := map[string]any{}
					for k, x := range v {
						if k == "$ref" {
							ref := x.(string)
							prefix := "#/components/schemas/"
							if !strings.HasPrefix(ref, prefix) {
								continue
							}
							name := strings.TrimPrefix(ref, prefix)
							result[k] = "#/$defs/" + name
							if _, exists := defs[name]; !exists {
								defs[name] = nil
								defs[name] = schema(resolve(v))
							}
							continue
						}
						if k == "nullable" {
							continue
						}
						result[k] = schema(x)
					}
					if v["nullable"] == true {
						return map[string]any{"anyOf": []any{result, map[string]any{"type": "null"}}}
					}
					return result
				case []any:
					result := make([]any, len(v))
					for i, x := range v {
						result[i] = schema(x)
					}
					return result
				default:
					return value
				}
			}
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
				props[name] = schema(s)
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
						props["body"] = schema(media["schema"])
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
