// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"
	"unicode"

	"github.com/KarakoramDev/Karakoram/internal/apispec"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"gopkg.in/yaml.v3"
)

func qualityWords(text string) []string {
	var split strings.Builder
	runes := []rune(text)
	for i, r := range runes {
		if unicode.IsUpper(r) && i > 0 && (unicode.IsLower(runes[i-1]) || (i+1 < len(runes) && unicode.IsLower(runes[i+1]))) {
			split.WriteByte(' ')
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			split.WriteRune(unicode.ToLower(r))
		} else {
			split.WriteByte(' ')
		}
	}
	return strings.Fields(split.String())
}

func TestToolQuality(t *testing.T) {
	for _, outputSchemas := range []bool{false, true} {
		t.Run(fmt.Sprintf("outputSchemas=%t", outputSchemas), func(t *testing.T) {
			testToolQuality(t, outputSchemas)
		})
	}
}

func testToolQuality(t *testing.T, outputSchemas bool) {
	operations, err := inventory()
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]string{}
	for _, op := range operations {
		ids[op.Tool.Name] = op.ID
	}
	server, _ := testEndpoint(t, "readwrite", "admin", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), nil, outputSchemas)
	session := connectClient(t, server, "2026-07-28", nil)
	list, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Tools) != len(operations)+2 {
		t.Fatalf("inventory incomplete: got %d tools, want %d", len(list.Tools), len(operations)+2)
	}
	counts := map[string]int{}
	for _, tool := range list.Tools {
		report := func(reason string) {
			category, _, _ := strings.Cut(reason, ":")
			counts[category]++
			t.Errorf("%s: %s", tool.Name, reason)
		}
		title := strings.TrimSpace(tool.Title)
		if title == "" || !strings.ContainsFunc(title, unicode.IsSpace) || strings.EqualFold(title, tool.Name) || strings.EqualFold(title, ids[tool.Name]) {
			report("missing or identifier-only title")
		}
		description := strings.TrimSpace(tool.Description)
		if ids[tool.Name] != "" {
			description = strings.TrimSpace(strings.TrimPrefix(description, tool.Title))
		}
		words := qualityWords(description)
		if len(words) < 12 {
			report("description has fewer than 12 words")
		}
		excluded := map[string]bool{}
		for _, word := range qualityWords(tool.Title + " " + tool.Name + " " + ids[tool.Name]) {
			excluded[word] = true
		}
		novel := map[string]bool{}
		for _, word := range words {
			if len([]rune(word)) >= 3 && !excluded[word] {
				novel[word] = true
			}
		}
		if len(novel) < 6 {
			report("description has fewer than 6 distinct contextual words")
		}
		input := tool.InputSchema.(map[string]any)
		props, _ := input["properties"].(map[string]any)
		names := make([]string, 0, len(props))
		for name := range props {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			prop, _ := props[name].(map[string]any)
			text, _ := prop["description"].(string)
			if strings.TrimSpace(text) == "" {
				report("input property missing description: " + name)
			}
		}
	}
	reasons := make([]string, 0, len(counts))
	for reason := range counts {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)
	for _, reason := range reasons {
		t.Logf("quality failures: %s: %d", reason, counts[reason])
	}
}

func TestToolSchemaSizes(t *testing.T) {
	var doc map[string]any
	if err := yaml.Unmarshal(apispec.YAML(), &doc); err != nil {
		t.Fatal(err)
	}
	resolve := func(value map[string]any) map[string]any {
		if ref, ok := value["$ref"].(string); ok {
			var item any = doc
			for _, part := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
				item = item.(map[string]any)[part]
			}
			return item.(map[string]any)
		}
		return value
	}
	operations, err := inventory()
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]string{}
	for _, op := range operations {
		names[op.ID] = op.Tool.Name
	}
	degraded := map[string]bool{}
	for _, path := range doc["paths"].(map[string]any) {
		for _, entry := range path.(map[string]any) {
			op, ok := entry.(map[string]any)
			if !ok {
				continue
			}
			id, _ := op["operationId"].(string)
			if names[id] == "" {
				continue
			}
			source := successResponseSchema(op, resolve)
			if source == nil {
				continue
			}
			full, err := json.Marshal(buildOutputSchema(source, resolve, maxSchemaRefDepth))
			if err != nil {
				t.Fatal(err)
			}
			if len(full) > maxOutputSchemaBytes {
				degraded[names[id]] = true
			}
		}
	}
	for _, mode := range []string{"readonly", "readwrite"} {
		for _, outputSchemas := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/outputSchemas=%t", mode, outputSchemas), func(t *testing.T) {
				server, _ := testEndpoint(t, mode, "admin", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), nil, outputSchemas)
				session := connectClient(t, server, "2026-07-28", nil)
				list, err := session.ListTools(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				after, err := json.Marshal(list)
				if err != nil {
					t.Fatal(err)
				}
				if !outputSchemas {
					// Descriptions and documented parameters alone exceed 1.25x
					// the old 115/303 KB baseline. Keep the agreed compact budgets.
					budget := 215000
					if mode == "readwrite" {
						budget = 540000
					}
					if len(after) > budget {
						t.Errorf("default tools/list exceeds compact size budget: %d > %d", len(after), budget)
					}
				}
				largest, name := 0, ""
				before := *list
				before.Tools = make([]*sdk.Tool, len(list.Tools))
				reduced := []string{}
				for i, tool := range list.Tools {
					if degraded[tool.Name] {
						reduced = append(reduced, tool.Name)
					}
					if tool.OutputSchema != nil {
						if !outputSchemas {
							t.Errorf("%s publishes outputSchema by default", tool.Name)
						}
						raw, err := json.Marshal(tool.OutputSchema)
						if err != nil {
							t.Fatal(err)
						}
						if len(raw) > maxOutputSchemaBytes {
							t.Errorf("%s output schema exceeds cap: %d", tool.Name, len(raw))
						}
						if len(raw) > largest {
							largest, name = len(raw), tool.Name
						}
					}
					copy := *tool
					copy.OutputSchema = nil
					before.Tools[i] = &copy
				}
				raw, err := json.Marshal(before)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("%d tools; tools/list result JSON without outputSchema=%d bytes, with outputSchema=%d bytes; largest outputSchema=%s (%d bytes)", len(list.Tools), len(raw), len(after), name, largest)
				t.Logf("depth-degraded tools: %s", strings.Join(reduced, ", "))
			})
		}
	}
}
