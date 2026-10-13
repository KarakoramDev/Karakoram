// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
)

func TestParseOptions(t *testing.T) {
	for _, tt := range []struct {
		name, raw      string
		valid, apiKeys bool
	}{
		{"empty", "", true, true}, {"object", "{}", true, true}, {"explicit false", `{"apm":{"public":true,"apiKeys":false}}`, true, false},
		{"settings", `{"elasticsearch":{"settings":{"indices":{"recovery":{"max_bytes_per_sec":"20mb"}}}},"kibana":{"settings":{"telemetry.optIn":false}}}`, true, true},
		{"unknown", `{"unknown":1}`, false, false}, {"nested unknown", `{"apm":{"bogus":true}}`, false, false}, {"pipeline unknown", `{"logstash":{"pipelines":[{"id":"main","config":"input{}","bogus":1}]}}`, false, false},
		{"null", "null", false, false}, {"array", "[]", false, false}, {"trailing", "{} {}", false, false}, {"trailing garbage", "{} garbage", false, false}, {"malformed", "{", false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			o, err := ParseOptions([]byte(tt.raw))
			if (err == nil) != tt.valid {
				t.Fatalf("parse result: %v", err)
			}
			if tt.valid && (o.APM.APIKeys == nil || *o.APM.APIKeys != tt.apiKeys) {
				t.Fatal("APM default")
			}
		})
	}
}

func TestPipelineOptions(t *testing.T) {
	for _, tt := range []struct {
		name      string
		pipelines []agentspec.Pipeline
		valid     bool
	}{
		{"none", nil, true}, {"valid", []agentspec.Pipeline{{ID: "main_1-test", Config: "input {}"}}, true}, {"boundary", []agentspec.Pipeline{{ID: "a" + strings.Repeat("b", 31), Config: strings.Repeat("a", 64*1024)}}, true},
		{"ID too long", []agentspec.Pipeline{{ID: "a" + strings.Repeat("b", 32), Config: "x"}}, false}, {"invalid ID", []agentspec.Pipeline{{ID: "../x", Config: "x"}}, false}, {"uppercase", []agentspec.Pipeline{{ID: "Main", Config: "x"}}, false}, {"digit first", []agentspec.Pipeline{{ID: "1main", Config: "x"}}, false},
		{"duplicate", []agentspec.Pipeline{{ID: "main", Config: "x"}, {ID: "main", Config: "y"}}, false}, {"empty config", []agentspec.Pipeline{{ID: "main"}}, false}, {"blank config", []agentspec.Pipeline{{ID: "main", Config: " \n"}}, false}, {"large config", []agentspec.Pipeline{{ID: "main", Config: strings.Repeat("x", 64*1024+1)}}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var o Options
			o.Logstash.Pipelines = tt.pipelines
			if (o.Validate() == nil) != tt.valid {
				t.Fatal("pipeline validation")
			}
		})
	}
	for _, n := range []int{10, 11} {
		var o Options
		for i := range n {
			o.Logstash.Pipelines = append(o.Logstash.Pipelines, agentspec.Pipeline{ID: string(rune('a' + i)), Config: "x"})
		}
		if (o.Validate() == nil) != (n <= 10) {
			t.Fatal("pipeline count limit")
		}
	}
}

func TestManagedSettings(t *testing.T) {
	for _, component := range []string{"elasticsearch", "kibana"} {
		keys := esManaged
		if component == "kibana" {
			keys = kibanaManaged
		}
		for _, key := range keys {
			if strings.HasSuffix(key, ".") {
				key += "example"
			}
			for _, nested := range []bool{false, true} {
				t.Run(component+"/"+key+"/nested="+map[bool]string{false: "no", true: "yes"}[nested], func(t *testing.T) {
					settings := map[string]any{key: "value"}
					if nested {
						parts := strings.Split(key, ".")
						var value any = "value"
						for i := len(parts) - 1; i >= 0; i-- {
							value = map[string]any{parts[i]: value}
						}
						settings = value.(map[string]any)
					}
					var o Options
					if component == "kibana" {
						o.Kibana.Settings = settings
					} else {
						o.Elasticsearch.Settings = settings
					}
					if err := o.Validate(); err == nil || !strings.Contains(err.Error(), "managed") {
						t.Fatalf("managed setting not rejected: %v", err)
					}
					raw, err := json.Marshal(o)
					if err != nil {
						t.Fatal(err)
					}
					if _, err = ParseOptions(raw); err == nil {
						t.Fatal("parse must validate")
					}
				})
			}
		}
	}
	for _, tt := range []struct{ component, key string }{{"elasticsearch", "xpack.security"}, {"elasticsearch", "path"}, {"kibana", "elasticsearch"}, {"kibana", "xpack.encryptedSavedObjects"}} {
		var o Options
		s := map[string]any{tt.key: map[string]any{}}
		if tt.component == "kibana" {
			o.Kibana.Settings = s
		} else {
			o.Elasticsearch.Settings = s
		}
		if o.Validate() == nil {
			t.Fatal("empty managed map bypass")
		}
	}
	var o Options
	o.Elasticsearch.Settings = map[string]any{"some.password": "not-managed", "node.attr.rack": "a", "cluster.routing.allocation.enable": "all"}
	o.Kibana.Settings = map[string]any{"server.basePath": "/app", "telemetry.optIn": false}
	if err := o.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestFlattenSettings(t *testing.T) {
	for _, tt := range []struct {
		name     string
		in, want map[string]any
		valid    bool
	}{
		{"nested", map[string]any{"a": map[string]any{"b": 1}}, map[string]any{"a.b": 1}, true},
		{"empty", nil, map[string]any{}, true}, {"empty map", map[string]any{"a": map[string]any{}}, map[string]any{"a": map[string]any{}}, true},
		{"collision", map[string]any{"a.b": 1, "a": map[string]any{"b": 2}}, nil, false}, {"empty name", map[string]any{"": 1}, nil, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FlattenSettings(tt.in)
			if (err == nil) != tt.valid {
				t.Fatalf("flatten: %v", err)
			}
			if tt.valid && !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %#v", got)
			}
		})
	}
}
