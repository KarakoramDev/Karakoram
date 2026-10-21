// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mcp

import (
	"encoding/json"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
)

func TestNullableOpenAPISchemas(t *testing.T) {
	operations, err := inventory()
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		id, input string
		valid     bool
	}{
		{"replaceAPIToken", `{"token":"id","body":{"name":"agent","expiresAt":null}}`, true},
		{"replaceAPIToken", `{"token":"id","body":{"name":null,"expiresAt":null}}`, false},
		{"patchCluster", `{"cluster":"prod","body":{"spec":{"provider":null,"engineConfig":null,"engineOptions":null,"topology":null,"tls":null,"backup":null,"monitoring":null,"dataSource":null,"exposure":null}}}`, true},
		{"patchCluster", `{"cluster":"prod","body":{"spec":{"exposure":{"ipType":null,"public":null,"allowlist":null},"dataSource":{"namespace":null}}}}`, true},
		{"patchCluster", `{"cluster":"prod","body":{"spec":{"exposure":{"ipType":"INVALID"}}}}`, false},
		{"patchCluster", `{"cluster":"prod","body":{"spec":{"engineConfig":123}}}`, false},
	} {
		t.Run(test.id+"/"+test.input, func(t *testing.T) {
			var inputSchema any
			for _, op := range operations {
				if op.ID == test.id {
					inputSchema = op.Tool.InputSchema
					break
				}
			}
			if inputSchema == nil {
				t.Fatal("operation missing")
			}
			data, err := json.Marshal(inputSchema)
			if err != nil {
				t.Fatal(err)
			}
			var schema jsonschema.Schema
			if err := json.Unmarshal(data, &schema); err != nil {
				t.Fatal(err)
			}
			resolved, err := schema.Resolve(nil)
			if err != nil {
				t.Fatal(err)
			}
			var input any
			if err := json.Unmarshal([]byte(test.input), &input); err != nil {
				t.Fatal(err)
			}
			err = resolved.Validate(input)
			if (err == nil) != test.valid {
				t.Fatalf("valid=%v, expected %v: %v", err == nil, test.valid, err)
			}
		})
	}
}
