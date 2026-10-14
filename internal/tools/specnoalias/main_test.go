// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/oasdiff/yaml3"
)

func TestRewriteRetainsReferencesAndComments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "openapi.yaml")
	source := "# License notice\nresponses:\n  '400': &bad\n    $ref: '#/components/responses/BadRequest'\n  '404': *bad\n"
	if err := os.WriteFile(path, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run(path, true); err == nil {
		t.Fatal("check accepted an alias")
	}
	if err := run(path, false); err != nil {
		t.Fatal(err)
	}
	if err := run(path, true); err != nil {
		t.Fatal(err)
	}
	output, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(output), "#/components/responses/BadRequest") != 2 || !strings.Contains(string(output), "# License notice") {
		t.Fatal(string(output))
	}
	var before, after any
	if err := yaml.Unmarshal([]byte(source), &before); err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(output, &after); err != nil {
		t.Fatal(err)
	}
	if yamlText(before) != yamlText(after) {
		t.Fatalf("rewrite changed the document: %v -> %v", before, after)
	}
}

func yamlText(v any) string {
	data, err := yaml.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func TestRewriteRejectsCycles(t *testing.T) {
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte("loop: &loop [*loop]\n"), &doc); err != nil {
		t.Fatal(err)
	}
	if _, err := expand(&doc, make(map[*yaml.Node]bool), false); err == nil {
		t.Fatal("accepted cyclic alias")
	}
}

func TestRepositorySpecificationHasNoAliases(t *testing.T) {
	if err := run(filepath.Join("..", "..", "..", "api", "openapi.yaml"), true); err != nil {
		t.Fatal(err)
	}
}
