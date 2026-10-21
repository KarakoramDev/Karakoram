// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package main

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const metadata = "{tool: version_get, risk: read, confirm: none, async: none, idempotent: true}"
const validSpec = `openapi: 3.0.3
paths:
  /version:
    get:
      operationId: getVersion
      x-karakoram-agent: {tool: version_get, risk: read, confirm: none, async: none, idempotent: true}
  /health:
    get:
      operationId: getHealth
      x-karakoram-agent: {tool: health_get, risk: read, confirm: none, async: none}
`

func TestGenerateDeterministicAndCompilable(t *testing.T) {
	first, err := generate([]byte(validSpec))
	if err != nil {
		t.Fatal(err)
	}
	second, err := generate([]byte(validSpec))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("nondeterministic output")
	}
	if !bytes.HasPrefix(first, []byte(licenseHeader)) {
		t.Fatal("missing SPDX header")
	}
	fs := token.NewFileSet()
	file, err := parser.ParseFile(fs, "agentmap.gen.go", first, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := new(types.Config).Check("api", fs, []*ast.File{file}, nil); err != nil {
		t.Fatal(err)
	}
	text := string(first)
	if strings.Index(text, `"getHealth"`) > strings.Index(text, `"getVersion"`) {
		t.Fatal("unsorted operation IDs")
	}
	if !strings.Contains(text, `Exposed: true`) || !strings.Contains(text, `Secret: false`) || !strings.Contains(text, `Untrusted: false`) {
		t.Fatal("incorrect defaults")
	}
}

func TestRejectInvalidMetadata(t *testing.T) {
	replace := func(value string) string { return strings.Replace(validSpec, metadata, value, 1) }
	cases := []struct{ name, source, message string }{
		{"version", strings.Replace(validSpec, "3.0.3", "3.1.0", 1), "expected OpenAPI"},
		{"missing id", strings.Replace(validSpec, "operationId: getVersion", "summary: Version", 1), "operationId"},
		{"invalid id", strings.Replace(validSpec, "getVersion", "get-version", 1), "operationId"},
		{"duplicate id", strings.Replace(validSpec, "getHealth", "getVersion", 1), "duplicate operationId"},
		{"missing extension", strings.Replace(validSpec, "x-karakoram-agent: "+metadata, "summary: Version", 1), "missing x-karakoram-agent"},
		{"scalar", replace("public"), "must be a mapping"},
		{"unknown key", replace(strings.TrimSuffix(metadata, "}") + ", unknown: true}"), "unknown key"},
		{"duplicate key", replace(strings.TrimSuffix(metadata, "}") + ", tool: other}"), "duplicate key"},
		{"invalid bool", replace(strings.Replace(metadata, "idempotent: true", "idempotent: yes", 1)), "invalid type"},
		{"null bool", replace(strings.Replace(metadata, "idempotent: true", "idempotent: null", 1)), "invalid type"},
		{"missing tool", replace("{risk: read, confirm: none, async: none}"), "tool name"},
		{"invalid tool", replace(strings.Replace(metadata, "version_get", "Version-get", 1)), "tool name"},
		{"long tool", replace(strings.Replace(metadata, "version_get", strings.Repeat("a", 65), 1)), "tool name"},
		{"duplicate tool", strings.Replace(validSpec, "health_get", "version_get", 1), "duplicate tool"},
		{"missing reason", replace("{exposed: false, risk: read, confirm: none, async: none}"), "requires reason"},
		{"blank reason", replace("{exposed: false, reason: ' ', risk: read, confirm: none, async: none}"), "requires reason"},
		{"hidden tool", replace("{exposed: false, reason: hidden, tool: version_get, risk: read, confirm: none, async: none}"), "forbids tool"},
		{"empty hidden tool", replace("{exposed: false, reason: hidden, tool: '', risk: read, confirm: none, async: none}"), "forbids tool"},
		{"invalid risk", replace(strings.Replace(metadata, "risk: read", "risk: safe", 1)), "invalid risk"},
		{"missing risk", replace(strings.Replace(metadata, "risk: read, ", "", 1)), "invalid risk"},
		{"read confirm", replace(strings.Replace(metadata, "confirm: none", "confirm: confirm", 1)), "read risk requires"},
		{"invalid confirm type", replace(strings.Replace(metadata, "confirm: none", "confirm: true", 1)), "invalid type"},
		{"unknown confirm", replace(strings.Replace(metadata, "confirm: none", "confirm: invalid", 1)), "read risk requires"},
		{"missing confirm", replace(strings.Replace(metadata, "confirm: none, ", "", 1)), "read risk requires"},
		{"write confirm", replace(strings.Replace(strings.Replace(metadata, "risk: read", "risk: write", 1), "confirm: none", "confirm: name", 1)), "write risk requires"},
		{"destructive none", replace(strings.Replace(metadata, "risk: read", "risk: destructive", 1)), "destructive risk requires"},
		{"invalid async", replace(strings.Replace(metadata, "async: none", "async: polling", 1)), "invalid async"},
		{"missing async", replace(strings.Replace(metadata, "async: none, ", "", 1)), "invalid async"},
		{"unsafe read", strings.Replace(validSpec, "    get:", "    post:", 1), "non-GET read"},
		{"no paths", "openapi: 3.0.3\npaths: {}", "no paths"},
		{"no operations", "openapi: 3.0.3\npaths:\n  /version:\n    summary: Version", "no operations"},
		{"invalid yaml", "openapi: [", "yaml"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := generate([]byte(tc.source))
			if err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("expected %q, got %v", tc.message, err)
			}
		})
	}
}

func TestReviewedNonGETReads(t *testing.T) {
	operations := []struct{ id, method, path string }{
		{"checkPermission", "post", "/rbac/can"},
		{"dryRunCreateCluster", "post", "/dry-run/clusters"},
		{"dryRunReplaceCluster", "put", "/dry-run/clusters/{cluster}"},
		{"dryRunPatchCluster", "patch", "/dry-run/clusters/{cluster}"},
		{"dryRunCreateRestore", "post", "/dry-run/clusters/{cluster}/restores"},
	}
	for _, op := range operations {
		t.Run(op.id, func(t *testing.T) {
			source := strings.Replace(validSpec, "    get:", "    "+op.method+":", 1)
			source = strings.NewReplacer("/version:", op.path+":", "getVersion", op.id).Replace(source)
			if _, err := generate([]byte(source)); err != nil {
				t.Fatal(err)
			}
			for _, bad := range []string{strings.Replace(source, "idempotent: true", "idempotent: false", 1), strings.Replace(source, op.path+":", "/unexpected:", 1), strings.Replace(source, op.id, "otherRead", 1), strings.Replace(source, "    "+op.method+":", "    delete:", 1)} {
				if _, err := generate([]byte(bad)); err == nil {
					t.Fatal("accepted unreviewed or non-idempotent read")
				}
			}
		})
	}
}

func TestValidMetadataVariants(t *testing.T) {
	for _, value := range []string{
		"{exposed: false, reason: 'Session plumbing.', risk: read, confirm: none, async: none}",
		"{tool: a, risk: write, confirm: none, async: operation, secret: true, untrusted: true}",
		"{tool: a, risk: destructive, confirm: name, async: generation}",
		"{tool: a, risk: destructive, confirm: confirm, async: record}",
	} {
		if _, err := generate([]byte(strings.Replace(validSpec, metadata, value, 1))); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRepositorySpecification(t *testing.T) {
	input, err := os.ReadFile("../../../api/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	generated, err := generate(input)
	if err != nil {
		t.Fatal(err)
	}
	checkedIn, err := os.ReadFile("../../api/agentmap.gen.go")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(generated, checkedIn) {
		t.Fatal("agent map is stale: run go generate ./api/...")
	}
}

func TestRunFailsBeforeWriting(t *testing.T) {
	dir := t.TempDir()
	spec := filepath.Join(dir, "spec.yaml")
	out := filepath.Join(dir, "agentmap.gen.go")
	if err := os.WriteFile(spec, []byte("openapi: ["), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out, []byte("unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run(spec, out); err == nil {
		t.Fatal("accepted invalid specification")
	}
	content, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "unchanged" {
		t.Fatal("changed output on validation failure")
	}
}
