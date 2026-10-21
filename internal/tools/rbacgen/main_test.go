// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package main

import (
	"bytes"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validSpec = `openapi: 3.0.3
paths:
  /version:
    get:
      operationId: getVersion
      security: []
      x-karakoram-rbac: public
  /clusters/{cluster}/connection:
    get:
      operationId: getClusterConnection
      x-karakoram-rbac: {resource: cluster-credentials, action: read, object: cluster}
  /namespaces/{namespace}:
    get:
      operationId: getNamespace
      x-karakoram-rbac: {resource: namespaces, action: read, object: namespace}
  /backup-storages/{storage}:
    get:
      operationId: getBackupStorage
      x-karakoram-rbac: {resource: backup-storages, action: read, object: storage}
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
		t.Fatal("generation is not deterministic")
	}
	if !bytes.HasPrefix(first, []byte(licenseHeader)) {
		t.Fatal("generated source lacks SPDX header")
	}
	if _, err := parser.ParseFile(token.NewFileSet(), "rbacmap.gen.go", first, parser.AllErrors); err != nil {
		t.Fatal(err)
	}
	text := string(first)
	if !strings.Contains(text, `Resource: "cluster-credentials", Action: "read", ObjectParam: "cluster", Public: false`) {
		t.Fatal("credential read authorization was lost")
	}
	if !strings.Contains(text, `"getVersion":`) || !strings.Contains(text, `Public: true`) {
		t.Fatal("public operation was lost")
	}
	for _, object := range []string{"namespace", "storage"} {
		if !strings.Contains(text, `ObjectParam: "`+object+`", Public: false`) {
			t.Fatalf("%s authorization was lost", object)
		}
	}
	if strings.Index(text, `"getClusterConnection"`) > strings.Index(text, `"getVersion"`) {
		t.Fatal("operation IDs are not sorted")
	}
}

func TestDataMcpVocabulary(t *testing.T) {
	for _, action := range []string{"read", "write"} {
		source := strings.Replace(validSpec, "resource: cluster-credentials, action: read", "resource: cluster-data, action: "+action, 1)
		if _, err := generate([]byte(source)); err != nil {
			t.Fatalf("cluster-data:%s: %v", action, err)
		}
	}
}

func TestRejectInvalidMetadata(t *testing.T) {
	tests := []struct{ name, source, message string }{
		{"wrong version", strings.Replace(validSpec, "3.0.3", "3.1.0", 1), "expected OpenAPI"},
		{"missing ID", strings.Replace(validSpec, "operationId: getVersion", "summary: Version", 1), "operationId"},
		{"non camel case ID", strings.Replace(validSpec, "getVersion", "get-version", 1), "operationId"},
		{"duplicate ID", strings.Replace(validSpec, "getClusterConnection", "getVersion", 1), "duplicate operationId"},
		{"missing extension", strings.Replace(validSpec, "x-karakoram-rbac: public", "summary: Public", 1), "missing x-karakoram-rbac"},
		{"invalid scalar", strings.Replace(validSpec, "x-karakoram-rbac: public", "x-karakoram-rbac: anonymous", 1), "scalar must be public"},
		{"secured public route", strings.Replace(validSpec, "security: []", "security: [{bearerAuth: []}]", 1), "security: []"},
		{"unknown resource", strings.Replace(validSpec, "cluster-credentials", "unknown", 1), "unknown resource/action"},
		{"unknown action", strings.Replace(validSpec, "action: read", "action: reveal", 1), "unknown resource/action"},
		{"missing scope", strings.Replace(validSpec, ", object: cluster", "", 1), "object must be"},
		{"incorrect parameter", strings.Replace(validSpec, "object: cluster", "object: member", 1), "object must be"},
		{"incorrect wildcard", strings.Replace(validSpec, "object: cluster", "object: '*'", 1), "cluster-scoped operation"},
		{"unbound cluster", strings.Replace(validSpec, "/clusters/{cluster}/connection", "/connection", 1), "object must be"},
		{"unbound namespace", strings.Replace(validSpec, "/namespaces/{namespace}", "/namespaces/example", 1), "object must be"},
		{"unbound storage", strings.Replace(validSpec, "/backup-storages/{storage}", "/backup-storages/example", 1), "object must be"},
		{"namespace wildcard", strings.Replace(validSpec, "object: namespace", "object: '*'", 1), "namespace-scoped operation"},
		{"storage wildcard", strings.Replace(validSpec, "object: storage", "object: '*'", 1), "storage-scoped operation"},
		{"namespace wrong kind", strings.Replace(validSpec, "object: namespace", "object: storage", 1), "storage path parameter"},
		{"storage wrong kind", strings.Replace(validSpec, "object: storage", "object: namespace", 1), "namespace path parameter"},
		{"empty paths", "openapi: 3.0.3\npaths: {}\n", "no paths"},
		{"empty operations", "openapi: 3.0.3\npaths:\n  /version:\n    summary: Version\n", "no operations"},
		{"invalid yaml", "openapi: [", "yaml"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := generate([]byte(test.source)); err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("expected %q error, got %v", test.message, err)
			}
		})
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
	checkedIn, err := os.ReadFile("../../api/rbacmap.gen.go")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(generated, checkedIn) {
		t.Fatal("RBAC map is stale: run go generate ./api/...")
	}
}

func TestRunPreservesGeneratedSourcesAndAnnotatesOnce(t *testing.T) {
	dir := t.TempDir()
	spec := filepath.Join(dir, "openapi.yaml")
	output := filepath.Join(dir, "rbacmap.gen.go")
	source := filepath.Join(dir, "server.gen.go")
	original := []byte("// Code generated by oapi-codegen; DO NOT EDIT.\npackage api\n")
	if err := os.WriteFile(spec, []byte(validSpec), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, original, 0600); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := run(spec, output, []string{source}); err != nil {
			t.Fatal(err)
		}
	}
	annotated, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(annotated, append([]byte(licenseHeader), original...)) {
		t.Fatal("annotation modified source content or repeated the header")
	}
}

func TestAdditionalAuditPermission(t *testing.T) {
	spec := validSpec + `  /events:
    get:
      operationId: listEvents
      x-karakoram-rbac: {resource: events, action: read, object: '*'}
      x-karakoram-rbac-extra: {resource: audit, action: read, object: '*'}
`
	generated, err := generate([]byte(spec))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(generated), `AdditionalRBAC = []RBACRule{`) || !strings.Contains(string(generated), `{Resource: "audit", Action: "read", ObjectParam: "*"},`) {
		t.Fatal("additional audit permission omitted")
	}
	for _, invalid := range []string{
		strings.Replace(spec, "resource: audit", "resource: unknown", 1),
		strings.Replace(spec, "resource: audit, action: read", "resource: audit, action: reveal", 1),
		strings.Replace(spec, "resource: audit, action: read, object: '*'", "resource: audit, action: read, object: cluster", 1),
	} {
		if _, err := generate([]byte(invalid)); err == nil {
			t.Fatal("invalid additional permission accepted")
		}
	}
}

func TestAdditionalScopedPermission(t *testing.T) {
	spec := validSpec + `  /clusters/{cluster}/rabbitmq/users:
    post:
      operationId: createRabbitMQUser
      x-karakoram-rbac: {resource: database-users, action: create, object: cluster}
      x-karakoram-rbac-extra: {resource: clusters, action: update, object: cluster}
`
	generated, err := generate([]byte(spec))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(generated), `OperationAdditionalRBAC = map[string]RBACRule{`) ||
		!strings.Contains(string(generated), `"createRabbitMQUser": {Resource: "clusters", Action: "update", ObjectParam: "cluster"},`) {
		t.Fatal("operation-scoped additional permission omitted")
	}
	for _, object := range []string{"namespace", "storage", "member", ""} {
		invalid := strings.Replace(spec, "resource: clusters, action: update, object: cluster", "resource: clusters, action: update, object: "+object, 1)
		if _, err := generate([]byte(invalid)); err == nil {
			t.Fatalf("unbound additional object %q accepted", object)
		}
	}
	public := strings.Replace(validSpec, "x-karakoram-rbac: public", "x-karakoram-rbac: public\n      x-karakoram-rbac-extra: {resource: clusters, action: update, object: '*'}", 1)
	if _, err := generate([]byte(public)); err == nil {
		t.Fatal("additional permission on public route accepted")
	}
}
