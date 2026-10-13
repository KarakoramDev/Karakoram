// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Command rbacgen validates operation authorization metadata and generates the
// operation-ID lookup consumed by the API authorization middleware.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/format"
	"os"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const licenseHeader = "// SPDX-License-Identifier: Apache-2.0\n// Copyright 2026 The Karakoram Authors\n\n"

var methods = map[string]bool{"get": true, "put": true, "post": true, "delete": true, "patch": true, "head": true, "options": true, "trace": true}
var resources = map[string]bool{"audit": true, "namespaces": true, "clusters": true, "cluster-credentials": true, "database-users": true, "backups": true, "restores": true, "backup-storages": true, "operations": true, "events": true, "monitoring": true, "accounts": true, "api-tokens": true, "rbac": true, "settings": true, "providers": true}
var actions = map[string]bool{"create": true, "read": true, "update": true, "delete": true, "*": true}
var operationIDPattern = regexp.MustCompile(`^[a-z][A-Za-z0-9]*$`)

type rule struct {
	Resource string `yaml:"resource"`
	Action   string `yaml:"action"`
	Object   string `yaml:"object"`
	Public   bool   `yaml:"-"`
}

type sourceFiles []string

func (s *sourceFiles) String() string { return strings.Join(*s, ",") }
func (s *sourceFiles) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	specPath := flag.String("spec", "api/openapi.yaml", "OpenAPI source")
	outputPath := flag.String("out", "internal/api/rbacmap.gen.go", "generated RBAC map")
	var sources sourceFiles
	flag.Var(&sources, "source", "generated Go source to annotate with SPDX (repeatable)")
	flag.Parse()
	if err := run(*specPath, *outputPath, sources); err != nil {
		fmt.Fprintln(os.Stderr, "rbacgen:", err)
		os.Exit(1)
	}
}

func run(specPath, outputPath string, sources []string) error {
	input, err := os.ReadFile(specPath)
	if err != nil {
		return err
	}
	output, err := generate(input)
	if err != nil {
		return err
	}
	for _, path := range sources {
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.HasPrefix(content, []byte(licenseHeader)) {
			continue
		}
		if err := os.WriteFile(path, append([]byte(licenseHeader), content...), 0644); err != nil {
			return err
		}
	}
	return os.WriteFile(outputPath, output, 0644)
}

func generate(input []byte) ([]byte, error) {
	var document struct {
		OpenAPI string                          `yaml:"openapi"`
		Paths   map[string]map[string]yaml.Node `yaml:"paths"`
	}
	if err := yaml.Unmarshal(input, &document); err != nil {
		return nil, err
	}
	if document.OpenAPI != "3.0.3" {
		return nil, fmt.Errorf("expected OpenAPI 3.0.3, got %q", document.OpenAPI)
	}
	if len(document.Paths) == 0 {
		return nil, fmt.Errorf("specification has no paths")
	}
	rules := make(map[string]rule)
	extraRules := make(map[string]rule)
	for path, item := range document.Paths {
		for method, node := range item {
			if !methods[method] {
				continue
			}
			var operation struct {
				ID       string    `yaml:"operationId"`
				RBAC     yaml.Node `yaml:"x-karakoram-rbac"`
				Security yaml.Node `yaml:"security"`
				Extra    *rule     `yaml:"x-karakoram-rbac-extra"`
			}
			if err := node.Decode(&operation); err != nil {
				return nil, fmt.Errorf("%s %s: %w", method, path, err)
			}
			if !operationIDPattern.MatchString(operation.ID) {
				return nil, fmt.Errorf("%s %s: missing or non-camelCase operationId %q", method, path, operation.ID)
			}
			if _, exists := rules[operation.ID]; exists {
				return nil, fmt.Errorf("duplicate operationId %q", operation.ID)
			}
			var entry rule
			switch operation.RBAC.Kind {
			case yaml.ScalarNode:
				if operation.RBAC.Value != "public" {
					return nil, fmt.Errorf("%s: RBAC scalar must be public", operation.ID)
				}
				if operation.Security.Kind != yaml.SequenceNode || len(operation.Security.Content) != 0 {
					return nil, fmt.Errorf("%s: public operations must explicitly set security: []", operation.ID)
				}
				entry.Public = true
			case yaml.MappingNode:
				if err := operation.RBAC.Decode(&entry); err != nil {
					return nil, fmt.Errorf("%s: %w", operation.ID, err)
				}
				if !resources[entry.Resource] || !actions[entry.Action] {
					return nil, fmt.Errorf("%s: unknown resource/action %q/%q", operation.ID, entry.Resource, entry.Action)
				}
				switch entry.Object {
				case "*":
				case "cluster", "namespace", "storage":
					if !strings.Contains(path, "{"+entry.Object+"}") {
						return nil, fmt.Errorf("%s: object must be bound to the %s path parameter", operation.ID, entry.Object)
					}
				default:
					return nil, fmt.Errorf("%s: object must be *, cluster, namespace or storage", operation.ID)
				}
				for _, parameter := range []string{"cluster", "namespace", "storage"} {
					if strings.Contains(path, "{"+parameter+"}") && entry.Object != parameter {
						return nil, fmt.Errorf("%s: %s-scoped operation must use object: %s", operation.ID, parameter, parameter)
					}
				}
			default:
				return nil, fmt.Errorf("%s: missing x-karakoram-rbac", operation.ID)
			}
			rules[operation.ID] = entry
			if extra := operation.Extra; extra != nil {
				if !resources[extra.Resource] || !actions[extra.Action] || extra.Object != "*" {
					return nil, fmt.Errorf("%s: invalid additional RBAC resource/action/object", operation.ID)
				}
				extraRules[extra.Resource+":"+extra.Action] = *extra
			}
		}
	}
	if len(rules) == 0 {
		return nil, fmt.Errorf("specification has no operations")
	}
	ids := make([]string, 0, len(rules))
	for id := range rules {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var output bytes.Buffer
	output.WriteString(licenseHeader)
	output.WriteString("// Code generated by rbacgen from api/openapi.yaml; DO NOT EDIT.\n\npackage api\n\n")
	output.WriteString("// RBACRule declares authorization for a generated operation. ObjectParam is\n// cluster, namespace or storage for the corresponding path parameter, or *\n// for a global route. Cluster and storage objects include the namespace query\n// (defaulting to default): namespace/name. Namespace objects use their path name.\n// Collections must additionally filter each item against its persisted object.\n// Public routes require no authentication. Authenticated self-service routes\n// are handled by the identity layer before administrative policy enforcement.\ntype RBACRule struct {\nResource string\nAction string\nObjectParam string\nPublic bool\n}\n\n")
	output.WriteString("// OperationRBAC is the complete OpenAPI operation-ID authorization inventory.\n// Treat it as immutable; a missing operation ID must fail closed.\nvar OperationRBAC = map[string]RBACRule{\n")
	for _, id := range ids {
		entry := rules[id]
		fmt.Fprintf(&output, "%q: {Resource: %q, Action: %q, ObjectParam: %q, Public: %t},\n", id, entry.Resource, entry.Action, entry.Object, entry.Public)
	}
	output.WriteString("}\n")
	extraIDs := make([]string, 0, len(extraRules))
	for id := range extraRules {
		extraIDs = append(extraIDs, id)
	}
	sort.Strings(extraIDs)
	output.WriteString("\n// AdditionalRBAC declares per-result permissions beyond operation-level grants.\nvar AdditionalRBAC = []RBACRule{\n")
	for _, id := range extraIDs {
		entry := extraRules[id]
		fmt.Fprintf(&output, "{Resource: %q, Action: %q, ObjectParam: %q},\n", entry.Resource, entry.Action, entry.Object)
	}
	output.WriteString("}\n")
	return format.Source(output.Bytes())
}
