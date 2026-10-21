// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Command agentgen validates operation safety metadata and generates its lookup.
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
var toolPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
var idPattern = regexp.MustCompile(`^[a-z][A-Za-z0-9]*$`)

type rule struct {
	Tool       string `yaml:"tool"`
	Exposed    bool   `yaml:"exposed"`
	Reason     string `yaml:"reason"`
	Risk       string `yaml:"risk"`
	Confirm    string `yaml:"confirm"`
	Secret     bool   `yaml:"secret"`
	Async      string `yaml:"async"`
	Idempotent bool   `yaml:"idempotent"`
	Untrusted  bool   `yaml:"untrusted"`
}

func main() {
	spec := flag.String("spec", "api/openapi.yaml", "OpenAPI source")
	out := flag.String("out", "internal/api/agentmap.gen.go", "generated agent map")
	flag.Parse()
	if err := run(*spec, *out); err != nil {
		fmt.Fprintln(os.Stderr, "agentgen:", err)
		os.Exit(1)
	}
}
func run(spec, out string) error {
	input, err := os.ReadFile(spec)
	if err != nil {
		return err
	}
	output, err := generate(input)
	if err != nil {
		return err
	}
	return os.WriteFile(out, output, 0644)
}

// Non-GET reads require an explicit reviewed method/path/operation allowlist,
// as idempotency alone does not establish freedom from domain side effects.
// Preview endpoints validate and observe only; authentication observability is
// not a domain side effect. New exceptions require review of their handlers.
func sideEffectFree(id, method, path string) bool {
	switch id {
	case "checkPermission":
		return method == "post" && path == "/rbac/can"
	case "dryRunCreateCluster":
		return method == "post" && path == "/dry-run/clusters"
	case "dryRunReplaceCluster":
		return method == "put" && path == "/dry-run/clusters/{cluster}"
	case "dryRunPatchCluster":
		return method == "patch" && path == "/dry-run/clusters/{cluster}"
	case "dryRunCreateRestore":
		return method == "post" && path == "/dry-run/clusters/{cluster}/restores"
	}
	return false
}

func decodeRule(node yaml.Node) (rule, error) {
	entry := rule{Exposed: true}
	if node.Kind == 0 {
		return entry, fmt.Errorf("missing x-karakoram-agent")
	}
	if node.Kind != yaml.MappingNode {
		return entry, fmt.Errorf("x-karakoram-agent must be a mapping")
	}
	allowed := map[string]string{"tool": "!!str", "exposed": "!!bool", "reason": "!!str", "risk": "!!str", "confirm": "!!str", "secret": "!!bool", "async": "!!str", "idempotent": "!!bool", "untrusted": "!!bool"}
	seen := map[string]bool{}
	for i := 0; i < len(node.Content); i += 2 {
		key, value := node.Content[i], node.Content[i+1]
		tag, ok := allowed[key.Value]
		if !ok {
			return entry, fmt.Errorf("unknown key %q", key.Value)
		}
		if seen[key.Value] {
			return entry, fmt.Errorf("duplicate key %q", key.Value)
		}
		seen[key.Value] = true
		if value.Kind != yaml.ScalarNode || value.Tag != tag {
			return entry, fmt.Errorf("invalid type for %s", key.Value)
		}
	}
	if err := node.Decode(&entry); err != nil {
		return entry, err
	}
	if entry.Exposed {
		if !toolPattern.MatchString(entry.Tool) {
			return entry, fmt.Errorf("invalid or missing tool name %q", entry.Tool)
		}
	} else {
		if strings.TrimSpace(entry.Reason) == "" {
			return entry, fmt.Errorf("exposed: false requires reason")
		}
		if seen["tool"] {
			return entry, fmt.Errorf("exposed: false forbids tool")
		}
	}
	switch entry.Risk {
	case "read", "write":
		if entry.Confirm != "none" {
			return entry, fmt.Errorf("%s risk requires confirm: none", entry.Risk)
		}
	case "destructive":
		if entry.Confirm != "confirm" && entry.Confirm != "name" {
			return entry, fmt.Errorf("destructive risk requires confirm or name")
		}
	default:
		return entry, fmt.Errorf("invalid risk %q", entry.Risk)
	}
	switch entry.Async {
	case "none", "operation", "generation", "record":
	default:
		return entry, fmt.Errorf("invalid async %q", entry.Async)
	}
	return entry, nil
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
		return nil, fmt.Errorf("expected OpenAPI 3.0.3")
	}
	if len(document.Paths) == 0 {
		return nil, fmt.Errorf("specification has no paths")
	}
	rules := map[string]rule{}
	tools := map[string]string{}
	for path, item := range document.Paths {
		for method, node := range item {
			if !methods[method] {
				continue
			}
			var operation struct {
				ID    string    `yaml:"operationId"`
				Agent yaml.Node `yaml:"x-karakoram-agent"`
			}
			if err := node.Decode(&operation); err != nil {
				return nil, fmt.Errorf("%s %s: %w", method, path, err)
			}
			if !idPattern.MatchString(operation.ID) {
				return nil, fmt.Errorf("%s %s: invalid operationId", method, path)
			}
			if _, ok := rules[operation.ID]; ok {
				return nil, fmt.Errorf("duplicate operationId %q", operation.ID)
			}
			entry, err := decodeRule(operation.Agent)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", operation.ID, err)
			}
			if entry.Risk == "read" && method != "get" && (!entry.Idempotent || !sideEffectFree(operation.ID, method, path)) {
				return nil, fmt.Errorf("%s: non-GET read requires idempotent: true and a reviewed side-effect-free operation", operation.ID)
			}
			if entry.Exposed {
				if previous, ok := tools[entry.Tool]; ok {
					return nil, fmt.Errorf("duplicate tool %q (%s, %s)", entry.Tool, previous, operation.ID)
				}
				tools[entry.Tool] = operation.ID
			}
			rules[operation.ID] = entry
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
	var out bytes.Buffer
	out.WriteString(licenseHeader)
	out.WriteString("// Code generated by agentgen from api/openapi.yaml; DO NOT EDIT.\n\npackage api\n\n")
	out.WriteString("// AgentRule declares tool exposure, safety and completion semantics.\ntype AgentRule struct {\nTool string\nExposed bool\nReason string\nRisk string\nConfirm string\nSecret bool\nAsync string\nIdempotent bool\nUntrusted bool\n}\n\n")
	out.WriteString("// OperationAgent is the complete operation-ID safety inventory.\n// Treat it as immutable; a missing operation ID must fail closed.\nvar OperationAgent = map[string]AgentRule{\n")
	for _, id := range ids {
		r := rules[id]
		fmt.Fprintf(&out, "%q: {Tool: %q, Exposed: %t, Reason: %q, Risk: %q, Confirm: %q, Secret: %t, Async: %q, Idempotent: %t, Untrusted: %t},\n", id, r.Tool, r.Exposed, r.Reason, r.Risk, r.Confirm, r.Secret, r.Async, r.Idempotent, r.Untrusted)
	}
	out.WriteString("}\n")
	return format.Source(out.Bytes())
}
