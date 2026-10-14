// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Command specnoalias expands YAML aliases for consumers with safe default
// alias-expansion limits. Response aliases retain their existing OpenAPI $refs.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"

	"github.com/oasdiff/yaml3"
)

func main() {
	check := flag.Bool("check", false, "fail if the specification contains YAML anchors or aliases")
	path := flag.String("path", "api/openapi.yaml", "OpenAPI document to check or rewrite")
	flag.Parse()
	if err := run(*path, *check); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(path string, check bool) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return err
	}
	if check {
		if hasAliases(&doc) {
			return fmt.Errorf("%s contains YAML anchors/aliases; run go run ./internal/tools/specnoalias -path %s", path, path)
		}
		return nil
	}
	expanded, err := expand(&doc, make(map[*yaml.Node]bool), false)
	if err != nil {
		return err
	}
	var output bytes.Buffer
	encoder := yaml.NewEncoder(&output)
	encoder.SetIndent(2)
	if err := encoder.Encode(expanded); err != nil {
		return err
	}
	if err := encoder.Close(); err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	return os.WriteFile(path, output.Bytes(), info.Mode().Perm())
}

func hasAliases(n *yaml.Node) bool {
	if n.Anchor != "" || n.Kind == yaml.AliasNode {
		return true
	}
	for _, child := range n.Content {
		if hasAliases(child) {
			return true
		}
	}
	return false
}

// Only alias targets need copies; ordinary document nodes are rewritten in place.
func expand(n *yaml.Node, visiting map[*yaml.Node]bool, clone bool) (*yaml.Node, error) {
	if n == nil {
		return nil, fmt.Errorf("YAML alias has no target")
	}
	if visiting[n] {
		return nil, fmt.Errorf("cyclic YAML alias cannot be expanded")
	}
	visiting[n] = true
	defer delete(visiting, n)
	if n.Kind == yaml.AliasNode {
		expanded, err := expand(n.Alias, visiting, true)
		if err != nil {
			return nil, err
		}
		// Reference-site comments belong to the copied node, not its declaration.
		expanded.HeadComment = n.HeadComment
		expanded.LineComment = n.LineComment
		expanded.FootComment = n.FootComment
		return expanded, nil
	}
	result := n
	if clone {
		copied := *n
		copied.Content = append([]*yaml.Node(nil), n.Content...)
		result = &copied
	}
	result.Anchor = ""
	result.Alias = nil
	for i, child := range result.Content {
		var err error
		result.Content[i], err = expand(child, visiting, clone)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
