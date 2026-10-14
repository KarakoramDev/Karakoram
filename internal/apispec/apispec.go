// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package apispec parses the embedded public OpenAPI document once for request
// validation, response-conformance tests and serving it to clients.
package apispec

import (
	"context"
	"fmt"
	"sync"

	specification "github.com/KarakoramDev/Karakoram/api"
	"github.com/getkin/kin-openapi/openapi3"
)

var load = sync.OnceValues(func() (*openapi3.T, error) {
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromData(specification.YAML)
	if err != nil {
		return nil, fmt.Errorf("parse OpenAPI document: %w", err)
	}
	if err := doc.Validate(context.Background()); err != nil {
		return nil, fmt.Errorf("invalid OpenAPI document: %w", err)
	}
	return doc, nil
})

// Load returns the parsed, validated OpenAPI document. Callers must not modify it.
func Load() (*openapi3.T, error) { return load() }

// YAML returns the raw embedded OpenAPI document.
func YAML() []byte { return specification.YAML }
