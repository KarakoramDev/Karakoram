// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package specification owns the public API source and its reproducible generators.
// Run from the repository root: go generate ./api/...
package specification

//go:generate go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.4.1 --config oapi-codegen.server.yaml openapi.yaml
//go:generate go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.4.1 --config oapi-codegen.client.yaml openapi.yaml
//go:generate go run ../internal/tools/rbacgen -spec openapi.yaml -out ../internal/api/rbacmap.gen.go -source ../internal/api/gen.go -source ../pkg/client/client.gen.go
//go:generate go run ../internal/tools/agentgen -spec openapi.yaml -out ../internal/api/agentmap.gen.go
