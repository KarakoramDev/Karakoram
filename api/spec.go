// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package specification

import _ "embed"

// YAML is the OpenAPI document compiled into the server: it is served to API
// clients and used to validate requests and, in tests, responses.
//
//go:embed openapi.yaml
var YAML []byte
