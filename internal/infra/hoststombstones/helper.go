// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package hoststombstones supplies the fixed resolver helper shared by providers.
package hoststombstones

import _ "embed"

// Script accepts only retired hostnames on stdin and updates /etc/hosts.
//
//go:embed hosts-tombstones.sh
var Script string
