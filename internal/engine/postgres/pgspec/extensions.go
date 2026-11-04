// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package pgspec

import "regexp"

// ExtensionNamePattern admits catalog extension names, including uuid-ossp,
// but not SQL fragments, qualified identifiers or quoting.
const ExtensionNamePattern = `^[a-zA-Z_][a-zA-Z0-9_-]{0,62}$`

var extensionName = regexp.MustCompile(ExtensionNamePattern)

func ValidExtensionName(name string) bool { return extensionName.MatchString(name) }
