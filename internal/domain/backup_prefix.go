// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package domain

import (
	"path"
	"strings"
)

// BackupStoragePrefix maps a binding prefix to its relative object namespace.
// Boundary slashes are optional; inner components are never cleaned or collapsed.
func BackupStoragePrefix(prefix string) string {
	return strings.Trim(prefix, "/")
}

// ValidateBackupStoragePrefix rejects ambiguous namespaces before registration
// and before direct object cleanup. A slash-only prefix denotes the bucket root.
func ValidateBackupStoragePrefix(prefix string) error {
	prefix = BackupStoragePrefix(prefix)
	if prefix == "" {
		return nil
	}
	for _, part := range strings.Split(prefix, "/") {
		if part == "" || part == "." || part == ".." || strings.Contains(part, "\\") {
			return InvalidField("prefix", ReasonInvalid, "prefix must contain safe object namespace components without empty inner components, dot components or backslashes")
		}
	}
	return nil
}

// BackupObjectPrefix appends an engine-owned namespace without filesystem path
// cleaning, so normalization cannot broaden an incarnation's ownership boundary.
func BackupObjectPrefix(prefix, namespace string) string {
	prefix = BackupStoragePrefix(prefix)
	namespace = BackupStoragePrefix(namespace)
	if prefix == "" {
		return namespace
	}
	return prefix + "/" + namespace
}

// PostgresObjectPrefix preserves pgBackRest repository identities for persisted
// bindings, including the historical filesystem cleaning of inner components.
// New registrations must still pass ValidateBackupStoragePrefix.
func PostgresObjectPrefix(prefix, namespace string) string {
	return path.Join(BackupStoragePrefix(prefix), namespace)
}

// ElasticsearchObjectPrefix preserves the historical removal of empty slash
// components, without interpreting dot components as filesystem traversal.
// Writers and cleanup must use the same mapping for already-persisted bindings.
func ElasticsearchObjectPrefix(prefix, namespace string) string {
	return strings.Join(strings.FieldsFunc(prefix+"/"+namespace, func(r rune) bool { return r == '/' }), "/")
}
