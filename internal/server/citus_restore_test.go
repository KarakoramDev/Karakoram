// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package server

import "testing"

func TestRestorePointSelectorValidation(t *testing.T) {
	s, _, token := testServer(t)
	for _, source := range []string{
		`{"backup":"base","restorePoint":"point"}`,
		`{"pitrTime":"2025-01-15T12:00:00Z","restorePoint":"point"}`,
		`{"backup":"base","pitrTime":"2025-01-15T12:00:00Z","restorePoint":"point"}`,
	} {
		requireDependencyError(t, call(s, "POST", "/api/v1/clusters/prod/restores", `{"name":"recover","source":`+source+`}`, token), 400, "invalid_request", map[string]string{"field": "source", "reason": "conflict"})
	}
	requireDependencyError(t, call(s, "POST", "/api/v1/clusters/prod/restores", `{"name":"recover","source":{"restorePoint":"point"}}`, token), 400, "invalid_request", map[string]string{"field": "source.restorePoint", "reason": "unsupported"})
}
