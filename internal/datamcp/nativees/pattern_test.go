// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nativees

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestSharedIndexPatternValidation(t *testing.T) {
	for _, pattern := range []string{"*", "?", "app-*", "events-2026.10", "logs_2026", strings.Repeat("x", 255)} {
		if !validPattern(pattern) || !engine.ValidElasticsearchDataIndexPattern(pattern) {
			t.Errorf("rejected valid pattern %q", pattern)
		}
	}
	for _, pattern := range []string{"", ".hidden", ".*", "_all", "_logs", "-logs", "+logs", "Logs", "résumé-*", "a+b", "a[b]", "a/b", `a\b`, "a:b", "a,b", "a#b", "a b", "a\tb", "a\x00b", "<logs>", `a"b`, "a|b", strings.Repeat("x", 256), string([]byte{0xff})} {
		if validPattern(pattern) || engine.ValidElasticsearchDataIndexPattern(pattern) {
			t.Errorf("accepted unsafe pattern %q", pattern)
		}
	}
}
