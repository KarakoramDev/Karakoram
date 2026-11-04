// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestApplicationUserNameBoundaries(t *testing.T) {
	for _, name := range []string{"a", "application-user_1.test", strings.Repeat("a", 63), strings.Repeat("a", 64)} {
		if err := validateUser(name); err != nil {
			t.Fatalf("valid %d-character user rejected", len(name))
		}
	}
	for _, name := range []string{"", strings.Repeat("a", 65), "0first", "with space", "neo4j", "neo4j_reserved", "system_user", "kk_reserved", "karakoram_user"} {
		if err := validateUser(name); !errors.Is(err, engine.ErrInvalid) {
			t.Fatalf("invalid or reserved user accepted: %q", name)
		}
	}
}
func TestAccessErrorsPreserveAgentUserClassifications(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   error
	}{{http.StatusBadRequest, engine.ErrInvalid}, {http.StatusUnprocessableEntity, engine.ErrInvalid}, {http.StatusNotFound, engine.ErrNotFound}, {http.StatusConflict, engine.ErrConflict}} {
		got := accessError(&agentapi.HTTPError{StatusCode: tc.status, Message: "sensitive native diagnostic"})
		if !errors.Is(got, tc.want) || strings.Contains(got.Error(), "sensitive") {
			t.Fatalf("HTTP %d mapping failed", tc.status)
		}
	}
}
