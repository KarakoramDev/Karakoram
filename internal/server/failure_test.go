// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"fmt"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// Users must learn which field a rejected request got wrong; bare sentinels
// (which may stand for unsanitized input) keep the generic message.
func TestFailureExposesValidationMessageOnly(t *testing.T) {
	s := &Server{}
	cases := []struct {
		err  error
		want string
	}{
		{engine.Invalid("public exposure requires an IP allowlist"), "public exposure requires an IP allowlist"},
		{fmt.Errorf("create: %w", engine.Invalid("shards must be 1..128")), "shards must be 1..128"},
		{engine.ErrInvalid, "Invalid request"},
		{fmt.Errorf("MongoDB command failed (code 2, BadValue): %w", engine.ErrInvalid), "Invalid request"},
		{auth.ErrInvalid, "Invalid request"},
	}
	for _, c := range cases {
		r := s.failure(c.err)
		body, _ := r.body.(api.Error)
		if r.status != 400 || body.Code != "invalid_request" || body.Message != c.want {
			t.Errorf("failure(%v) = %d %+v, want 400 invalid_request %q", c.err, r.status, body, c.want)
		}
	}
}
