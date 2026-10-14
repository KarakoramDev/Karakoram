// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
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

// Field and resource errors carry machine-readable details naming the field
// or resource, and the details object is omitted when there is nothing to add.
func TestFailureReportsFieldAndResourceDetails(t *testing.T) {
	s := &Server{}
	cases := []struct {
		err     error
		status  int
		code    string
		details map[string]any
	}{
		{engine.InvalidField("spec.components.engine.replicas", domain.ReasonOutOfRange, "spec.components.engine.replicas must be at least 1"), 400, "invalid_request",
			map[string]any{"field": "spec.components.engine.replicas", "reason": "out_of_range"}},
		{fmt.Errorf("wrap: %w", domain.InvalidField("newPassword", domain.ReasonReused, "newPassword must differ from the current password")), 400, "invalid_request",
			map[string]any{"field": "newPassword", "reason": "reused"}},
		{domain.NotFound("backupStorage", "missing", store.ErrNotFound), 404, "not_found",
			map[string]any{"resource": "backupStorage", "name": "missing", "reason": "not_found"}},
		{&domain.ResourceError{Resource: "backup", Name: "b1", Reason: domain.ReasonInvalidState, State: "Pending", Message: "backup b1 is Pending"}, 409, "conflict",
			map[string]any{"resource": "backup", "name": "b1", "reason": "invalid_state", "state": "Pending"}},
		{engine.Invalid("plain"), 400, "invalid_request", nil},
	}
	for _, c := range cases {
		r := s.failure(c.err)
		body, _ := r.body.(api.Error)
		if r.status != c.status || body.Code != c.code || body.Message == "" {
			t.Fatalf("failure(%v) = %d %+v", c.err, r.status, body)
		}
		if c.details == nil {
			if body.Details != nil {
				t.Fatalf("failure(%v) details = %v, want omitted", c.err, *body.Details)
			}
			continue
		}
		if body.Details == nil || fmt.Sprint(*body.Details) != fmt.Sprint(c.details) {
			t.Fatalf("failure(%v) details = %v, want %v", c.err, body.Details, c.details)
		}
	}
	if raw, _ := json.Marshal(api.Error{Code: "x", Message: "y"}); string(raw) != `{"code":"x","message":"y"}` {
		t.Fatalf("empty details serialized as %s", raw)
	}
}
