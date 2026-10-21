// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
)

func TestDataAccessRotationWhitelist(t *testing.T) {
	for _, user := range []string{"karakoram-mcp-ro", "karakoram-mcp-rw"} {
		if err := validateRequest(&rmqspec.SetPasswordRequest{User: user}); err != nil {
			t.Fatal(user, err)
		}
	}
	for _, user := range []string{"", "customer", "karakoram-mcp", "karakoram-mcp-ro-extra", "guest"} {
		if err := validateRequest(&rmqspec.SetPasswordRequest{User: user}); err == nil {
			t.Fatal("accepted unmanaged identity", user)
		}
	}
}
