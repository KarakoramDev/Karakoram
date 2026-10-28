// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"testing"
)

func TestGeneratedCookieNativeContract(t *testing.T) {
	// Erlang distribution cookies are nonempty strings up to 255 characters;
	// RabbitMQ recommends generated alphanumeric cookies.
	// https://www.rabbitmq.com/docs/clustering#erlang-cookie
	// Exercise the actual config.go generator, not secrets.Password.
	for range 16 {
		value, err := cookie()
		if err != nil {
			t.Fatal(err)
		}
		generatedCookieLimit.Assert(t, "generated cookie", value)
	}
}
