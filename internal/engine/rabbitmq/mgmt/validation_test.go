// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mgmt

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestPolicyValidationDiagnosticsDoNotEchoNativeBodies(t *testing.T) {
	for _, tc := range []struct{ reason, field string }{
		{`Validation failed: private-key is not a recognised policy setting; private-secret`, "definition"},
		{`Validation failed: private-key is not a recognized policy setting; private-secret`, "definition"},
		{`invalid regular expression: [private-secret`, "pattern"},
		{`unclassified private-secret`, "pattern,definition"},
	} {
		for _, status := range []int{400, 422} {
			t.Run(tc.field+http.StatusText(status), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(status)
					json.NewEncoder(w).Encode(map[string]string{"error": "bad_request", "reason": tc.reason})
				}))
				defer server.Close()
				client, err := New(server.URL, "admin", "private-secret", nil)
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				for _, write := range []func() error{
					func() error { return client.PutPolicy(t.Context(), Policy{Vhost: "/", Name: "app"}) },
					func() error { return client.PutOperatorPolicy(t.Context(), Policy{Vhost: "/", Name: "app"}) },
				} {
					err := write()
					var field *engine.ValidationError
					var native *HTTPError
					if !errors.As(err, &field) || field.Field != tc.field || !errors.Is(err, engine.ErrInvalid) || !errors.As(err, &native) || native.StatusCode != status {
						t.Fatalf("unexpected typed native error: %#v", err)
					}
					if strings.Contains(err.Error(), "private-") || strings.Contains(field.Message, "private-") {
						t.Fatal("native payload leaked", err)
					}
				}
			})
		}
	}
}
