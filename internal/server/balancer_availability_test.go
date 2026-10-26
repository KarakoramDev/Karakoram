// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
)

type balancerReadProvider struct {
	fakeProvider
	err error
}

func (p *balancerReadProvider) Balancer(context.Context, *engine.Context) (domain.BalancerStatus, error) {
	return domain.BalancerStatus{Enabled: true, Running: true, Mode: "full"}, mongoadmin.ReadError(p.err)
}

func TestBalancerReadAvailabilityAndFailureLog(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"success", nil, 200, ""},
		{"stopped mongos", topology.ServerSelectionError{}, 503, "unavailable"},
		{"shutdown", mongo.CommandError{Code: 91}, 503, "unavailable"},
		{"auth", mongo.CommandError{Code: 18, Message: "authentication failed"}, 500, "internal_error"},
		{"command", errors.New("native command failed mongodb://admin:private-uri-value@router password=private-field-value"), 500, "internal_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, token := testServer(t)
			var logs bytes.Buffer
			s.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
			registry, err := engine.NewRegistry(&balancerReadProvider{err: tc.err})
			if err != nil {
				t.Fatal(err)
			}
			s.Registry = registry
			w := call(s, "GET", "/api/v1/clusters/prod/balancer", "", token)
			if w.Code != tc.status {
				t.Fatalf("status %d: %s", w.Code, w.Body.String())
			}
			if tc.code != "" {
				var body api.Error
				if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body.Code != tc.code {
					t.Fatalf("body: %s (%v)", w.Body.String(), err)
				}
			} else if !strings.Contains(w.Body.String(), `"mode":"full"`) {
				t.Fatalf("success body: %s", w.Body.String())
			}
			if tc.status == 500 {
				found := false
				for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
					var record map[string]any
					if json.Unmarshal(line, &record) != nil || record["msg"] != "API operation failed" {
						continue
					}
					found = true
					if record["error"] == "" || record["error"] == nil || record["request_id"] != w.Header().Get("X-Request-ID") || record["operation"] != "GetClusterBalancer" {
						t.Fatalf("missing diagnostic fields: %v", record)
					}
				}
				if !found {
					t.Fatalf("missing failure log: %s", logs.String())
				}
			}
			for _, secret := range []string{"private-uri-value", "private-field-value"} {
				if strings.Contains(logs.String(), secret) || strings.Contains(w.Body.String(), secret) {
					t.Fatalf("credential leaked: %s %s", logs.String(), w.Body.String())
				}
			}
		})
	}
}
