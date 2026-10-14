// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestRecordedErrorDiagnostics(t *testing.T) {
	autoscaling, err := os.ReadFile("testdata/autoscaling-error.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		body []byte
		want []string
	}{
		{"validation", autoscaling, []string{"invalidCustomAutoscalingValue", "minFreeResource.memoryGBytes", "no more than 3 decimal places"}},
		{"array", []byte(`{"error":{"code":"importFailed","message":"See metadata","meta":[{"code":"invalidResource","error":"See metadata","metadata":{"minDisk":["Value must be at least 5"]}}]}}`), []string{"importFailed", "invalidResource", "minDisk", "Value must be at least 5"}},
		{"object", []byte(`{"error":{"code":"serviceStackNotFound","message":"See metadata","meta":{"field":"serviceStackId","reason":"Service no longer exists"}}}`), []string{"serviceStackNotFound", "serviceStackId", "Service no longer exists"}},
		{"metadata", []byte(`{"error":{"code":"importFailed","message":"See metadata","metadata":{"minDisk":["Value must be at least 5"]}}}`), []string{"importFailed", "minDisk", "Value must be at least 5"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write(tc.body)
			})
			_, err := b.client.ImportServices(context.Background(), "services: []")
			if err == nil {
				t.Fatal("missing platform failure")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("missing %q in %s", want, err)
				}
			}
			if strings.Contains(err.Error(), "See metadata") {
				t.Fatalf("unresolved metadata reference: %s", err)
			}
		})
	}
}

func TestRecordedProcessDiagnostics(t *testing.T) {
	var failed Process
	if err := json.Unmarshal(fixture(t, "failed"), &failed); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []json.RawMessage{
		failed.Error,
		json.RawMessage(`{"code":"deployFailed","message":"See metadata","meta":[{"metadata":{"minDisk":["Value must be at least 5"]}}]}`),
	} {
		failed.Error = raw
		b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(failed)
		})
		state, err := b.OpStatus(context.Background(), infra.Op{ID: "failed-process"})
		if err != nil || !state.Failed || state.Message == "" || strings.Contains(state.Message, "See metadata") {
			t.Fatalf("process diagnostic: %+v %v", state, err)
		}
		if !strings.Contains(state.Message, "containerName") && !strings.Contains(state.Message, "minDisk") {
			t.Fatalf("metadata omitted: %s", state.Message)
		}
	}
}

func TestDiagnosticRedactionAndBounds(t *testing.T) {
	raw := json.RawMessage(`[{"metadata":{"minDisk":["Value must be at least 5"],"token":"do-not-show-token","envSecrets":{"PASSWORD":"do-not-show-password"},"reason":"Bearer do-not-show-bearer","endpoint":"https://user:password@example.invalid/?token=do-not-show-url","resourceName":"test-token","detail":"` + strings.Repeat("a", 80) + `"}},{"field":"envSecrets.PASSWORD","reason":"do-not-show-unmarked-value"}]`)
	e := &APIError{StatusCode: 400, Code: "deployFailed", Message: "See metadata", Meta: raw, token: "test-token"}
	if !strings.Contains(e.Error(), "minDisk: Value must be at least 5") {
		t.Fatalf("safe metadata disappeared: %s", e.Error())
	}
	for _, detail := range []string{e.Error(), processErrorSummary(json.RawMessage(`"raw secret data"`), "test-token")} {
		for _, secret := range []string{"do-not-show", "test-token", "raw secret data", strings.Repeat("a", 80)} {
			if strings.Contains(detail, secret) {
				t.Fatalf("sensitive response escaped: %s", detail)
			}
		}
	}
	e.Meta = json.RawMessage(`{"reason":"` + strings.Repeat("a short reason ", 300) + `"}`)
	if detail := e.Error(); len(detail) > diagnosticLimit || !strings.HasSuffix(detail, "...") {
		t.Fatalf("unbounded detail (%d): %s", len(detail), detail)
	}
}

func TestValidateProjectPreservesFailureClassification(t *testing.T) {
	for _, status := range []int{400, 401, 403, 429, 500, 503} {
		for _, services := range []bool{false, true} {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if services && !strings.HasSuffix(r.URL.Path, "/service-stack") {
					_, _ = w.Write([]byte(`{"id":"project"}`))
					return
				}
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"error":{"code":"platformFailure","message":"See metadata","meta":{"reason":"Try again later"}}}`))
			}))
			_, _, err := ValidateProject(context.Background(), Options{BaseURL: s.URL, Token: "private-token", ProjectID: "project", MaxRetries: -1})
			s.Close()
			var api *APIError
			if !errors.As(err, &api) || api.StatusCode != status || IsAuthenticationError(err) != (status == 401 || status == 403) {
				t.Fatalf("status=%d services=%t classification lost: %v", status, services, err)
			}
		}
	}
	if !IsAuthenticationError(&APIError{StatusCode: 400, Code: "tokenExpired"}) {
		t.Fatal("explicit authentication code was not classified")
	}
}
