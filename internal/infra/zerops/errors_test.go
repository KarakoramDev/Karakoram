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
	"unicode/utf8"

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
		if !strings.Contains(state.Message, "containerAction") && !strings.Contains(state.Message, "minDisk") {
			t.Fatalf("safe diagnostic omitted: %s", state.Message)
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

func TestDiagnosticMetadataExportsOnlySafeKeys(t *testing.T) {
	for _, key := range []string{"pwd", "pass", "jwt", "key", "hash", "host", "email", "containerName", "unexpected"} {
		t.Run(key, func(t *testing.T) {
			meta, err := json.Marshal(map[string]any{
				"field": "minDisk", "reason": "Value must be at least 5",
				key: "private-value-123",
			})
			if err != nil {
				t.Fatal(err)
			}
			e := &APIError{StatusCode: 400, Code: "deployFailed", Message: "See metadata", Meta: meta}
			process, err := json.Marshal(map[string]any{"code": "deployFailed", "metadata": json.RawMessage(meta)})
			if err != nil {
				t.Fatal(err)
			}
			for _, detail := range []string{e.Error(), processErrorSummary(process, "")} {
				if strings.Contains(detail, "private-value-123") || strings.Contains(detail, key+":") {
					t.Fatalf("unapproved metadata escaped: %s", detail)
				}
				if !strings.Contains(detail, "field: minDisk") || !strings.Contains(detail, "reason: Value must be at least 5") {
					t.Fatalf("safe diagnostic disappeared: %s", detail)
				}
			}
		})
	}
	meta := json.RawMessage(`{"minDisk":5,"maxDisk":100,"min":1,"max":10,"minFreeResource.memoryGBytes":["Value must have no more than 3 decimal places"],"unexpected":{"reason":"private-nested-value"}}`)
	detail := errorSummary("invalidResource", "", meta, "")
	for _, want := range []string{"minDisk: 5", "maxDisk: 100", "min: 1", "max: 10", "minFreeResource.memoryGBytes: Value must have no more than 3 decimal places"} {
		if !strings.Contains(detail, want) {
			t.Fatalf("validation limit disappeared: %s", detail)
		}
	}
	if strings.Contains(detail, "private-nested-value") {
		t.Fatalf("unapproved nested metadata escaped: %s", detail)
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

func validationFixture(t *testing.T, name string) json.RawMessage {
	t.Helper()
	data, err := os.ReadFile("testdata/autoscaling-validation-errors.json")
	if err != nil {
		t.Fatal(err)
	}
	var recorded map[string]json.RawMessage
	if err := json.Unmarshal(data, &recorded); err != nil {
		t.Fatal(err)
	}
	if recorded[name] == nil {
		t.Fatalf("missing validation fixture %q", name)
	}
	return recorded[name]
}

func TestRecordedAutoscalingFieldDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, field, message string
	}{
		{"startCpuBelowMin", "startCpuCoreCount", "Value must not be less than minResource.cpuCoreCount"},
		{"minRamAboveMax", "minResource.memoryGBytes", "Value must not be greater than maxResource.memoryGBytes"},
		{"cpuAboveMax", "maxResource.cpuCoreCount", "Value must not be greater than [88]"},
		{"diskBelowMin", "maxResource.diskGBytes", "Value must not be less than [1.0]"},
		{"startCpuAboveMax", "startCpuCoreCount", "Value must not be greater than maxResource.cpuCoreCount"},
		{"containersBelowMin", "minContainerCount", "Value must not be less than [1]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := validationFixture(t, tc.name)
			b := newTestBackend(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write(body)
			})
			_, err := b.client.ImportServices(t.Context(), "services: []")
			if err == nil {
				t.Fatal("missing validation failure")
			}
			var response struct{ Error json.RawMessage }
			if err := json.Unmarshal(body, &response); err != nil {
				t.Fatal(err)
			}
			for _, detail := range []string{err.Error(), processErrorSummary(response.Error, "")} {
				for _, want := range []string{"invalidCustomAutoscalingValue", tc.field + ": " + tc.message} {
					if !strings.Contains(detail, want) {
						t.Fatalf("missing %q in %s", want, detail)
					}
				}
			}
		})
	}
}

func TestValidationParameterValuesAndRedaction(t *testing.T) {
	meta := json.RawMessage(`[
		{"parameter":"verticalAutoscaling.startCpuCoreCount","min":2,"max":4,"value":1,"provided":1,"actual":1},
		{"parameter":"verticalAutoscaling.cpuMode","value":"SHARED"},
		{"parameter":"envSecrets.PASSWORD","value":"do-not-show-password","message":"do-not-show-message"},
		{"field":"envVariables.DATA","actual":"do-not-show-env"},
		{"value":"do-not-show-unattributed"},
		{"actual":{"reason":"do-not-show-nested"}},
		{"metadata":{"minResource.password":["do-not-show-field"],"maxResource.unexpected":["do-not-show-unknown"]}},
		{"reason":"https://example.invalid/signed?key=do-not-show-url"},
		{"parameter":"startCpuCoreCount","value":"` + strings.Repeat("a", 80) + `"}
	]`)
	detail := errorSummary("invalidCustomAutoscalingValue", "", meta, "")
	for _, want := range []string{"parameter: verticalAutoscaling.startCpuCoreCount", "min: 2", "max: 4", "value: 1", "provided: 1", "actual: 1", "value: SHARED"} {
		if !strings.Contains(detail, want) {
			t.Fatalf("missing %q in %s", want, detail)
		}
	}
	if strings.Contains(detail, "do-not-show") || strings.Contains(detail, strings.Repeat("a", 80)) {
		t.Fatalf("sensitive metadata escaped: %s", detail)
	}
}

func TestResourceRejectionClassificationAndBounds(t *testing.T) {
	for _, code := range []string{"lastCpuModeChangeTooRecent", "tokenExpired", "invalidCustomAutoscalingValue"} {
		api := &APIError{StatusCode: 400, Code: code, Message: strings.Repeat("界 ", 1000)}
		err := rejectedUpdate(api, "resource update", "dbzero", "cpuMode=SHARED cpu=2-2 startCpu=1")
		var got *APIError
		var coded interface{ ErrorCode() string }
		if !errors.As(err, &got) || got != api || !errors.As(err, &coded) || coded.ErrorCode() != code {
			t.Fatalf("classification lost: %v", err)
		}
		if IsAuthenticationError(err) != (code == "tokenExpired") {
			t.Fatalf("authentication classification lost: %v", err)
		}
		detail := err.Error()
		if len(detail) > diagnosticLimit || !utf8.ValidString(detail) || !strings.Contains(detail, "; attempted cpuMode=SHARED cpu=2-2 startCpu=1") {
			t.Fatalf("unbounded or incomplete rejection (%d): %s", len(detail), detail)
		}
	}
	transport := errors.New("connection failed")
	if rejectedUpdate(transport, "resource update", "dbzero", "") != transport {
		t.Fatal("transport failure mislabeled as rejection")
	}
}

func TestResourceMetadataRejectsCredentialShapedStrings(t *testing.T) {
	for _, payload := range []string{
		"DB_HOST=db.internal;DB_PASS=hunter2",
		"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkYXRhIiwiaWF0IjoxMjM0NTY3ODkwfQ.c2lnbmF0dXJl",
		"ghp_abcdefghijklmnopqrstuvwxyz0123456789",
		"Value must not contain DB_HOST=db.internal",
		"Value must use abc:def",
		"Value must use abc/def",
		"Value must use abc;def",
		"Value must " + strings.Repeat("a short sentence ", 20),
	} {
		for _, key := range []string{"value", "provided", "actual", "startCpuCoreCount", "minResource.memoryGBytes"} {
			t.Run(key+"/"+payload[:min(len(payload), 12)], func(t *testing.T) {
				meta, err := json.Marshal(map[string]any{"field": "cpuMode", key: payload})
				if err != nil {
					t.Fatal(err)
				}
				api := &APIError{StatusCode: 400, Code: "invalidCustomAutoscalingValue", Meta: meta}
				raw, err := json.Marshal(map[string]any{"code": api.Code, "meta": json.RawMessage(meta)})
				if err != nil {
					t.Fatal(err)
				}
				for _, detail := range []string{api.Error(), processErrorSummary(raw, "")} {
					if strings.Contains(detail, key+":") || strings.Contains(detail, payload) {
						t.Fatalf("unsafe resource metadata escaped: %s", detail)
					}
					if !strings.Contains(detail, "invalidCustomAutoscalingValue") {
						t.Fatalf("safe code disappeared: %s", detail)
					}
				}
			})
		}
	}
	meta := json.RawMessage(`{"startCpuCoreCount":{"reason":"unmarked-sensitive-data"},"maxResource.cpuCoreCount":["Value must use abcdefghijklmnopqrst"]}`)
	if detail := errorSummary("invalidCustomAutoscalingValue", "", meta, ""); strings.Contains(detail, "sensitive-data") || strings.Contains(detail, "abcdefghijklmnopqrst") {
		t.Fatalf("nested or opaque validation metadata escaped: %s", detail)
	}
}

func TestMetadataOmitsRepeatedPrimaryDiagnostics(t *testing.T) {
	meta := json.RawMessage(`[
		{"code":" INVALIDCUSTOMAUTOSCALINGVALUE ","error":" invalid CUSTOM autoscaling VALUE. ","message":" INVALID CUSTOM AUTOSCALING VALUE. "},
		{"metadata":{"startCpuCoreCount":["Value must not be less than minResource.cpuCoreCount"]}},
		{"code":"distinctError","error":"A different diagnostic"}
	]`)
	detail := errorSummary("invalidCustomAutoscalingValue", "Invalid custom autoscaling value.", meta, "")
	want := "invalidCustomAutoscalingValue; Invalid custom autoscaling value.; startCpuCoreCount: Value must not be less than minResource.cpuCoreCount; code: distinctError; error: A different diagnostic"
	if detail != want {
		t.Fatalf("primary diagnostics repeated or distinct details lost:\ngot  %s\nwant %s", detail, want)
	}
	var recorded struct {
		Error struct {
			Code, Message string
			Meta          json.RawMessage
		}
	}
	if err := json.Unmarshal(validationFixture(t, "startCpuBelowMin"), &recorded); err != nil {
		t.Fatal(err)
	}
	detail = errorSummary(recorded.Error.Code, recorded.Error.Message, recorded.Error.Meta, "")
	want = "invalidCustomAutoscalingValue; Invalid custom autoscaling value.; startCpuCoreCount: Value must not be less than minResource.cpuCoreCount"
	if detail != want {
		t.Fatalf("recorded diagnostic repeated:\ngot  %s\nwant %s", detail, want)
	}
}
