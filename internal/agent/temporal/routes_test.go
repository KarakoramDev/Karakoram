// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package temporal

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// TestDeleteNamespaceStatusMapping checks typed gRPC trailer status translation.
func TestDeleteNamespaceStatusMapping(t *testing.T) {
	for _, test := range []struct{ code, want int }{
		{0, 200}, {3, 400}, {5, 404}, {6, 409}, {9, 409},
		{1, 503}, {2, 503}, {4, 503}, {7, 503}, {8, 503}, {10, 503},
		{11, 503}, {12, 503}, {13, 503}, {14, 503}, {15, 503}, {16, 503},
	} {
		t.Run(strconv.Itoa(test.code), func(t *testing.T) {
			response := &http.Response{StatusCode: 200, Header: make(http.Header), Trailer: make(http.Header)}
			response.Trailer.Set("Grpc-Status", strconv.Itoa(test.code))
			response.Trailer.Set("Grpc-Message", "postgres://owner:private-password@host/db")
			err := deletionResponse(response)
			if test.code == 0 {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var status grpcStatusError
			if !errors.As(err, &status) || status.code != test.code {
				t.Fatalf("missing typed status: %v", err)
			}
			recorder := httptest.NewRecorder()
			writeNamespaceError(recorder, fmt.Errorf("delete namespace: %w", err))
			if recorder.Code != test.want {
				t.Errorf("HTTP status=%d want %d", recorder.Code, test.want)
			}
			if strings.Contains(recorder.Body.String(), "private-password") || strings.Contains(recorder.Body.String(), "postgres://") {
				t.Fatal("upstream message leaked")
			}
		})
	}
}

// TestDeleteNamespaceMalformedStatus rejects missing and invalid gRPC statuses.
func TestDeleteNamespaceMalformedStatus(t *testing.T) {
	for _, status := range []string{"", "-1", "17", "invalid"} {
		response := &http.Response{StatusCode: 200, Header: make(http.Header), Trailer: make(http.Header)}
		response.Trailer.Set("Grpc-Status", status)
		if err := deletionResponse(response); err == nil || namespaceHTTPStatus(err) != 503 {
			t.Errorf("status %q: %v", status, err)
		}
	}
	response := &http.Response{StatusCode: 200, Header: make(http.Header), Trailer: make(http.Header)}
	response.Header.Set("Grpc-Status", "5")
	if err := deletionResponse(response); err == nil || namespaceHTTPStatus(err) != 404 {
		t.Fatal("trailers-only response header status was lost")
	}
	response.StatusCode = 502
	response.Header.Set("Grpc-Status", "0")
	if err := deletionResponse(response); err == nil || namespaceHTTPStatus(err) != 503 {
		t.Fatal("non-OK transport response was accepted")
	}
}

// TestFrontendHTTPStatusMapping preserves actionable HTTP API failures.
func TestFrontendHTTPStatusMapping(t *testing.T) {
	for _, test := range []struct{ code, want int }{{400, 400}, {422, 422}, {404, 404}, {409, 409}, {401, 503}, {403, 503}, {429, 503}, {500, 503}, {503, 503}, {504, 503}} {
		t.Run(strconv.Itoa(test.code), func(t *testing.T) {
			response := &http.Response{StatusCode: test.code, Body: io.NopCloser(strings.NewReader(`{"message":"postgres://owner:private-password@host/db"}`))}
			_, err := frontendResponse(response, http.MethodPost, "/api/v1/namespaces/orders/update")
			var status frontendError
			if !errors.As(err, &status) || status.code != test.code {
				t.Fatalf("missing typed status: %v", err)
			}
			recorder := httptest.NewRecorder()
			writeNamespaceError(recorder, fmt.Errorf("namespace API: %w", err))
			if recorder.Code != test.want {
				t.Errorf("HTTP status=%d want %d", recorder.Code, test.want)
			}
			if strings.Contains(recorder.Body.String(), "private-password") || strings.Contains(recorder.Body.String(), "postgres://") {
				t.Fatal("upstream body leaked")
			}
		})
	}
	response := &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"namespaces":[]}`))}
	body, err := frontendResponse(response, http.MethodGet, "/api/v1/namespaces")
	if err != nil || string(body) != `{"namespaces":[]}` {
		t.Fatal("successful response changed", err)
	}
	if namespaceHTTPStatus(errors.New("transport unavailable")) != 503 {
		t.Fatal("untyped error did not map to unavailable")
	}
}

// TestFrontendGatewayCodeMapping distinguishes gateway conflicts from invalid input.
func TestFrontendGatewayCodeMapping(t *testing.T) {
	for _, test := range []struct{ code, want int }{{3, 400}, {5, 404}, {6, 409}, {9, 409}, {4, 503}, {14, 503}} {
		response := &http.Response{
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(strings.NewReader(fmt.Sprintf(`{"code":%d,"message":"private-password"}`, test.code))),
		}
		_, err := frontendResponse(response, http.MethodPost, "/api/v1/namespaces/orders/update")
		if err == nil || namespaceHTTPStatus(err) != test.want {
			t.Errorf("gateway gRPC code %d: %v, want HTTP %d", test.code, err, test.want)
		}
		if err != nil && strings.Contains(err.Error(), "private-password") {
			t.Fatal("gateway diagnostic leaked")
		}
	}
}
