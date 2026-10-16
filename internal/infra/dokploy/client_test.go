// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestClientHeaderQueryAndRotation(t *testing.T) {
	var token string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token = r.Header.Get("x-api-key")
		if r.URL.Path != "/api/environment.one" || r.URL.Query().Get("environmentId") != "env +/&" {
			t.Error("incorrect procedure or query encoding")
		}
		io.WriteString(w, `{"environmentId":"env"}`)
	}))
	defer server.Close()
	c := newClient(server.URL, "initial-test-key")
	for _, key := range []string{"initial-test-key", "rotated-test-key"} {
		c.setToken(key)
		var out struct {
			EnvironmentID string `json:"environmentId"`
		}
		if err := c.get(context.Background(), "environment.one", map[string]string{"environmentId": "env +/&"}, &out); err != nil {
			t.Fatal(err)
		}
		if token != key || out.EnvironmentID != "env" {
			t.Fatal("header rotation or direct JSON decoding failed")
		}
	}
}

func TestClientRedirectNotFollowed(t *testing.T) {
	var visits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		visits.Add(1)
		t.Error("redirect target received a request")
	}))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }))
	defer server.Close()
	c := newClient(server.URL, "redirect-test-key")
	// A supplied client must not weaken redirect policy.
	c.http = server.Client()
	if err := c.get(context.Background(), "environment.one", nil, nil); err == nil {
		t.Fatal("redirect should fail")
	}
	if visits.Load() != 0 {
		t.Fatal("redirect leaked request")
	}
}

func TestClientGETRetryAndPOSTNoRetry(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		t.Run(method, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.WriteHeader(http.StatusServiceUnavailable)
					io.WriteString(w, `{"message":"temporarily unavailable"}`)
					return
				}
				io.WriteString(w, `true`)
			}))
			defer server.Close()
			c := newClient(server.URL, "retry-test-key")
			var err error
			if method == http.MethodGet {
				err = c.get(context.Background(), "application.one", nil, nil)
			} else {
				err = c.post(context.Background(), "application.create", map[string]string{"name": "test"}, nil)
			}
			if method == http.MethodGet && (err != nil || calls.Load() != 2) {
				t.Fatalf("GET retry: calls=%d err=%v", calls.Load(), err)
			}
			if method == http.MethodPost && (!errors.Is(err, infra.ErrUnavailable) || calls.Load() != 1) {
				t.Fatalf("POST retried: calls=%d err=%v", calls.Load(), err)
			}
		})
	}
}

func TestClientErrorMapping(t *testing.T) {
	cases := []struct {
		status int
		code   string
		want   error
	}{
		{401, "", infra.ErrUnauthorized}, {403, "", infra.ErrUnauthorized}, {400, "UNAUTHORIZED", infra.ErrUnauthorized},
		{400, "FORBIDDEN", infra.ErrUnauthorized}, {404, "", infra.ErrNotFound}, {400, "NOT_FOUND", infra.ErrNotFound},
		{409, "", infra.ErrConflict}, {400, "CONFLICT", infra.ErrConflict}, {400, "", infra.ErrRejected},
		{422, "", infra.ErrRejected}, {400, "BAD_REQUEST", infra.ErrRejected}, {429, "", infra.ErrUnavailable}, {503, "", infra.ErrUnavailable},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%d-%s", tc.status, tc.code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				json.NewEncoder(w).Encode(map[string]string{"message": "Invalid input", "code": tc.code})
			}))
			defer server.Close()
			err := newClient(server.URL, "mapping-test-key").post(context.Background(), "application.update", nil, nil)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			var api *APIError
			if !errors.As(err, &api) || infra.SafeDetail(err) == "" {
				t.Fatal("missing APIError or safe detail")
			}
			if tc.want == infra.ErrRejected {
				var rejected *infra.RejectedError
				if !errors.As(err, &rejected) {
					t.Fatal("missing RejectedError")
				}
			}
		})
	}
}

func TestClientErrorsNeverExposeCredentialsOrBody(t *testing.T) {
	const key = "confidential-test-api-key"
	const value = "confidential-request-value"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"code": key, "message": "Invalid " + key + " " + value + " https://example.invalid/signed?key=" + key + " " + strings.Repeat("x", 80), "issues": []any{map[string]string{"received": value}}})
	}))
	defer server.Close()
	err := newClient(server.URL, key).post(context.Background(), "application.update", map[string]any{"nested": []any{map[string]string{"value": value}}}, nil)
	for _, text := range []string{err.Error(), infra.SafeDetail(err)} {
		for _, secret := range []string{key, value, "https://", strings.Repeat("x", 80)} {
			if strings.Contains(text, secret) {
				t.Fatal("unsafe diagnostic")
			}
		}
		if len(text) > diagnosticLimit+16 {
			t.Fatal("unbounded diagnostic")
		}
	}
}

func TestClientSensitivePayloadErrors(t *testing.T) {
	const key = "sensitive-payload-test-key"
	const secret = "private-env-fixture"
	cases := []struct {
		status int
		code   string
		want   error
	}{
		{400, "BAD_REQUEST", infra.ErrRejected},
		{401, "UNAUTHORIZED", infra.ErrUnauthorized},
		{404, "NOT_FOUND", infra.ErrNotFound},
		{409, "CONFLICT", infra.ErrConflict},
		{500, "INTERNAL_SERVER_ERROR", infra.ErrUnavailable},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				json.NewEncoder(w).Encode(map[string]string{"code": tc.code, "message": "rejected " + key + " " + secret})
			}))
			defer server.Close()
			err := newClient(server.URL, key).post(context.Background(), "application.saveEnvironment", map[string]any{
				"applicationId": "app",
				"env":           "SECRET='" + secret + "'\nMULTILINE='line one\nline two'\n",
				"createEnvFile": false,
			}, nil)
			if !errors.Is(err, tc.want) {
				t.Fatalf("incorrect error class: %v", err)
			}
			for _, text := range []string{err.Error(), infra.SafeDetail(err)} {
				if strings.Contains(text, secret) || strings.Contains(text, key) {
					t.Fatal("sensitive payload leaked through a platform diagnostic")
				}
			}
		})
	}
}

type clientRoundTripFunc func(*http.Request) (*http.Response, error)

func (f clientRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestClientNetworkErrorsAndContext(t *testing.T) {
	c := newClient("https://example.invalid", "network-test-key")
	var calls atomic.Int32
	c.http.Transport = clientRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("https://example.invalid/network-test-key")
	})
	err := c.get(context.Background(), "environment.one", nil, nil)
	if !errors.Is(err, infra.ErrUnavailable) || strings.Contains(err.Error(), "example.invalid") || strings.Contains(err.Error(), "network-test-key") || calls.Load() != 3 {
		t.Fatal("unsafe or unbounded network retry")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.get(ctx, "environment.one", nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := c.get(ctx, "environment.one", nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
}

func TestClientResponseBounds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.CopyN(w, strings.NewReader(strings.Repeat("x", int(jsonResponseLimit)+1)), jsonResponseLimit+1)
	}))
	defer server.Close()
	if err := newClient(server.URL, "bounds-test-key").get(context.Background(), "application.one", nil, nil); !errors.Is(err, infra.ErrUnavailable) {
		t.Fatalf("got %v", err)
	}
}

func TestClientUploadMultipartStream(t *testing.T) {
	path := filepath.Join(t.TempDir(), "deployment.zip")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	const size = 12 << 20
	if err := file.Truncate(size); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/application.dropDeployment" || r.Header.Get("x-api-key") != "upload-test-key" {
			t.Error("invalid upload request")
		}
		if r.ContentLength <= size || len(r.TransferEncoding) != 0 {
			t.Error("upload must have exact Content-Length")
		}
		reader, err := r.MultipartReader()
		if err != nil {
			t.Error(err)
			return
		}
		part, err := reader.NextPart()
		if err != nil {
			t.Error(err)
			return
		}
		value, _ := io.ReadAll(part)
		if part.FormName() != "applicationId" || string(value) != "application-test" {
			t.Error("invalid applicationId field")
		}
		part, err = reader.NextPart()
		if err != nil {
			t.Error(err)
			return
		}
		if part.FormName() != "zip" || part.FileName() != "karakoram.zip" {
			t.Error("invalid zip field")
		}
		n, err := io.Copy(io.Discard, part)
		if err != nil || n != size {
			t.Errorf("archive truncated: %d, %v", n, err)
		}
		if _, err := reader.NextPart(); err != io.EOF {
			t.Error("unexpected multipart tail")
		}
		io.WriteString(w, "true")
	}))
	defer server.Close()
	if err := newClient(server.URL, "upload-test-key").upload(context.Background(), "application-test", path); err != nil {
		t.Fatal(err)
	}
	// Inspect the body before transmission: multipart framing is composed with
	// the file reader, never converted into a zip-sized byte/string buffer.
	c := newClient(server.URL, "upload-test-key")
	c.http.Transport = clientRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if _, ok := r.Body.(interface{ Bytes() []byte }); ok {
			t.Error("archive buffered in memory")
		}
		if r.ContentLength <= size || r.GetBody != nil {
			t.Error("upload body is not streamed")
		}
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil || n != r.ContentLength {
			t.Error("incorrect multipart content length")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("true")), Header: make(http.Header)}, nil
	})
	if err := c.upload(context.Background(), "application-test", path); err != nil {
		t.Fatal(err)
	}
}
