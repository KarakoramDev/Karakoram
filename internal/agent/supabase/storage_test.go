// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
)

// Verify the actual SigV4 MAC, not just the presence of an Authorization header.
func validBucketSignature(r *http.Request, secret string) bool {
	auth := r.Header.Get("Authorization")
	parts := strings.Split(strings.TrimPrefix(auth, "AWS4-HMAC-SHA256 "), ", ")
	if len(parts) != 3 {
		return false
	}
	credential := strings.Split(strings.TrimPrefix(parts[0], "Credential="), "/")
	if len(credential) != 5 || credential[0] != "test-access" {
		return false
	}
	signed := strings.TrimPrefix(parts[1], "SignedHeaders=")
	var headers strings.Builder
	for _, name := range strings.Split(signed, ";") {
		value := r.Header.Get(name)
		if name == "host" {
			value = r.Host
		}
		fmt.Fprintf(&headers, "%s:%s\n", name, strings.TrimSpace(value))
	}
	query := r.URL.Query()
	for key := range query {
		sort.Strings(query[key])
	}
	canonical := r.Method + "\n" + r.URL.EscapedPath() + "\n" + query.Encode() + "\n" + headers.String() + "\n" + signed + "\n" + r.Header.Get("X-Amz-Content-Sha256")
	sum := sha256.Sum256([]byte(canonical))
	scope := strings.Join(credential[1:], "/")
	toSign := "AWS4-HMAC-SHA256\n" + r.Header.Get("X-Amz-Date") + "\n" + scope + "\n" + hex.EncodeToString(sum[:])
	mac := func(key []byte, data string) []byte {
		h := hmac.New(sha256.New, key)
		h.Write([]byte(data))
		return h.Sum(nil)
	}
	key := mac([]byte("AWS4"+secret), credential[1])
	key = mac(key, credential[2])
	key = mac(key, "s3")
	key = mac(key, "aws4_request")
	return hmac.Equal([]byte(strings.TrimPrefix(parts[2], "Signature=")), []byte(hex.EncodeToString(mac(key, toSign))))
}

func TestStorageProbeAuthenticatedCachedAndRecovers(t *testing.T) {
	var calls atomic.Int32
	var denied atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "HEAD" || r.URL.Path != "/objects/" && r.URL.Path != "/objects" || r.URL.RawQuery != "" {
			t.Error("probe must only HEAD the configured bucket")
			w.WriteHeader(400)
			return
		}
		if denied.Load() || !validBucketSignature(r, "correct-test-value") {
			w.WriteHeader(403)
			return
		}
		w.WriteHeader(200)
	}))
	defer server.Close()
	s := sbspec.Storage{Endpoint: server.URL, Bucket: "objects", Region: "us-east-1", PathStyle: true, AccessKey: "test-access", SecretKey: "incorrect-test-value"}
	p := &storageProbe{}
	first, message := p.check(t.Context(), s)
	if first.IsZero() || !strings.Contains(message, "access denied") {
		t.Fatal("bad credentials not diagnosed:", message)
	}
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			_, message := p.check(t.Context(), s)
			if !strings.Contains(message, "access denied") {
				t.Error("cached failure lost")
			}
		})
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatal("status callers hammered S3", calls.Load())
	}
	s.SecretKey = "correct-test-value"
	at, message := p.check(t.Context(), s)
	if message != "" || at.Before(first) || calls.Load() != 2 {
		t.Fatal("corrected registration did not invalidate failed sample:", message, calls.Load())
	}
	_, message = p.check(t.Context(), s)
	if message != "" || calls.Load() != 2 {
		t.Fatal("successful sample not cached")
	}
	denied.Store(true)
	p.mu.Lock()
	p.checkedAt = time.Now().Add(-engine.ReadinessInterval)
	p.mu.Unlock()
	_, message = p.check(t.Context(), s)
	if !strings.Contains(message, "access denied") || calls.Load() != 3 {
		t.Fatal("expired successful sample hid dependency failure:", message)
	}
	denied.Store(false)
	p.mu.Lock()
	p.checkedAt = time.Now().Add(-engine.ReadinessInterval)
	p.mu.Unlock()
	_, message = p.check(t.Context(), s)
	if message != "" || calls.Load() != 4 {
		t.Fatal("dependency recovery not observed:", message)
	}
}

func TestStorageProbeBoundedAndSanitized(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	s := sbspec.Storage{Endpoint: server.URL, Bucket: "objects", Region: "us-east-1", PathStyle: true, AccessKey: "test-access", SecretKey: "must-not-appear"}
	p := &storageProbe{}
	started := time.Now()
	_, message := p.check(t.Context(), s)
	if time.Since(started) > engine.ObservationTimeout+time.Second || !strings.Contains(message, "timed out") {
		t.Fatal("probe exceeded observation budget:", message)
	}
	if strings.Contains(message, s.SecretKey) || strings.Contains(message, server.URL) {
		t.Fatal("probe leaked configuration")
	}
	// A blocked concurrent caller must also respect its own deadline.
	p.mu.Lock()
	p.running = make(chan struct{})
	done := p.running
	p.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, message = p.check(ctx, s)
	close(done)
	if !strings.Contains(message, "timed out") {
		t.Fatal("in-flight waiter ignored cancellation")
	}
}

func TestStorageProbeMissingBucketAndConfiguration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) }))
	defer server.Close()
	s := sbspec.Storage{Endpoint: server.URL, Bucket: "missing", Region: "us-east-1", PathStyle: true, AccessKey: "test-access", SecretKey: "test-value"}
	p := &storageProbe{}
	_, message := p.check(t.Context(), s)
	if !strings.Contains(message, "does not exist") {
		t.Fatal("missing bucket diagnosis lost:", message)
	}
	s.SecretKey = ""
	_, message = p.check(t.Context(), s)
	if !strings.Contains(message, "not configured") {
		t.Fatal("anonymous HEAD must never satisfy readiness:", message)
	}
}

func TestStorageProbeAcceptsIPAndEndpointPrefix(t *testing.T) {
	for _, tc := range []struct {
		name, prefix string
		pathStyle    bool
	}{
		{"ipv4-auto-path", "", false},
		{"prefix-path", "/s3", true},
		{"prefix-auto-path", "/s3/", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "HEAD" || strings.TrimSuffix(r.URL.Path, "/") != "/s3/objects" && !(tc.prefix == "" && strings.TrimSuffix(r.URL.Path, "/") == "/objects") {
					t.Error("bucket request lost endpoint prefix")
					w.WriteHeader(400)
					return
				}
				if !validBucketSignature(r, "correct-test-value") {
					t.Error("bucket HEAD signature does not cover endpoint path")
					w.WriteHeader(403)
					return
				}
				w.WriteHeader(200)
			}))
			defer server.Close()
			s := sbspec.Storage{Endpoint: server.URL + tc.prefix, Bucket: "objects", Region: "us-east-1", PathStyle: tc.pathStyle, AccessKey: "test-access", SecretKey: "correct-test-value"}
			_, message := (&storageProbe{}).check(t.Context(), s)
			if message != "" {
				t.Fatal("working endpoint rejected:", message)
			}
		})
	}
}

func TestStorageProbeEndpointAddressing(t *testing.T) {
	for _, tc := range []struct {
		name, endpoint, bucket, host, path string
		forcePath                          bool
	}{
		{"dns-virtual", "https://objects.example.test", "objects", "objects.objects.example.test", "/", false},
		{"dns-forced-path", "https://objects.example.test", "objects", "objects.example.test", "/objects", true},
		{"dotted-https", "https://objects.example.test", "application.objects", "objects.example.test", "/application.objects", false},
		{"dotted-http", "http://objects.example.test", "application.objects", "application.objects.objects.example.test", "/", false},
		{"ipv4", "http://127.0.0.1:9000", "objects", "127.0.0.1:9000", "/objects", false},
		{"ipv6", "http://[::1]:9000", "objects", "[::1]:9000", "/objects", false},
		{"prefix-forced", "https://objects.example.test/s3", "objects", "objects.example.test", "/s3/objects", true},
		{"prefix-virtual", "https://objects.example.test/s3/", "objects", "objects.objects.example.test", "/s3/", false},
		{"prefix-dotted", "https://objects.example.test/s3/", "application.objects", "objects.example.test", "/s3/application.objects", false},
		{"prefix-escaped-space", "https://objects.example.test/object%20store", "objects", "objects.example.test", "/object%20store/objects", true},
		{"legacy-bucket", "https://objects.example.test", "Legacy_Bucket", "objects.example.test", "/Legacy_Bucket", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := sbspec.Storage{Endpoint: tc.endpoint, Bucket: tc.bucket, Region: "us-east-1", PathStyle: tc.forcePath, AccessKey: "test-access", SecretKey: "correct-test-value"}
			client := newStorageClient(config)
			if client == nil {
				t.Fatal("valid endpoint rejected")
			}
			if client.url.Host != tc.host || client.url.EscapedPath() != tc.path {
				t.Fatalf("host/path=%s %s, want %s %s", client.url.Host, client.url.EscapedPath(), tc.host, tc.path)
			}
			old := storageHTTPClient.Transport
			storageHTTPClient.Transport = roundTripper(func(req *http.Request) (*http.Response, error) {
				if req.Host != tc.host || req.URL.EscapedPath() != tc.path || !validBucketSignature(req, config.SecretKey) {
					t.Error("HEAD addressing or signed path differs from component endpoint")
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("")), Header: http.Header{}}, nil
			})
			defer func() { storageHTTPClient.Transport = old }()
			if message := checkStorageBucket(t.Context(), client); message != "" {
				t.Fatal(message)
			}
		})
	}
}

func TestStorageProbeDoesNotFollowRedirects(t *testing.T) {
	var forwarded atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwarded.Add(1)
		w.WriteHeader(200)
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	config := sbspec.Storage{Endpoint: source.URL, Bucket: "objects", PathStyle: true, AccessKey: "test-access", SecretKey: "test-value"}
	_, message := (&storageProbe{}).check(t.Context(), config)
	if message == "" || forwarded.Load() != 0 {
		t.Fatal("redirect forwarded authenticated dependency request")
	}
}
