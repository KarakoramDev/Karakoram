// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package ui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
)

func testFS() fstest.MapFS {
	return fstest.MapFS{
		"index.html":           {Data: []byte("<!doctype html><title>k</title>")},
		"assets/app.abc123.js": {Data: []byte("console.log(1)")},
		"favicon.svg":          {Data: []byte("<svg/>")},
	}
}

func get(t *testing.T, h http.Handler, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, target, nil))
	return rec
}

func TestSPAHandler(t *testing.T) {
	files := testFS()
	h := &spaHandler{files: files, fileServer: http.FileServer(http.FS(files))}

	t.Run("root serves index with no-cache", func(t *testing.T) {
		rec := get(t, h, http.MethodGet, "/")
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "<title>k</title>") {
			t.Fatalf("unexpected response %d %q", rec.Code, rec.Body.String())
		}
		if cc := rec.Header().Get("Cache-Control"); cc != "no-cache" {
			t.Fatalf("Cache-Control = %q", cc)
		}
	})

	t.Run("hashed asset is immutable", func(t *testing.T) {
		rec := get(t, h, http.MethodGet, "/assets/app.abc123.js")
		if rec.Code != http.StatusOK || rec.Body.String() != "console.log(1)" {
			t.Fatalf("unexpected response %d %q", rec.Code, rec.Body.String())
		}
		if cc := rec.Header().Get("Cache-Control"); !strings.Contains(cc, "immutable") {
			t.Fatalf("Cache-Control = %q", cc)
		}
	})

	t.Run("non-hashed static file is revalidated", func(t *testing.T) {
		rec := get(t, h, http.MethodGet, "/favicon.svg")
		if rec.Code != http.StatusOK {
			t.Fatalf("code %d", rec.Code)
		}
		if cc := rec.Header().Get("Cache-Control"); cc != "no-cache" {
			t.Fatalf("Cache-Control = %q", cc)
		}
	})

	t.Run("client route falls back to index", func(t *testing.T) {
		rec := get(t, h, http.MethodGet, "/clusters/prod/backups")
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "<title>k</title>") {
			t.Fatalf("unexpected response %d %q", rec.Code, rec.Body.String())
		}
	})

	t.Run("missing file with extension is 404", func(t *testing.T) {
		rec := get(t, h, http.MethodGet, "/assets/missing.js")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("code %d", rec.Code)
		}
	})
	t.Run("path traversal is confined to bundle", func(t *testing.T) {
		rec := get(t, h, http.MethodGet, "/../../etc/passwd")
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "<title>k</title>") {
			t.Fatalf("traversal escaped the bundle: %d %q", rec.Code, rec.Body.String())
		}
	})

	t.Run("head omits body", func(t *testing.T) {
		rec := get(t, h, http.MethodHead, "/")
		if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
			t.Fatalf("unexpected response %d len=%d", rec.Code, rec.Body.Len())
		}
	})

	t.Run("mutating methods rejected", func(t *testing.T) {
		rec := get(t, h, http.MethodPost, "/")
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("code %d", rec.Code)
		}
	})
}

func TestNotBuiltFallback(t *testing.T) {
	rec := get(t, http.HandlerFunc(serveNotBuilt), http.MethodGet, "/clusters")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("code %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "not included in this build") {
		t.Fatalf("body %q", rec.Body.String())
	}
}

func TestHandlerMatchesBuildTag(t *testing.T) {
	if (Assets() != nil) != Built {
		t.Fatalf("Assets()!=nil is %v but Built is %v", Assets() != nil, Built)
	}
}
