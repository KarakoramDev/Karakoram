// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func testArchive(t *testing.T, name, data string, kind byte, copies int) []byte {
	t.Helper()
	var archive bytes.Buffer
	gz := gzip.NewWriter(&archive)
	tw := tar.NewWriter(gz)
	for range copies {
		size := int64(len(data))
		if kind != tar.TypeReg {
			size = 0
		}
		header := &tar.Header{Name: name, Mode: 0755, Size: size, Typeflag: kind}
		if kind == tar.TypeSymlink || kind == tar.TypeLink {
			header.Linkname = "other"
		}
		if err := tw.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if size > 0 {
			if _, err := io.WriteString(tw, data); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return archive.Bytes()
}

func testArtifact(data []byte) infra.Artifact {
	return infra.Artifact{Reader: bytes.NewReader(data), Name: "test.tar.gz", Binary: "karakoram"}
}

func TestArtifactResolverIntegrityAndCache(t *testing.T) {
	local := testArchive(t, "karakoram", "the binary", tar.TypeReg, 1)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); _, _ = w.Write(local) }))
	defer server.Close()
	resolver := newArtifactResolver(testEnv(map[string]string{"KARAKORAM_NOMAD_ARTIFACT_URL": server.URL}), func() string { return "dev" }, server.Client())
	digest := sha256.Sum256(local)
	expected := "sha256:" + hex.EncodeToString(digest[:])
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			source, checksum, err := resolver.resolve(context.Background(), testArtifact(local), "")
			if err != nil || source != server.URL || checksum != expected {
				t.Errorf("resolve = %q, %q, %v", source, checksum, err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("downloads = %d; want 1", calls.Load())
	}
}

func TestArtifactResolverRejects(t *testing.T) {
	good := testArchive(t, "karakoram", "binary", tar.TypeReg, 1)
	missing := testArchive(t, "other", "binary", tar.TypeReg, 1)
	mismatch := testArchive(t, "karakoram", "different", tar.TypeReg, 1)
	nested := testArchive(t, "dir/karakoram", "binary", tar.TypeReg, 1)
	symlink := testArchive(t, "karakoram", "", tar.TypeSymlink, 1)
	hardlink := testArchive(t, "karakoram", "", tar.TypeLink, 1)
	duplicate := testArchive(t, "karakoram", "binary", tar.TypeReg, 2)
	for _, tt := range []struct {
		name          string
		local, remote []byte
		limit         int64
	}{
		{name: "mismatch", local: good, remote: mismatch},
		{name: "missing local", local: missing, remote: good},
		{name: "missing remote", local: good, remote: missing},
		{name: "not root", local: nested, remote: good},
		{name: "symlink", local: symlink, remote: good},
		{name: "hardlink", local: hardlink, remote: good},
		{name: "duplicate", local: duplicate, remote: good},
		{name: "invalid local", local: []byte("not gzip"), remote: good},
		{name: "invalid remote", local: good, remote: []byte("not gzip")},
		{name: "oversized input", local: good, remote: good, limit: 32},
		{name: "oversized expansion", local: good, remote: good, limit: 1024},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(tt.remote) }))
			defer server.Close()
			r := newArtifactResolver(nil, func() string { return "dev" }, server.Client())
			if tt.limit != 0 {
				r.limit = tt.limit
			}
			_, _, err := r.resolve(context.Background(), testArtifact(tt.local), server.URL)
			if !errors.Is(err, infra.ErrRejected) {
				t.Fatalf("want rejection, got %v", err)
			}
			if tt.name == "mismatch" && !strings.Contains(err.Error(), "artifact URL does not contain this control plane's binary") {
				t.Fatalf("wrong mismatch error: %v", err)
			}
		})
	}
}

func TestArtifactFailuresNotCached(t *testing.T) {
	good := testArchive(t, "karakoram", "binary", tar.TypeReg, 1)
	bad := testArchive(t, "karakoram", "different", tar.TypeReg, 1)
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			_, _ = w.Write(bad)
		} else {
			_, _ = w.Write(good)
		}
	}))
	defer server.Close()
	resolver := newArtifactResolver(nil, nil, server.Client())
	if _, _, err := resolver.resolve(context.Background(), testArtifact(good), server.URL); !errors.Is(err, infra.ErrRejected) {
		t.Fatalf("want rejection, got %v", err)
	}
	if _, _, err := resolver.resolve(context.Background(), testArtifact(good), server.URL); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("downloads = %d", calls)
	}
}

type testRoundTripper func(*http.Request) (*http.Response, error)

func (f testRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestArtifactSourceSelection(t *testing.T) {
	archive := testArchive(t, "karakoram", "binary", tar.TypeReg, 1)
	for _, tt := range []struct {
		version, source, env, want string
		reject                     bool
	}{
		{version: "v1.2.3", want: "https://github.com/KarakoramDev/Karakoram/releases/download/v1.2.3/karakoram-v1.2.3-linux-amd64.tar.gz"},
		{version: "v0.2.0-rc.1", want: "https://github.com/KarakoramDev/Karakoram/releases/download/v0.2.0-rc.1/karakoram-v0.2.0-rc.1-linux-amd64.tar.gz"},
		{version: "dev", reject: true}, {version: "v01.2.3", reject: true}, {version: "v1.2.3-01", reject: true}, {version: "1.2.3", reject: true},
		{version: "dev", source: "http://mirror.example.com/a.tar.gz", want: "http://mirror.example.com/a.tar.gz"},
		{version: "dev", env: "https://mirror.example.com/a.tar.gz", want: "https://mirror.example.com/a.tar.gz"},
		{version: "dev", source: "https://user:secret@example.com/a", reject: true},
		{version: "dev", source: "file:///artifact", reject: true},
	} {
		t.Run(tt.version+tt.source+tt.env, func(t *testing.T) {
			h := &http.Client{Transport: testRoundTripper(func(req *http.Request) (*http.Response, error) {
				if req.URL.String() != tt.want {
					t.Errorf("requested %q; want %q", req.URL, tt.want)
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(archive)), Header: make(http.Header)}, nil
			})}
			resolver := newArtifactResolver(testEnv(map[string]string{"KARAKORAM_NOMAD_ARTIFACT_URL": tt.env}), func() string { return tt.version }, h)
			source, _, err := resolver.resolve(context.Background(), testArtifact(archive), tt.source)
			if tt.reject {
				if !errors.Is(err, infra.ErrRejected) {
					t.Fatalf("want rejected, got %v", err)
				}
				if strings.Contains(err.Error(), "secret") {
					t.Fatal("leaked userinfo")
				}
				if tt.source == "" && !strings.Contains(err.Error(), "KARAKORAM_NOMAD_ARTIFACT_URL") {
					t.Fatalf("missing operator guidance: %v", err)
				}
				return
			}
			if err != nil || source != tt.want {
				t.Fatalf("source = %q, error=%v", source, err)
			}
		})
	}
}

func TestArtifactRedirectPolicy(t *testing.T) {
	archive := testArchive(t, "karakoram", "binary", tar.TypeReg, 1)
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(archive) }))
	defer target.Close()
	for _, scheme := range []string{"http", "https"} {
		t.Run(scheme, func(t *testing.T) {
			location := target.URL
			if scheme == "http" {
				location = strings.Replace(location, "https:", "http:", 1)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, location, http.StatusFound) }))
			defer server.Close()
			resolver := newArtifactResolver(nil, nil, target.Client())
			_, _, err := resolver.resolve(context.Background(), testArtifact(archive), server.URL)
			if scheme == "https" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("HTTP redirect accepted")
			}
		})
	}
}

func TestArchiveCompressedSizeCap(t *testing.T) {
	archive := testArchive(t, "karakoram", "binary", tar.TypeReg, 1)
	// Trailing bytes are part of the download and must count toward the input cap.
	archive = append(archive, bytes.Repeat([]byte{0}, 4096)...)
	_, err := archiveBinaryDigest(bytes.NewReader(archive), "karakoram", 2048, nil)
	if !errors.Is(err, infra.ErrRejected) {
		t.Fatalf("want bounded input rejection, got %v", err)
	}
	if artifactSizeLimit != 512<<20 {
		t.Fatalf("wrong production limit: %d", artifactSizeLimit)
	}
}
