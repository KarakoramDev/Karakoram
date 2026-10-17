// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/version"
)

const artifactSizeLimit int64 = 512 << 20

var releaseVersion = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$`)

type artifactCacheKey struct {
	source string
	binary [sha256.Size]byte
}

type artifactResolver struct {
	getenv  func(string) string
	version func() string
	http    *http.Client
	limit   int64
	mu      sync.Mutex
	cache   map[artifactCacheKey]string
}

// newArtifactResolver permits operator configuration and HTTP injection without
// changing process-global environment or build metadata.
func newArtifactResolver(getenv func(string) string, getVersion func() string, h *http.Client) *artifactResolver {
	if getenv == nil {
		getenv = os.Getenv
	}
	if getVersion == nil {
		getVersion = func() string { return version.Version }
	}
	if h == nil {
		h = &http.Client{Timeout: 10 * time.Minute}
	}
	return &artifactResolver{getenv: getenv, version: getVersion, http: h, limit: artifactSizeLimit, cache: make(map[artifactCacheKey]string)}
}

func isReleaseVersion(v string) bool {
	match := releaseVersion.FindStringSubmatch(v)
	if match == nil {
		return false
	}
	for _, identifier := range strings.Split(match[4], ".") {
		if len(identifier) > 1 && identifier[0] == '0' && strings.Trim(identifier, "0123456789") == "" {
			return false
		}
	}
	return true
}

func artifactRejected(message string) error { return rejectedConfig("nomadArtifact", message) }

func (r *artifactResolver) resolve(ctx context.Context, art infra.Artifact, source string) (string, string, error) {
	if source == "" {
		source = r.getenv("KARAKORAM_NOMAD_ARTIFACT_URL")
	}
	if source == "" {
		v := r.version()
		if !isReleaseVersion(v) {
			return "", "", artifactRejected("set KARAKORAM_NOMAD_ARTIFACT_URL for a non-release build")
		}
		source = "https://github.com/KarakoramDev/Karakoram/releases/download/" + v + "/karakoram-" + v + "-linux-amd64.tar.gz"
	}
	u, err := url.Parse(source)
	if err != nil || u.Host == "" || u.Opaque != "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return "", "", artifactRejected("artifact URL must use HTTP(S) without userinfo")
	}
	digest, err := archiveBinaryDigest(art.Reader, art.Binary, r.limit, nil)
	if err != nil {
		return "", "", err
	}
	key := artifactCacheKey{source: source, binary: digest}
	r.mu.Lock()
	defer r.mu.Unlock()
	if checksum, ok := r.cache[key]; ok {
		return source, checksum, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, source, nil)
	if err != nil {
		return "", "", artifactRejected("cannot construct artifact request")
	}
	h := *r.http
	if h.Timeout == 0 {
		h.Timeout = 10 * time.Minute
	}
	h.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 || req.URL.Scheme != "https" || req.URL.User != nil {
			return http.ErrUseLastResponse
		}
		return nil
	}
	resp, err := h.Do(req)
	if err != nil {
		return "", "", silentFailure("artifact download failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", "", &APIError{Status: resp.StatusCode, Op: "artifact download", Message: "artifact server rejected request"}
	}
	archiveHash := sha256.New()
	downloaded, err := archiveBinaryDigest(resp.Body, art.Binary, r.limit, archiveHash)
	if err != nil {
		return "", "", err
	}
	if downloaded != digest {
		return "", "", artifactRejected("artifact URL does not contain this control plane's binary")
	}
	checksum := "sha256:" + hex.EncodeToString(archiveHash.Sum(nil))
	r.cache[key] = checksum
	return source, checksum, nil
}

// Bound both compressed input and expanded tar data. The complete compressed
// stream is consumed so the checksum describes exactly what Nomad downloads.
func archiveBinaryDigest(input io.Reader, binary string, limit int64, archiveHash hash.Hash) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	if input == nil || binary == "" || binary == "." || binary == ".." || strings.ContainsAny(binary, "/\\") {
		return digest, artifactRejected("artifact requires a regular binary file at archive root")
	}
	compressed := &io.LimitedReader{R: input, N: limit + 1}
	var source io.Reader = compressed
	if archiveHash != nil {
		source = io.TeeReader(source, archiveHash)
	}
	gz, err := gzip.NewReader(source)
	if err != nil {
		return digest, artifactRejected("artifact is not a valid tar.gz archive")
	}
	defer gz.Close()
	expanded := &io.LimitedReader{R: gz, N: limit + 1}
	tr := tar.NewReader(expanded)
	found := false
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return digest, artifactRejected("artifact archive is invalid or exceeds size limit")
		}
		if header.Name != binary {
			continue
		}
		if found || header.Typeflag != tar.TypeReg {
			return digest, artifactRejected("artifact requires one regular binary file at archive root")
		}
		found = true
		binaryHash := sha256.New()
		if _, err := io.Copy(binaryHash, tr); err != nil {
			return digest, artifactRejected("cannot read artifact binary")
		}
		copy(digest[:], binaryHash.Sum(nil))
	}
	if _, err := io.Copy(io.Discard, expanded); err != nil {
		return digest, artifactRejected("artifact archive is invalid or exceeds size limit")
	}
	if expanded.N == 0 {
		return digest, artifactRejected("artifact archive exceeds size limit")
	}
	if _, err := io.Copy(io.Discard, source); err != nil {
		return digest, artifactRejected("cannot read artifact archive")
	}
	if compressed.N == 0 {
		return digest, artifactRejected("artifact archive exceeds size limit")
	}
	if !found {
		return digest, artifactRejected("artifact requires a regular binary file at archive root")
	}
	return digest, nil
}
