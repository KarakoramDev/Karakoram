// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
	"github.com/KarakoramDev/Karakoram/internal/s3endpoint"
	"github.com/minio/minio-go/v7/pkg/signer"
)

// storageProbe shares a single non-mutating bucket HEAD across status/metrics
// callers. Both outcomes expire, and configuration changes invalidate the sample.
// Never expose SDK errors: they may contain endpoint URLs or credential material.
type storageProbe struct {
	mu        sync.Mutex
	config    sbspec.Storage
	client    *storageBucketClient
	checkedAt time.Time
	message   string
	running   chan struct{}
}

func (p *storageProbe) check(ctx context.Context, config sbspec.Storage) (time.Time, string) {
	for {
		p.mu.Lock()
		if p.running != nil {
			done := p.running
			p.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return time.Time{}, "Storage S3 dependency: observation timed out"
			}
		}
		if p.config == config && !p.checkedAt.IsZero() && time.Since(p.checkedAt) >= 0 && time.Since(p.checkedAt) < engine.ReadinessInterval {
			at, message := p.checkedAt, p.message
			p.mu.Unlock()
			return at, message
		}
		if p.config != config || p.client == nil {
			p.config = config
			p.client = newStorageClient(config)
		}
		client := p.client
		p.running = make(chan struct{})
		p.mu.Unlock()

		at := time.Now()
		probe, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
		message := checkStorageBucket(probe, client)
		cancel()
		p.mu.Lock()
		p.checkedAt, p.message = at, message
		close(p.running)
		p.running = nil
		p.mu.Unlock()
		return at, message
	}
}

// storageBucketClient uses the same SigV4 implementation as storage registration,
// but resolves the endpoint like Storage's AWS SDK: force-path when requested,
// for IP endpoints, or for buckets that cannot be virtual-hosted (including
// dotted bucket names over HTTPS). Custom DNS endpoints otherwise use DNS style.
// Unlike the object SDK's origin-only constructor, this retains endpoint prefixes.
type storageBucketClient struct {
	url    *url.URL
	config sbspec.Storage
}

var storageHTTPClient = &http.Client{
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

func newStorageClient(s sbspec.Storage) *storageBucketClient {
	u, err := url.Parse(s.Endpoint)
	if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" || s.AccessKey == "" || s.SecretKey == "" || s.Bucket == "" {
		return nil
	}
	if !s3endpoint.PathStyle(u, s.Bucket, s.PathStyle) {
		u.Host = s.Bucket + "." + u.Host
		if u.Path == "" {
			u.Path = "/"
		}
	} else {
		u.Path = strings.TrimSuffix(u.Path, "/") + "/" + s.Bucket
		if u.RawPath != "" {
			u.RawPath = strings.TrimSuffix(u.RawPath, "/") + "/" + url.PathEscape(s.Bucket)
		}
	}
	if s.Region == "" {
		s.Region = "us-east-1"
	}
	return &storageBucketClient{url: u, config: s}
}

func checkStorageBucket(ctx context.Context, client *storageBucketClient) string {
	if client == nil {
		return "Storage S3 dependency: bucket endpoint or credentials are not configured"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, client.url.String(), nil)
	if err != nil {
		return "Storage S3 dependency: bucket endpoint or credentials are not configured"
	}
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	req = signer.SignV4(*req, client.config.AccessKey, client.config.SecretKey, "", client.config.Region)
	response, err := storageHTTPClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return "Storage S3 dependency: observation timed out"
		}
		return "Storage S3 dependency: authenticated bucket HEAD unavailable"
	}
	response.Body.Close()
	switch response.StatusCode {
	case http.StatusOK, http.StatusNoContent:
		return ""
	case http.StatusUnauthorized, http.StatusForbidden:
		return "Storage S3 dependency: authentication or bucket access denied"
	case http.StatusNotFound:
		return "Storage S3 dependency: configured bucket does not exist"
	}
	return "Storage S3 dependency: authenticated bucket HEAD unavailable"
}
