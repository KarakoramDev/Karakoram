// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package baoapi is a minimal OpenBao HTTP API client shared by the control
// plane and the member agent. It never includes request bodies, tokens or
// response secrets in errors, and it does not follow redirects.
package baoapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// MaxResponseBytes bounds ordinary JSON responses.
const MaxResponseBytes = 4 << 20

// Client talks to one OpenBao server address.
type Client struct {
	base  *url.URL
	http  *http.Client
	token string
}

// New returns a client for baseURL (https://host:port). The supplied
// http.Client carries the transport (dialer, CA, server name); redirects are
// always refused.
func New(baseURL string, httpClient *http.Client) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.Path != "" && u.Path != "/" {
		return nil, fmt.Errorf("invalid OpenBao address %q", baseURL)
	}
	u.Path = ""
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	hc := *httpClient
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{base: u, http: &hc}, nil
}

// WithToken returns a copy authenticating with token.
func (c *Client) WithToken(token string) *Client {
	cp := *c
	cp.token = token
	return &cp
}

// WithTimeout returns a copy with a timeout suitable for streaming snapshots.
// The caller's context still bounds the complete transfer.
func (c *Client) WithTimeout(timeout time.Duration) *Client {
	cp := *c
	hc := *c.http
	hc.Timeout = timeout
	cp.http = &hc
	return &cp
}

// Address returns the base URL.
func (c *Client) Address() string { return c.base.String() }

// Error is a non-2xx OpenBao response. Server messages are deliberately omitted:
// an upstream server or plugin can echo secret request values in its diagnostics.
type Error struct {
	StatusCode int
	Method     string
	Path       string
}

func (e *Error) Error() string {
	return fmt.Sprintf("openbao %s %s: HTTP %d", e.Method, e.Path, e.StatusCode)
}

// IsStatus reports whether err is an *Error with the given status code.
func IsStatus(err error, code int) bool {
	var e *Error
	return errors.As(err, &e) && e.StatusCode == code
}

// Do performs method on /v1/<path> with an optional JSON body and decodes a
// JSON response into out (when non-nil and the response has a body).
func (c *Client) Do(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("openbao %s %s: encode request: %w", method, cleanPath(path), err)
		}
		reader = bytes.NewReader(data)
	}
	resp, err := c.raw(ctx, method, path, reader, "application/json")
	if err != nil {
		return err
	}
	defer drain(resp.Body)
	if out == nil {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("openbao %s %s: read response: %w", method, cleanPath(path), err)
	}
	if len(data) > MaxResponseBytes {
		return fmt.Errorf("openbao %s %s: response too large", method, cleanPath(path))
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("openbao %s %s: decode response: %w", method, cleanPath(path), err)
	}
	return nil
}

// Stream performs a request whose response body the caller consumes; the
// caller must close it. Errors are returned for non-2xx answers.
func (c *Client) Stream(ctx context.Context, method, path string, body io.Reader, contentType string) (io.ReadCloser, error) {
	resp, err := c.raw(ctx, method, path, body, contentType)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (c *Client) raw(ctx context.Context, method, path string, body io.Reader, contentType string) (*http.Response, error) {
	p := cleanPath(path)
	u := *c.base
	rel, err := url.Parse("/v1/" + p)
	if err != nil {
		return nil, fmt.Errorf("openbao %s %s: invalid path", method, p)
	}
	u.Path, u.RawQuery = rel.Path, rel.RawQuery
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	if body != nil && contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if c.token != "" {
		req.Header.Set("X-Vault-Token", c.token)
	}
	req.Header.Set("X-Vault-Request", "true")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openbao %s %s: %w", method, rel.Path, redactURLError(err))
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp, nil
	}
	defer drain(resp.Body)
	e := &Error{StatusCode: resp.StatusCode, Method: method, Path: rel.Path}
	// Do not decode server error strings: they can reflect tokens or shares.
	return nil, e
}

func cleanPath(p string) string { return strings.TrimPrefix(strings.TrimPrefix(p, "/"), "v1/") }

func drain(r io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(r, 64<<10))
	_ = r.Close()
}

// redactURLError strips the URL (which never carries secrets, but may carry
// internal addresses) only from *url.Error wrapping, keeping the cause.
func redactURLError(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}
