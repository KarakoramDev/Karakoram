// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

const jsonResponseLimit int64 = 16 << 20

type client struct {
	origin       string
	region       string
	namespace    string
	token        atomic.Value // string
	http         *http.Client
	observations *infra.ReadBudget
}

func newClient(origin, region, namespace, token string, tlsConfig *tls.Config) *client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	if tlsConfig == nil {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	transport.TLSClientConfig = tlsConfig.Clone()
	c := &client{origin: origin, region: region, namespace: namespace, http: &http.Client{Transport: transport, Timeout: 60 * time.Second, CheckRedirect: noRedirect}}
	c.observations = infra.ObservationBudget("nomad\x00" + origin)
	c.setToken(token)
	return c
}

func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
func (c *client) setToken(token string)               { c.token.Store(token) }
func (c *client) currentToken() string {
	if token := c.token.Load(); token != nil {
		return token.(string)
	}
	return ""
}

func (c *client) get(ctx context.Context, path string, query url.Values, out any) (uint64, error) {
	return c.requestJSON(ctx, http.MethodGet, path, query, nil, out, 0)
}
func (c *client) put(ctx context.Context, path string, query url.Values, body, out any) error {
	_, err := c.requestJSON(ctx, http.MethodPut, path, query, body, out, 0)
	return err
}
func (c *client) post(ctx context.Context, path string, query url.Values, body, out any) error {
	_, err := c.requestJSON(ctx, http.MethodPost, path, query, body, out, 0)
	return err
}
func (c *client) delete(ctx context.Context, path string, query url.Values, out any) error {
	_, err := c.requestJSON(ctx, http.MethodDelete, path, query, nil, out, 0)
	return err
}
func (c *client) getRaw(ctx context.Context, path string, query url.Values, limit int64) ([]byte, error) {
	data, _, err := c.request(ctx, http.MethodGet, path, query, nil, limit, 0, true)
	return data, err
}
func (c *client) getBlocking(ctx context.Context, path string, query url.Values, index uint64, wait time.Duration, out any) (uint64, error) {
	if wait < 0 {
		return 0, rejectedConfig("nomadInvalidWait", "blocking query wait must not be negative")
	}
	query = cloneQuery(query)
	query.Set("index", strconv.FormatUint(index, 10))
	query.Set("wait", wait.String())
	return c.requestJSON(ctx, http.MethodGet, path, query, nil, out, wait)
}

func cloneQuery(query url.Values) url.Values {
	result := make(url.Values, len(query)+2)
	for key, values := range query {
		result[key] = append([]string(nil), values...)
	}
	return result
}

func (c *client) requestJSON(ctx context.Context, method, path string, query url.Values, body, out any, wait time.Duration) (uint64, error) {
	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		if err != nil {
			return 0, rejectedConfig("nomadInvalidJSON", "cannot encode request")
		}
	}
	data, index, err := c.request(ctx, method, path, query, encoded, jsonResponseLimit, wait, false)
	if err != nil {
		return 0, err
	}
	if out != nil && len(data) != 0 {
		if json.Unmarshal(data, out) != nil {
			return 0, silentFailure("invalid JSON response")
		}
	}
	return index, nil
}

func (c *client) request(ctx context.Context, method, path string, query url.Values, body []byte, limit int64, wait time.Duration, truncate bool) ([]byte, uint64, error) {
	if limit <= 0 || limit == 1<<63-1 {
		return nil, 0, rejectedConfig("nomadInvalidLimit", "response limit must be positive and bounded")
	}
	parsed, err := url.Parse(path)
	if err != nil || !strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") || parsed.IsAbs() || parsed.Host != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, 0, rejectedConfig("nomadInvalidPath", "API path must be relative to the configured origin")
	}
	query = cloneQuery(query)
	if _, ok := query["namespace"]; !ok {
		query.Set("namespace", c.namespace)
	}
	if c.region != "" {
		if _, ok := query["region"]; !ok {
			query.Set("region", c.region)
		}
	}
	target := c.origin + path
	if len(query) != 0 {
		target += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
	if err != nil {
		return nil, 0, silentFailure("cannot construct request")
	}
	token := c.currentToken()
	if token != "" {
		req.Header.Set("X-Nomad-Token", token)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	h := *c.http
	h.CheckRedirect = noRedirect
	if h.Timeout == 0 {
		h.Timeout = 60 * time.Second
	}
	h.Timeout += wait
	if method == http.MethodGet && infra.IsObservation(ctx) {
		if err := c.observations.Acquire(ctx); err != nil {
			return nil, 0, err
		}
		defer c.observations.Release()
	}
	resp, err := h.Do(req)
	if err != nil {
		return nil, 0, transportError(err)
	}
	defer resp.Body.Close()
	if (resp.StatusCode < 200 || resp.StatusCode >= 300) && (strings.HasPrefix(parsed.Path, "/v1/var/") || parsed.Path == "/v1/vars") {
		// CAS failures may contain secret Items. Preserve only the exact
		// plaintext revoked-token marker, never text extracted from JSON.
		message := "variable request failed"
		if resp.StatusCode == http.StatusConflict {
			message = "stale variable index"
		} else if resp.StatusCode == http.StatusBadRequest {
			marker, err := io.ReadAll(io.LimitReader(resp.Body, 65))
			if err == nil && len(marker) < 65 && strings.TrimSpace(string(marker)) == "ACL token not found" {
				message = "ACL token not found"
			}
		}
		return nil, 0, responseRetryAfter(resp, &APIError{Status: resp.StatusCode, Op: method, Message: message})
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, 0, silentFailure("cannot read platform response")
	}
	if int64(len(data)) > limit {
		if !truncate {
			return nil, 0, silentFailure("platform response exceeds size limit")
		}
		data = data[:limit]
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, 0, responseRetryAfter(resp, responseError(resp.StatusCode, method, data, token))
	}
	var index uint64
	if value := resp.Header.Get("X-Nomad-Index"); value != "" {
		index, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			return nil, 0, silentFailure("invalid Nomad response index")
		}
	}
	return data, index, nil
}

func responseRetryAfter(resp *http.Response, cause error) error {
	if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode != http.StatusServiceUnavailable {
		return cause
	}
	now := time.Now()
	value := strings.TrimSpace(resp.Header.Get("Retry-After"))
	var at time.Time
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds >= 0 && seconds <= int64((time.Duration(1<<63-1))/time.Second) {
		at = now.Add(time.Duration(seconds) * time.Second)
	} else {
		at, _ = http.ParseTime(value)
	}
	if at.After(now) {
		return &infra.RetryAfterError{Cause: cause, At: at}
	}
	return cause
}
