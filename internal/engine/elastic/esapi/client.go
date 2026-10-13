// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package esapi implements Elasticsearch administration over CA-pinned HTTPS.
// Requests use endpoints in order. Only connection-establishment failures permit
// failover; mutations with an uncertain outcome are never replayed.
package esapi

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

// Config selects endpoints, authentication and the namespace transport.
type Config struct {
	Endpoints          []string
	CAPEM              []byte
	Username, Password string
	BearerToken        string
	DialContext        func(ctx context.Context, network, addr string) (net.Conn, error)
	Timeout            time.Duration
}

// Client is safe for concurrent use. Authentication and trust are immutable.
type Client struct {
	endpoints                  []*url.URL
	http                       *http.Client
	username, password, bearer string
}

// New constructs a client trusting only CAPEM, with hostname verification enabled.
func New(cfg Config) (*Client, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("no Elasticsearch endpoints configured")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(cfg.CAPEM) {
		return nil, errors.New("invalid Elasticsearch CA")
	}
	c := &Client{username: cfg.Username, password: cfg.Password, bearer: cfg.BearerToken}
	for _, s := range cfg.Endpoints {
		u, err := url.Parse(s)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return nil, errors.New("invalid Elasticsearch HTTPS endpoint")
		}
		c.endpoints = append(c.endpoints, u)
	}
	if cfg.Timeout < 0 {
		return nil, errors.New("invalid Elasticsearch timeout")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	dial := cfg.DialContext
	if dial == nil {
		dial = (&net.Dialer{Timeout: cfg.Timeout, KeepAlive: 30 * time.Second}).DialContext
	}
	tr := &http.Transport{DialContext: dial, TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, TLSHandshakeTimeout: cfg.Timeout, IdleConnTimeout: 90 * time.Second, MaxIdleConns: 32, MaxIdleConnsPerHost: 4}
	c.http = &http.Client{Transport: tr, Timeout: cfg.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return c, nil
}

// Close releases idle transport connections; it does not interrupt requests.
func (c *Client) Close() { c.http.CloseIdleConnections() }

// Error is an Elasticsearch HTTP error. Reason contains no client credentials.
type Error struct {
	Status       int
	Type, Reason string
}

// Error describes the HTTP status and Elasticsearch error type and reason.
func (e *Error) Error() string {
	return fmt.Sprintf("Elasticsearch HTTP %d (%s): %s", e.Status, e.Type, e.Reason)
}

// IsNotFound reports whether err represents HTTP 404.
func IsNotFound(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Status == http.StatusNotFound
}

// IsConflict reports whether err represents HTTP 409.
func IsConflict(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Status == http.StatusConflict
}

func (c *Client) do(ctx context.Context, method, path string, q url.Values, body, out any) error {
	return c.doSecret(ctx, method, path, q, body, out, nil)
}
func (c *Client) doSecret(ctx context.Context, method, path string, q url.Values, body, out any, secrets []string) error {
	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		if err != nil {
			return errors.New("encoding Elasticsearch request failed")
		}
	}
	for _, endpoint := range c.endpoints {
		u := *endpoint
		// path is already escaped segment-by-segment, preserving list delimiters.
		decoded, err := url.PathUnescape(path)
		if err != nil {
			return errors.New("invalid Elasticsearch request path")
		}
		u.Path = decoded
		u.RawPath = path
		u.RawQuery = q.Encode()
		var connected atomic.Bool
		trace := &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { connected.Store(true) }}
		req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), method, u.String(), bytes.NewReader(data))
		if err != nil {
			return errors.New("creating Elasticsearch request failed")
		}
		req.Header.Set("Accept", "application/json")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if c.bearer != "" {
			req.Header.Set("Authorization", "Bearer "+c.bearer)
		} else if c.username != "" {
			req.SetBasicAuth(c.username, c.password)
		}
		resp, err := c.http.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if connected.Load() {
				return errors.New("connection to Elasticsearch failed after establishment")
			}
			continue
		}
		err = c.decode(resp, out, secrets)
		resp.Body.Close()
		return err
	}
	return errors.New("connecting to Elasticsearch endpoints failed")
}
func (c *Client) decode(resp *http.Response, out any, secrets []string) error {
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var v struct {
			Error json.RawMessage `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&v)
		e := &Error{Status: resp.StatusCode, Reason: http.StatusText(resp.StatusCode)}
		var detail struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		}
		if json.Unmarshal(v.Error, &detail) == nil {
			e.Type = detail.Type
			e.Reason = detail.Reason
		} else {
			var reason string
			if json.Unmarshal(v.Error, &reason) == nil {
				e.Reason = reason
			}
		}
		sensitive := append([]string{base64.StdEncoding.EncodeToString([]byte(c.username + ":" + c.password)), c.password, c.bearer, c.username}, secrets...)
		for _, s := range sensitive {
			if s != "" {
				e.Type = strings.ReplaceAll(e.Type, s, "[redacted]")
				e.Reason = strings.ReplaceAll(e.Reason, s, "[redacted]")
			}
		}
		return e
	}
	if out == nil {
		_, err := io.Copy(io.Discard, resp.Body)
		if err != nil {
			return errors.New("reading Elasticsearch response failed")
		}
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return errors.New("decoding Elasticsearch JSON response failed")
	}
	return nil
}
func segment(s string) string { return url.PathEscape(s) }
func listPath(names []string) string {
	escaped := make([]string, len(names))
	for i, n := range names {
		escaped[i] = segment(n)
	}
	return strings.Join(escaped, ",")
}
func duration(d time.Duration) string {
	return fmt.Sprintf("%dms", (d+time.Millisecond-1)/time.Millisecond)
}
func boolQuery(key string, value bool) url.Values { return url.Values{key: {fmt.Sprint(value)}} }
