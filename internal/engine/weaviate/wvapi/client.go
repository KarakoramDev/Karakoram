// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package wvapi implements the Weaviate v1 REST administration API.
// Only connection-establishment failures permit failover: uncertain mutations
// are never replayed. Clients are safe for concurrent use.
//
// Wire models and routes follow the v1.40.1 OpenAPI. The v1.39.12 API has the
// same routes, but shard responses lack per_node_status; the legacy aggregate
// status and vectorQueueSize fields are deprecated in v1.40.1. Collection helpers
// should explicitly set Vectorizer: v1.40.1 no longer uses the global default.
// Enterprise backup deduplication controls introduced in v1.40.1 are deliberately
// absent from BackupConfig. Permission.Extra and nested scope Extra fields retain
// unknown native permission properties without overriding typed fields. There is
// no native cluster permission scope: read_cluster is an action without a scope.
package wvapi

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
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

// Config is immutable after New. Endpoints are member host:port addresses (or
// absolute URLs with the selected Scheme). HTTPS trusts only CAPEM. The TLS
// ServerName is derived from each endpoint hostname, not the dialer's address.
type Config struct {
	Endpoints []string
	Scheme    string
	CAPEM     []byte
	// ExpiredLeafPins maps endpoint hostnames to the exact DER active leaf.
	// Only an expired, byte-equal leaf may be verified at its validity midpoint;
	// all other certificates retain normal chain, hostname and time checks.
	ExpiredLeafPins  map[string][]byte
	APIKey           string
	DialContext      func(context.Context, string, string) (net.Conn, error)
	Timeout          time.Duration
	MaxResponseBytes int64
}

// Client holds immutable credentials and a pooled, bounded HTTP transport.
type Client struct {
	endpoints                  []*url.URL
	http                       *http.Client
	apiKey, username, password string
	timeout                    time.Duration
	maxResponseBytes           int64
}

// New constructs the public REST client. Scheme defaults to https, timeout to
// 30 seconds and MaxResponseBytes to 16 MiB. Redirects are never followed.
func New(cfg Config) (*Client, error) { return newClient(cfg, "", "") }

// NewInternal constructs a distinct client for private data-port endpoints
// (normally member:7947). It uses basic auth, never the public bearer API key.
// The caller must explicitly select Scheme "http" for upstream's plaintext API.
func NewInternal(cfg Config, username, password string) (*Client, error) {
	cfg.APIKey = ""
	return newClient(cfg, username, password)
}

func newClient(cfg Config, username, password string) (*Client, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("no Weaviate endpoints configured")
	}
	if cfg.Scheme == "" {
		cfg.Scheme = "https"
	}
	if cfg.Scheme != "http" && cfg.Scheme != "https" {
		return nil, errors.New("invalid Weaviate scheme")
	}
	if cfg.Timeout < 0 || cfg.MaxResponseBytes < 0 || cfg.MaxResponseBytes == int64(^uint64(0)>>1) {
		return nil, errors.New("invalid Weaviate request bounds")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.MaxResponseBytes == 0 {
		cfg.MaxResponseBytes = 16 << 20
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.Scheme == "https" {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(cfg.CAPEM) {
			return nil, errors.New("invalid Weaviate CA")
		}
		tlsConfig.RootCAs = roots
		if len(cfg.ExpiredLeafPins) > 0 {
			pins := make(map[string][]byte, len(cfg.ExpiredLeafPins))
			for host, der := range cfg.ExpiredLeafPins {
				pins[host] = bytes.Clone(der)
			}
			// Verification is performed below, including hostname and roots.
			tlsConfig.InsecureSkipVerify = true
			tlsConfig.VerifyConnection = func(cs tls.ConnectionState) error {
				if len(cs.PeerCertificates) == 0 {
					return errors.New("missing Weaviate server certificate")
				}
				if cs.ServerName == "" {
					return errors.New("missing Weaviate TLS hostname")
				}
				leaf := cs.PeerCertificates[0]
				at := time.Now()
				if at.After(leaf.NotAfter) && bytes.Equal(pins[cs.ServerName], leaf.Raw) {
					at = leaf.NotBefore.Add(leaf.NotAfter.Sub(leaf.NotBefore) / 2)
				}
				intermediates := x509.NewCertPool()
				for _, cert := range cs.PeerCertificates[1:] {
					intermediates.AddCert(cert)
				}
				_, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, DNSName: cs.ServerName, CurrentTime: at})
				return err
			}
		}
	}
	c := &Client{apiKey: cfg.APIKey, username: username, password: password, timeout: cfg.Timeout, maxResponseBytes: cfg.MaxResponseBytes}
	for _, endpoint := range cfg.Endpoints {
		if !strings.Contains(endpoint, "://") {
			endpoint = cfg.Scheme + "://" + endpoint
		}
		u, err := url.Parse(endpoint)
		if err != nil || u.Scheme != cfg.Scheme || u.Hostname() == "" || u.Port() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return nil, errors.New("invalid Weaviate endpoint")
		}
		c.endpoints = append(c.endpoints, u)
	}
	dial := cfg.DialContext
	if dial == nil {
		dial = (&net.Dialer{Timeout: cfg.Timeout, KeepAlive: 30 * time.Second}).DialContext
	}
	tr := &http.Transport{DialContext: dial, TLSClientConfig: tlsConfig, TLSHandshakeTimeout: cfg.Timeout, ResponseHeaderTimeout: cfg.Timeout, IdleConnTimeout: 90 * time.Second, MaxIdleConns: 32, MaxIdleConnsPerHost: 4, MaxConnsPerHost: 16, DisableCompression: true}
	c.http = &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return c, nil
}

// Close releases idle connections without interrupting requests.
func (c *Client) Close() { c.http.CloseIdleConnections() }

// Error is a native HTTP failure. Message is bounded and credential-redacted.
type Error struct {
	Status  int
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("Weaviate HTTP %d: %s", e.Status, e.Message) }
func IsNotFound(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Status == http.StatusNotFound
}
func IsConflict(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Status == http.StatusConflict
}

// Redact named credentials as well as opaque token-shaped strings. Native API
// keys contain long random components; never trust an error response as public.
var opaqueSecret = regexp.MustCompile(`[A-Za-z0-9_+/=-]{24,}`)
var labeledSecret = regexp.MustCompile(`(?i)(bearer\s+|(?:api[-_ ]?key|token|password)["' ]*\s*[:=]\s*["']?)[^\s,"'}]+`)

func (c *Client) sanitize(message string) string {
	for _, secret := range []string{c.apiKey, c.password, base64.StdEncoding.EncodeToString([]byte(c.username + ":" + c.password))} {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "[redacted]")
		}
	}
	message = labeledSecret.ReplaceAllString(message, "${1}[redacted]")
	message = opaqueSecret.ReplaceAllString(message, "[redacted]")
	return strings.Map(func(r rune) rune {
		if r < 32 && r != '\t' {
			return ' '
		}
		return r
	}, message)
}

func (c *Client) do(ctx context.Context, method, path string, q url.Values, body, out any) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		if err != nil {
			return errors.New("encoding Weaviate request failed")
		}
	}
	for _, endpoint := range c.endpoints {
		u := *endpoint
		decoded, err := url.PathUnescape(path)
		if err != nil {
			return errors.New("invalid Weaviate request path")
		}
		u.Path = decoded
		u.RawPath = path
		u.RawQuery = q.Encode()
		var connected atomic.Bool
		trace := &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { connected.Store(true) }}
		req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), method, u.String(), bytes.NewReader(data))
		if err != nil {
			return errors.New("creating Weaviate request failed")
		}
		req.Header.Set("Accept", "application/json")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if c.apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+c.apiKey)
		} else if c.username != "" {
			req.SetBasicAuth(c.username, c.password)
		}
		resp, err := c.http.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if connected.Load() {
				return errors.New("connection to Weaviate failed after establishment")
			}
			continue
		}
		payload, readErr := io.ReadAll(io.LimitReader(resp.Body, c.maxResponseBytes+1))
		resp.Body.Close()
		if readErr != nil {
			return errors.New("reading Weaviate response failed")
		}
		if int64(len(payload)) > c.maxResponseBytes {
			return errors.New("response from Weaviate exceeds configured limit")
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			message := http.StatusText(resp.StatusCode)
			var native struct {
				Error []struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if json.Unmarshal(payload, &native) == nil && len(native.Error) > 0 {
				parts := make([]string, 0, len(native.Error))
				for _, e := range native.Error {
					parts = append(parts, e.Message)
				}
				message = strings.Join(parts, "; ")
			}
			if len(data) != 0 {
				message = strings.ReplaceAll(message, string(data), "[redacted request]")
			}
			// Unstructured internal errors may echo request contents. Do not expose them.
			return &Error{Status: resp.StatusCode, Message: c.sanitize(message)}
		}
		if out != nil && json.Unmarshal(payload, out) != nil {
			return errors.New("decoding Weaviate JSON response failed")
		}
		return nil
	}
	return errors.New("connecting to Weaviate endpoints failed")
}
func segment(s string) string { return url.PathEscape(s) }
