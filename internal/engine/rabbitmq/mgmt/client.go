// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package mgmt implements the RabbitMQ Management HTTP API.
package mgmt

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

type Client struct {
	base               *url.URL
	username, password string
	http               *http.Client
}

// HTTPError reports a status without exposing a potentially secret-bearing response body.
type HTTPError struct {
	StatusCode int
	Cause      error
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("RabbitMQ Management API HTTP %d", e.StatusCode)
}
func (e *HTTPError) Unwrap() error { return e.Cause }

// New uses the supplied transport, including any namespace relay or pinned TLS configuration.
func New(baseURL, username, password string, client *http.Client) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, engine.Invalid("invalid Management API URL")
	}
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	return &Client{base: u, username: username, password: password, http: client}, nil
}

// Close releases idle transport connections.
func (c *Client) Close() { c.http.CloseIdleConnections() }

func (c *Client) request(ctx context.Context, method string, parts []string, body any, out any) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var b []byte
	var err error
	if raw, ok := body.(json.RawMessage); ok {
		b = raw
	} else if body != nil {
		b, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	escaped := make([]string, len(parts))
	for i, p := range parts {
		escaped[i] = url.PathEscape(p)
	}
	target := strings.TrimRight(c.base.String(), "/") + "/api/" + strings.Join(escaped, "/")
	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.SetBasicAuth(c.username, c.password)
	// Some write resources (e.g. vhost-limits) produce no representation and
	// answer 406 to a strict JSON Accept header, so only demand JSON when decoding.
	if out != nil {
		req.Header.Set("Accept", "application/json")
	} else {
		req.Header.Set("Accept", "*/*")
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("RabbitMQ Management API: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Do not echo server response bodies: an error may contain submitted credentials.
		var sentinel error
		switch resp.StatusCode {
		case 404:
			sentinel = engine.ErrNotFound
		case 409, 412:
			sentinel = engine.ErrConflict
		case 400, 422:
			sentinel = engine.ErrInvalid
			if method == http.MethodPut && len(parts) > 0 {
				// Recognize fixed native diagnostics without exposing submitted data.
				var rejection struct {
					Reason json.RawMessage `json:"reason"`
				}
				_ = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&rejection)
				reason := string(rejection.Reason)
				switch parts[0] {
				case "permissions", "topic-permissions":
					if reason == `"vhost_or_user_not_found"` {
						sentinel = engine.InvalidField("user,vhost", domain.ReasonNotFound, "user or vhost does not exist in RabbitMQ")
					} else if parts[0] == "permissions" {
						sentinel = engine.InvalidField("configure,write,read", domain.ReasonInvalid, "configure, write or read contains a regular expression rejected by RabbitMQ")
					} else {
						sentinel = engine.InvalidField("write,read", domain.ReasonInvalid, "write or read contains a regular expression rejected by RabbitMQ")
					}
				case "policies", "operator-policies":
					sentinel = engine.InvalidField("pattern,definition", domain.ReasonInvalid, "pattern or definition was rejected by RabbitMQ; check regular expression syntax and supported policy definition keys and values")
					if strings.Contains(reason, "not a recognised policy setting") || strings.Contains(reason, "not a recognized policy setting") || strings.Contains(reason, "are not recognised policy settings") || strings.Contains(reason, "are not recognized policy settings") {
						sentinel = engine.InvalidField("definition", domain.ReasonInvalid, "definition contains a policy setting not supported by RabbitMQ")
					} else if strings.Contains(reason, "invalid regular expression") {
						sentinel = engine.InvalidField("pattern", domain.ReasonInvalid, "pattern contains a regular expression rejected by RabbitMQ")
					}
				}
			}
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return &HTTPError{StatusCode: resp.StatusCode, Cause: sentinel}
	}
	if out == nil {
		_, err = io.Copy(io.Discard, resp.Body)
		return err
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, (64<<20)+1))
	if err != nil {
		return err
	}
	if len(data) > 64<<20 {
		return fmt.Errorf("RabbitMQ Management API response too large")
	}
	if raw, ok := out.(*[]byte); ok {
		*raw = data
		return nil
	}
	if err = json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode RabbitMQ Management API response: %w", err)
	}
	return nil
}

type Overview struct {
	RabbitMQVersion string           `json:"rabbitmq_version"`
	ClusterName     string           `json:"cluster_name"`
	ObjectTotals    map[string]int64 `json:"object_totals"`
}
type Node struct {
	Name          string   `json:"name"`
	Running       bool     `json:"running"`
	MemAlarm      bool     `json:"mem_alarm"`
	DiskFreeAlarm bool     `json:"disk_free_alarm"`
	MemUsed       int64    `json:"mem_used"`
	DiskFree      int64    `json:"disk_free"`
	Partitions    []string `json:"partitions"`
}
type Vhost struct {
	Name             string         `json:"name"`
	DefaultQueueType string         `json:"default_queue_type,omitempty"`
	Description      string         `json:"description,omitempty"`
	Tags             []string       `json:"tags,omitempty"`
	Limits           map[string]int `json:"limits,omitempty"`
}
type User struct {
	Name             string   `json:"name"`
	Tags             []string `json:"tags"`
	PasswordHash     string   `json:"password_hash"`
	HashingAlgorithm string   `json:"hashing_algorithm"`
}
type UserWrite struct {
	Password         *string  `json:"password,omitempty"`
	PasswordHash     *string  `json:"password_hash,omitempty"`
	Tags             []string `json:"tags"`
	HashingAlgorithm string   `json:"hashing_algorithm,omitempty"`
}
type Permission struct {
	Vhost     string `json:"vhost"`
	User      string `json:"user"`
	Configure string `json:"configure"`
	Write     string `json:"write"`
	Read      string `json:"read"`
}
type TopicPermission struct {
	Vhost    string `json:"vhost"`
	User     string `json:"user"`
	Exchange string `json:"exchange"`
	Write    string `json:"write"`
	Read     string `json:"read"`
}
type Policy struct {
	Vhost      string         `json:"vhost"`
	Name       string         `json:"name"`
	Pattern    string         `json:"pattern"`
	ApplyTo    string         `json:"apply-to"`
	Priority   int            `json:"priority"`
	Definition map[string]any `json:"definition"`
}

func (c *Client) Overview(ctx context.Context) (Overview, error) {
	var v Overview
	e := c.request(ctx, "GET", []string{"overview"}, nil, &v)
	return v, e
}
func (c *Client) Nodes(ctx context.Context) ([]Node, error) {
	v := []Node{}
	e := c.request(ctx, "GET", []string{"nodes"}, nil, &v)
	return v, e
}
func (c *Client) Alarms(ctx context.Context) error {
	return c.request(ctx, "GET", []string{"health", "checks", "alarms"}, nil, nil)
}

// NodeIsQuorumCritical returns true for the health check's documented 503 response.
func (c *Client) NodeIsQuorumCritical(ctx context.Context) (bool, error) {
	e := c.request(ctx, "GET", []string{"health", "checks", "node-is-quorum-critical"}, nil, nil)
	var status *HTTPError
	if errors.As(e, &status) && status.StatusCode == http.StatusServiceUnavailable {
		return true, nil
	}
	return false, e
}
func (c *Client) ExportDefinitions(ctx context.Context) ([]byte, error) {
	var v []byte
	e := c.request(ctx, "GET", []string{"definitions"}, nil, &v)
	return v, e
}
func (c *Client) ImportDefinitions(ctx context.Context, data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(trimmed) {
		return engine.Invalid("definitions must be a JSON object")
	}
	return c.request(ctx, "POST", []string{"definitions"}, json.RawMessage(data), nil)
}

// VhostWrite is the Management API's comma-separated metadata representation.
type VhostWrite struct {
	DefaultQueueType string `json:"default_queue_type,omitempty"`
	Description      string `json:"description"`
	Tags             string `json:"tags"`
}
