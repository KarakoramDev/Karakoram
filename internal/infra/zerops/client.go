// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// DefaultBaseURL is the public Prague Zerops REST endpoint.
const DefaultBaseURL = "https://api.app-prg1.zerops.io/api/rest/public"

// Options configures authentication, bounded retries and synchronous backend waits.
// MaxRetries zero selects three retries; a negative value disables retries.
type Options struct {
	BaseURL, Token, ProjectID            string
	HTTPClient                           *http.Client
	MaxRetries                           int
	RetryBase, PollInterval, WaitTimeout time.Duration
}

// Client is a context-aware, credential-silent thin Zerops REST client.
type Client struct {
	base, token, project string
	http                 *http.Client
	retries              int
	retryBase            time.Duration
}

// NewClient constructs a client. Credentials are never included in errors or logs.
func NewClient(o Options) (*Client, error) {
	if o.BaseURL == "" {
		o.BaseURL = DefaultBaseURL
	}
	u, err := url.Parse(o.BaseURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("zerops: invalid base URL")
	}
	if o.Token == "" || o.ProjectID == "" {
		return nil, errors.New("zerops: token and project ID required")
	}
	if o.HTTPClient == nil {
		o.HTTPClient = &http.Client{Timeout: 2 * time.Minute}
	}
	// Do not allow credentials to follow redirects, including custom transports.
	h := *o.HTTPClient
	h.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if o.MaxRetries == 0 {
		o.MaxRetries = 3
	}
	if o.MaxRetries < 0 {
		o.MaxRetries = 0
	}
	if o.RetryBase <= 0 {
		o.RetryBase = 250 * time.Millisecond
	}
	return &Client{base: strings.TrimRight(o.BaseURL, "/"), token: o.Token, project: o.ProjectID, http: &h, retries: o.MaxRetries, retryBase: o.RetryBase}, nil
}
func escape(s string) string                       { return url.PathEscape(s) }
func (c *Client) projectPath(suffix string) string { return "/project/" + escape(c.project) + suffix }
func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func (c *Client) json(ctx context.Context, method, path string, body, out any, retry bool) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return errors.New("zerops: cannot encode request")
		}
	}
	return c.request(ctx, method, c.base+path, func() io.Reader {
		if body == nil {
			return nil
		}
		return bytes.NewReader(data)
	}, int64(len(data)), "application/json", out, retry, true)
}

func networkFailure(err error) bool {
	var target *url.Error
	if errors.As(err, &target) {
		err = target.Err
	}
	var network net.Error
	return errors.As(err, &network) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}
func (c *Client) request(ctx context.Context, method, target string, body func() io.Reader, size int64, contentType string, out any, retry, auth bool) error {
	if method != http.MethodGet && method != http.MethodPut {
		retry = false
	}
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, target, body())
		if err != nil {
			return errors.New("zerops: invalid request")
		}
		req.ContentLength = size
		if contentType != "" && req.Body != nil {
			req.Header.Set("Content-Type", contentType)
		}
		if auth {
			req.Header.Set("Authorization", "Bearer "+c.token)
		}
		req.Header.Set("Accept", "application/json")
		resp, err := c.http.Do(req)
		var retryAfter time.Duration
		var retryable bool
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// net/http errors contain target URLs. In particular signed log URLs must not escape.
			retryable = networkFailure(err)
			err = errors.New("zerops: network request failed")
		} else {
			retryable = resp.StatusCode == 429 || (resp.StatusCode >= 500 && resp.StatusCode < 600)
			if n, e := strconv.Atoi(resp.Header.Get("Retry-After")); e == nil {
				retryAfter = time.Duration(n) * time.Second
			} else if t, e := http.ParseTime(resp.Header.Get("Retry-After")); e == nil {
				retryAfter = time.Until(t)
			}
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				var envelope struct {
					Error struct {
						Code, Message string
						Meta          json.RawMessage
					} `json:"error"`
				}
				_ = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&envelope)
				err = &APIError{StatusCode: resp.StatusCode, Code: envelope.Error.Code, Message: envelope.Error.Message, Meta: envelope.Error.Meta}
			} else if out != nil {
				if e := json.NewDecoder(resp.Body).Decode(out); e != nil {
					retryable = networkFailure(e)
					err = errors.New("zerops: invalid JSON response")
				}
			} else {
				_, err = io.Copy(io.Discard, resp.Body)
				retryable = networkFailure(err)
			}
			resp.Body.Close()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil {
			return nil
		}
		if !retry || !retryable || attempt >= c.retries {
			return err
		}
		delay := c.retryBase * time.Duration(1<<min(attempt, 8))
		if retryAfter > delay {
			delay = retryAfter
		}
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
		if err = sleep(ctx, delay); err != nil {
			return err
		}
	}
}
func getPages[T any](ctx context.Context, c *Client, path string) ([]T, error) {
	var all []T
	for offset := 0; ; {
		var p page[T]
		sep := "?"
		if strings.Contains(path, "?") {
			sep = "&"
		}
		if err := c.json(ctx, "GET", fmt.Sprintf("%s%slimit=100&offset=%d", path, sep, offset), nil, &p, true); err != nil {
			return nil, err
		}
		all = append(all, p.List...)
		if len(p.List) == 0 {
			if offset < p.Total {
				return nil, errors.New("zerops: truncated paginated response")
			}
			break
		}
		offset += len(p.List)
		if offset >= p.Total {
			break
		}
	}
	return all, nil
}

// ImportServices imports a services YAML document, without retrying creation.
func (c *Client) ImportServices(ctx context.Context, yaml string) (ImportResult, error) {
	var v ImportResult
	err := c.json(ctx, "POST", c.projectPath("/service-stack/import"), map[string]string{"yaml": yaml}, &v, false)
	return v, err
}

// ListServices follows pagination and excludes protected platform services.
func (c *Client) ListServices(ctx context.Context) ([]ServiceStack, error) {
	list, err := getPages[ServiceStack](ctx, c, c.projectPath("/service-stack"))
	if err != nil {
		return nil, err
	}
	out := list[:0]
	for _, s := range list {
		if !s.protected() {
			out = append(out, s)
		}
	}
	return out, nil
}

// GetService fetches a service by its platform ID.
func (c *Client) GetService(ctx context.Context, id string) (ServiceStack, error) {
	var v ServiceStack
	err := c.json(ctx, "GET", "/service-stack/"+escape(id), nil, &v, true)
	return v, err
}

// DeleteService schedules service deletion.
func (c *Client) DeleteService(ctx context.Context, id string) (Process, error) {
	var v Process
	err := c.json(ctx, "DELETE", "/service-stack/"+escape(id), nil, &v, false)
	return v, err
}

// Lifecycle schedules start, stop or restart. These process-creating PUTs are not replayed.
func (c *Client) Lifecycle(ctx context.Context, id, action string) (Process, error) {
	if action != "start" && action != "stop" && action != "restart" {
		return Process{}, errors.New("zerops: invalid lifecycle action")
	}
	var v Process
	err := c.json(ctx, "PUT", "/service-stack/"+escape(id)+"/"+action, struct{}{}, &v, false)
	return v, err
}

// CreateAppVersion allocates an artifact version without exposing its upload URL.
func (c *Client) CreateAppVersion(ctx context.Context, id, name string) (AppVersion, error) {
	var v AppVersion
	err := c.json(ctx, "POST", "/service-stack/"+escape(id)+"/app-version", map[string]string{"name": name}, &v, false)
	return v, err
}

// Upload sends raw gzip-tar bytes directly to the authenticated upload endpoint.
// A streaming upload is never replayed after an ambiguous network failure.
func (c *Client) Upload(ctx context.Context, id string, artifact io.Reader, size int64) error {
	var v struct {
		Success bool `json:"success"`
	}
	err := c.request(ctx, "PUT", c.base+"/app-version/"+escape(id)+"/upload", func() io.Reader { return artifact }, size, "application/octet-stream", &v, false, true)
	if err == nil && !v.Success {
		return errors.New("zerops: artifact upload rejected")
	}
	return err
}

// Deploy starts the prebuilt deployment with its complete runtime YAML.
func (c *Client) Deploy(ctx context.Context, id, yaml, setup string) (Process, error) {
	var v Process
	err := c.json(ctx, "PUT", "/app-version/"+escape(id)+"/deploy", map[string]string{"zeropsYaml": yaml, "zeropsYamlSetup": setup}, &v, false)
	return v, err
}

// Env lists entries, including sensitive content. Callers must never log entries.
func (c *Client) Env(ctx context.Context, id string) ([]EnvItem, error) {
	var v struct {
		Items []EnvItem `json:"items"`
	}
	err := c.json(ctx, "GET", "/service-stack/"+escape(id)+"/env", nil, &v, true)
	return v.Items, err
}

// SetUserData creates or updates a sensitive user entry (existingID empty means create).
func (c *Client) SetUserData(ctx context.Context, serviceID, existingID, key, value string) (Process, error) {
	method, path, retry := "POST", "/service-stack/"+escape(serviceID)+"/user-data", false
	if existingID != "" {
		method, path, retry = "PUT", "/user-data/"+escape(existingID), true
	}
	body := struct {
		Key       string `json:"key"`
		Content   string `json:"content"`
		Sensitive bool   `json:"sensitive"`
	}{key, value, true}
	var v Process
	err := c.json(ctx, method, path, body, &v, retry)
	return v, err
}

// Autoscaling replaces the COMPLETE custom autoscaling configuration.
func (c *Client) Autoscaling(ctx context.Context, id string, config map[string]json.RawMessage) (*Process, error) {
	var v struct {
		Process *Process `json:"process"`
	}
	err := c.json(ctx, "PUT", "/service-stack/"+escape(id)+"/autoscaling", map[string]any{"customAutoscaling": config}, &v, true)
	return v.Process, err
}

// PublicPorts follows pagination for the service's staged routes.
func (c *Client) PublicPorts(ctx context.Context, id string) ([]PublicPortRouting, error) {
	return getPages[PublicPortRouting](ctx, c, "/service-stack/"+escape(id)+"/public-port-routing")
}

// CreatePublicPort stages a public routing rule.
func (c *Client) CreatePublicPort(ctx context.Context, id string, p PublicPortRouting) (PublicPortRouting, error) {
	var v PublicPortRouting
	err := c.json(ctx, "POST", "/service-stack/"+escape(id)+"/public-port-routing", p, &v, false)
	return v, err
}

// DeletePublicPort stages deletion; deleted:false means synchronization is required.
func (c *Client) DeletePublicPort(ctx context.Context, id string) error {
	return c.json(ctx, "DELETE", "/public-port-routing/"+escape(id), nil, nil, false)
}

// SyncPublicPorts activates staged project routing changes.
func (c *Client) SyncPublicPorts(ctx context.Context) (Process, error) {
	var v Process
	err := c.json(ctx, "PUT", c.projectPath("/sync-public-port-routing"), struct{}{}, &v, false)
	return v, err
}

// ProjectInfo fetches project public addresses.
func (c *Client) ProjectInfo(ctx context.Context) (Project, error) {
	var v Project
	err := c.json(ctx, "GET", c.projectPath(""), nil, &v, true)
	return v, err
}

// Containers follows runtime container pagination.
func (c *Client) Containers(ctx context.Context, id string) ([]RuntimeContainer, error) {
	return getPages[RuntimeContainer](ctx, c, "/service-stack/"+escape(id)+"/container")
}

// GetProcess fetches an asynchronous process without waiting.
func (c *Client) GetProcess(ctx context.Context, id string) (Process, error) {
	var v Process
	err := c.json(ctx, "GET", "/process/"+escape(id), nil, &v, true)
	return v, err
}

// Logs fetches application facility 16 logs from the signed project log endpoint.
// The signed URL is never sent a bearer header or included in an error.
func (c *Client) Logs(ctx context.Context, id string, limit int) ([]RuntimeLog, error) {
	var access struct {
		URL string `json:"url"`
	}
	if err := c.json(ctx, "GET", c.projectPath("/log"), nil, &access, true); err != nil {
		return nil, err
	}
	u, err := url.Parse(access.URL)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil {
		return nil, errors.New("zerops: invalid log endpoint")
	}
	q := u.Query()
	q.Set("limit", strconv.Itoa(limit))
	q.Set("desc", "1")
	q.Set("facility", "16")
	q.Set("serviceStackId", id)
	u.RawQuery = q.Encode()
	var v struct {
		Items []RuntimeLog `json:"items"`
	}
	err = c.request(ctx, "GET", u.String(), func() io.Reader { return nil }, 0, "", &v, true, false)
	return v.Items, err
}
