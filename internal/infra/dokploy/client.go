// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"
	"time"
)

const (
	jsonResponseLimit int64 = 8 << 20
	logsResponseLimit int64 = 32 << 20
	getRetries              = 2
	retryBase               = 100 * time.Millisecond
)

type client struct {
	origin string
	http   *http.Client
	token  atomic.Value // string
}

func newClient(origin, token string) *client {
	c := &client{origin: origin, http: &http.Client{Timeout: 2 * time.Minute, CheckRedirect: noRedirect}}
	c.setToken(token)
	return c
}

func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
func (c *client) setToken(token string)               { c.token.Store(token) }
func (c *client) currentToken() string {
	value, _ := c.token.Load().(string)
	return value
}

func (c *client) get(ctx context.Context, procedure string, query map[string]string, out any) error {
	params := make(url.Values, len(query))
	values := make([]string, 0, len(query))
	for key, value := range query {
		params.Set(key, value)
		values = append(values, value)
	}
	target := c.origin + "/api/" + procedure
	if len(params) != 0 {
		target += "?" + params.Encode()
	}
	limit := jsonResponseLimit
	if procedure == "application.readLogs" {
		limit = logsResponseLimit
	}
	return c.request(ctx, http.MethodGet, target, nil, 0, "", out, limit, values, nil, false)
}

func (c *client) post(ctx context.Context, procedure string, body any, out any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return silentFailure("cannot encode request")
	}
	return c.request(ctx, http.MethodPost, c.origin+"/api/"+procedure, bytes.NewReader(data), int64(len(data)), "application/json", out, jsonResponseLimit, nil, data, false)
}

func (c *client) upload(ctx context.Context, appID, zipPath string) error {
	file, err := os.Open(zipPath)
	if err != nil {
		return silentFailure("cannot open deployment archive")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return silentFailure("cannot read deployment archive")
	}
	// Build only the small multipart framing in memory. The archive remains a
	// file reader throughout and net/http sends the exact known Content-Length.
	var framing bytes.Buffer
	writer := multipart.NewWriter(&framing)
	if err := writer.WriteField("applicationId", appID); err != nil {
		return silentFailure("cannot encode upload")
	}
	if _, err := writer.CreateFormFile("zip", "karakoram.zip"); err != nil {
		return silentFailure("cannot encode upload")
	}
	headerLength := framing.Len()
	if err := writer.Close(); err != nil {
		return silentFailure("cannot encode upload")
	}
	data := framing.Bytes()
	body := io.MultiReader(bytes.NewReader(data[:headerLength]), io.LimitReader(file, info.Size()), bytes.NewReader(data[headerLength:]))
	return c.request(ctx, http.MethodPost, c.origin+"/api/application.dropDeployment", body, int64(len(data))+info.Size(), writer.FormDataContentType(), nil, jsonResponseLimit, []string{appID}, nil, true)
}

func (c *client) request(ctx context.Context, method, target string, body io.Reader, size int64, contentType string, out any, limit int64, values []string, requestJSON []byte, upload bool) error {
	// Enforce redirect and timeout policy even when tests/operators replace the
	// transport. Never mutate a shared http.Client during requests.
	h := *c.http
	h.CheckRedirect = noRedirect
	if upload {
		h.Timeout = 15 * time.Minute
	} else if h.Timeout == 0 {
		h.Timeout = 2 * time.Minute
	}
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, method, target, body)
		if err != nil {
			return silentFailure("cannot construct request")
		}
		req.ContentLength = size
		token := c.currentToken()
		req.Header.Set("x-api-key", token)
		req.Header.Set("Accept", "application/json")
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		resp, requestErr := h.Do(req)
		retry := false
		if requestErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			err = silentFailure("network request failed")
			retry = true
		} else {
			retry = resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
			data, readErr := io.ReadAll(io.LimitReader(resp.Body, limit+1))
			resp.Body.Close()
			switch {
			case readErr != nil:
				err = silentFailure("cannot read platform response")
				retry = true
			case int64(len(data)) > limit:
				err = silentFailure("platform response exceeds size limit")
				retry = false
			case resp.StatusCode < 200 || resp.StatusCode >= 300:
				err = responseError(resp.StatusCode, data, token, append([]string(nil), values...), requestJSON)
			case out != nil:
				if json.Unmarshal(data, out) != nil {
					err = silentFailure("invalid JSON response")
				}
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil {
			return nil
		}
		if method != http.MethodGet || !retry || attempt >= getRetries {
			return err
		}
		timer := time.NewTimer(retryBase << attempt)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
