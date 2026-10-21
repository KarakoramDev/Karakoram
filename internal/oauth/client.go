// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package oauth

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	ID         string   `json:"client_id"`
	Name       string   `json:"client_name"`
	Redirects  []string `json:"redirect_uris"`
	AuthMethod string   `json:"token_endpoint_auth_method"`
}
type cachedClient struct {
	client  Client
	expires time.Time
	err     error
}

func validRedirect(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" {
		return false
	}
	ip := net.ParseIP(u.Hostname())
	return u.Scheme == "https" || u.Scheme == "http" && ip != nil && ip.IsLoopback()
}
func redirectMatches(registered, requested string) bool {
	if registered == requested {
		return true
	}
	a, e := url.Parse(registered)
	b, f := url.Parse(requested)
	if e != nil || f != nil || a.Scheme != "http" || b.Scheme != "http" {
		return false
	}
	ip := net.ParseIP(a.Hostname())
	if ip == nil || !ip.IsLoopback() || a.Hostname() != b.Hostname() {
		return false
	}
	a.Host = a.Hostname()
	b.Host = b.Hostname()
	return a.String() == b.String()
}
func validateClient(c Client) error {
	if strings.TrimSpace(c.Name) == "" || len(c.Name) > 128 || len(c.Redirects) == 0 || len(c.Redirects) > 20 || c.AuthMethod != "none" {
		return errors.New("invalid client metadata")
	}
	for _, r := range c.Redirects {
		if !validRedirect(r) {
			return errors.New("invalid redirect URI")
		}
	}
	return nil
}
func (s *Service) client(ctx context.Context, id string) (c Client, resultErr error) {
	if !strings.HasPrefix(id, "https://") {
		err := s.o.Store.OAuthGet(ctx, "client", id, &c)
		return c, err
	}
	u, err := url.Parse(id)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" || u.Path == "" {
		return c, errors.New("invalid client ID")
	}
	for _, segment := range strings.Split(u.Path, "/") {
		if segment == "." || segment == ".." {
			return c, errors.New("invalid client ID path")
		}
	}
	if len(s.o.ClientHosts) > 0 {
		found := false
		for _, h := range s.o.ClientHosts {
			if strings.EqualFold(h, u.Hostname()) {
				found = true
			}
		}
		if !found {
			return c, errors.New("client host not allowed")
		}
	}
	s.clientsMu.Lock()
	if v, ok := s.clients[id]; ok && v.expires.After(s.now()) {
		s.clientsMu.Unlock()
		return v.client, v.err
	}
	if len(s.clients) >= 256 {
		clear(s.clients)
	}
	// Reserve this ID briefly so concurrent requests cannot amplify a slow
	// fetch. Neither DNS nor HTTP holds the credential lifecycle/cache locks.
	s.clients[id] = cachedClient{err: errors.New("client metadata fetch in progress"), expires: s.now().Add(5 * time.Second)}
	s.clientsMu.Unlock()
	defer func() {
		ttl := 5 * time.Minute
		if resultErr != nil {
			ttl = 5 * time.Second
			c = Client{}
		}
		s.clientsMu.Lock()
		s.clients[id] = cachedClient{client: c, err: resultErr, expires: s.now().Add(ttl)}
		s.clientsMu.Unlock()
	}()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ips, err := s.o.Resolve(ctx, u.Hostname())
	if err != nil || len(ips) == 0 {
		return c, errors.New("client host resolution failed")
	}
	for _, ip := range ips {
		if !backupstorage.EndpointAddressAllowed(ip, false) {
			return c, errors.New("client host is not public")
		}
	}
	port := u.Port()
	if port == "" {
		port = "443"
	}
	transport := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return s.o.Dial(ctx, network, net.JoinHostPort(ips[0].String(), port))
	}, DisableKeepAlives: true, TLSHandshakeTimeout: 3 * time.Second}
	defer transport.CloseIdleConnections()
	client := metadataHTTPClient(transport)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, id, nil)
	if err != nil {
		return c, err
	}
	response, err := client.Do(req)
	if err != nil {
		return c, errors.New("client metadata fetch failed")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return c, errors.New("client metadata fetch failed")
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 5121))
	if err != nil || len(raw) > 5120 {
		return c, errors.New("client metadata too large")
	}
	if json.Unmarshal(raw, &c) != nil || c.ID != id {
		return c, errors.New("client ID mismatch")
	}
	if c.AuthMethod == "" {
		c.AuthMethod = "none"
	}
	if err = validateClient(c); err != nil {
		return c, err
	}
	return c, nil
}

func metadataHTTPClient(transport http.RoundTripper) *http.Client {
	return &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func defaultResolve(ctx context.Context, host string) ([]netip.Addr, error) {
	return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
}
