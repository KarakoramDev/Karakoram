// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
)

func localManagementClient(c *agentapi.MemberConfig, s *rmqspec.MemberSpec) (*http.Client, string, error) {
	scheme, port := "http", s.ManagementPort
	transport := &http.Transport{Proxy: nil}
	if s.ManagementTLSPort > 0 {
		scheme, port = "https", s.ManagementTLSPort
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(c.TLS.CAPEM)) {
			return nil, "", errors.New("management observation CA unavailable")
		}
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: strings.TrimPrefix(s.NodeName, "rabbit@")}
	}
	client := &http.Client{Transport: transport, Timeout: 4 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return client, scheme + "://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), nil
}

// managedAccessCurrent proves that bootstrap would be a no-op. Every predicate
// is read fresh with the desired credential; unknown/partial/drifted data falls
// through to the existing cookie-authenticated repair. Customer users/vhosts are
// neither inspected nor modified. No management statistics are requested.
func managedAccessCurrent(ctx context.Context, c agentapi.MemberConfig) bool {
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	s, err := spec(c)
	if err != nil {
		return false
	}
	client, base, err := localManagementClient(&c, &s)
	if err != nil {
		return false
	}
	defer client.CloseIdleConnections()
	get := func(user, path string, out any, status int) bool {
		credential, exists := c.SystemUsers[user]
		if !exists || credential.Password == "" {
			return false
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/"+path, nil)
		if err != nil {
			return false
		}
		req.SetBasicAuth(user, credential.Password)
		response, err := client.Do(req)
		if err != nil {
			return false
		}
		defer response.Body.Close()
		if response.StatusCode != status {
			return false
		}
		if out == nil {
			return true
		}
		data, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
		return err == nil && len(data) <= 64<<10 && json.Unmarshal(data, out) == nil
	}
	var current [3]bool
	var probes sync.WaitGroup
	for i, user := range []string{"karakoram", "admin"} {
		probes.Go(func() {
			var identity struct {
				Name string   `json:"name"`
				Tags []string `json:"tags"`
			}
			if !get(user, "whoami", &identity, http.StatusOK) || identity.Name != user || len(identity.Tags) != 1 || identity.Tags[0] != "administrator" {
				return
			}
			var permission struct {
				User      string `json:"user"`
				VHost     string `json:"vhost"`
				Configure string `json:"configure"`
				Write     string `json:"write"`
				Read      string `json:"read"`
			}
			if !get(user, "permissions/"+url.PathEscape("/")+"/"+user, &permission, http.StatusOK) {
				return
			}
			current[i] = permission.User == user && permission.VHost == "/" && permission.Configure == ".*" && permission.Write == ".*" && permission.Read == ".*"
		})
	}
	probes.Go(func() { current[2] = get("karakoram", "users/guest", nil, http.StatusNotFound) })
	probes.Wait()
	return current[0] && current[1] && current[2]
}
