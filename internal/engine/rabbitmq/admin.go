// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/mgmt"
)

// adminClient selects an observed healthy broker and verifies TLS against the cluster CA.
func (p *Provider) adminClient(ctx context.Context, rc *engine.Context) (*mgmt.Client, error) {
	s, err := loadState(rc.Cluster)
	if err != nil {
		return nil, err
	}
	members := s.runningMembers()
	if len(members) == 0 {
		return nil, fmt.Errorf("%w: no running RabbitMQ members", engine.ErrConflict)
	}
	password, err := secret(ctx, rc, "users/karakoram")
	if err != nil {
		return nil, err
	}
	var last error
	for _, m := range members {
		transport := &http.Transport{TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 10 * time.Second, DisableKeepAlives: true}
		if rc.Dialer != nil {
			transport.DialContext = rc.Dialer.DialContext
		}
		scheme, port := "http", 15672
		mode := m.TLSMode
		if mode == "" {
			mode = rc.Cluster.Spec.TLS.Mode
		}
		if mode != "disabled" && mode != "" {
			ca, err := secret(ctx, rc, "tls/ca.crt")
			if err != nil {
				return nil, err
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(ca) {
				return nil, fmt.Errorf("invalid RabbitMQ CA certificate")
			}
			transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}
			scheme, port = "https", 15671
		}
		client, err := mgmt.New(scheme+"://"+net.JoinHostPort(m.Name, strconv.Itoa(port)), "karakoram", string(password), &http.Client{Transport: transport, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }})
		if err != nil {
			return nil, err
		}
		if _, err = client.Overview(ctx); err == nil {
			return client, nil
		}
		last = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("no healthy RabbitMQ Management API member: %w", last)
}
