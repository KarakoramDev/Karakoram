// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baoapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
)

func (p *Provider) agent(ctx context.Context, rc *engine.Context, name string) (*agentapi.Client, error) {
	return rc.Agents.Dial(ctx, rc.Cluster.Ref(), name)
}
func (p *Provider) call(ctx context.Context, rc *engine.Context, name, action string, req, resp any) error {
	a, err := p.agent(ctx, rc, name)
	if err != nil {
		return err
	}
	defer a.Close()
	return a.Engine(ctx, "openbao", action, req, resp)
}
func (p *Provider) agentStatus(ctx context.Context, rc *engine.Context, name string) (agentapi.Status, error) {
	ctx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	a, err := p.agent(ctx, rc, name)
	if err != nil {
		return agentapi.Status{}, err
	}
	defer a.Close()
	return a.Status(ctx)
}
func (p *Provider) nativeStatus(ctx context.Context, rc *engine.Context, name string) (baospec.StatusResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	var out baospec.StatusResponse
	err := p.call(ctx, rc, name, baospec.ActionStatus, struct{}{}, &out)
	return out, err
}

// recoveryClient never falls back to the customer/API listener. A separately
// issued client certificate is required in addition to server CA/hostname checks.
func (p *Provider) recoveryClient(ctx context.Context, rc *engine.Context, name string) (*baoapi.Client, error) {
	cert, err := memberCertificate(ctx, rc, secretRecoveryClient)
	if err != nil {
		return nil, err
	}
	pair, err := tls.X509KeyPair([]byte(cert.CertPEM), []byte(cert.KeyPEM))
	if err != nil {
		return nil, err
	}
	return p.memberAPIClient(ctx, rc, name, "", baospec.RecoveryPort, &pair)
}

func (p *Provider) nativeClient(ctx context.Context, rc *engine.Context, name, token string) (*baoapi.Client, error) {
	return p.memberAPIClient(ctx, rc, name, token, baospec.APIPort, nil)
}

func (p *Provider) memberAPIClient(ctx context.Context, rc *engine.Context, name, token string, port int, certificate *tls.Certificate) (*baoapi.Client, error) {
	ca, err := secret(ctx, rc, "tls/ca.crt")
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("invalid OpenBao cluster CA")
	}
	tr := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 10 * time.Second, DisableKeepAlives: true}
	if certificate != nil {
		tr.TLSClientConfig.Certificates = []tls.Certificate{*certificate}
	}
	if rc.Dialer != nil {
		tr.DialContext = rc.Dialer.DialContext
	}
	c, err := baoapi.New("https://"+net.JoinHostPort(name, strconv.Itoa(port)), &http.Client{Transport: tr, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }})
	if err != nil {
		return nil, err
	}
	return c.WithToken(token), nil
}
func (p *Provider) activeClient(ctx context.Context, rc *engine.Context, s *internalState, token string) (*baoapi.Client, *member, error) {
	var last error
	for _, m := range ordered(s) {
		if m.Stage == "planned" || m.Stage == "imported" || m.ResumeDeploy != nil {
			continue
		}
		c, err := p.nativeClient(ctx, rc, m.Name, token)
		if err != nil {
			last = err
			continue
		}
		leader, err := c.Leader(ctx)
		if err == nil && leader.IsSelf {
			return c, m, nil
		}
		if err != nil {
			last = err
		}
	}
	return nil, nil, fmt.Errorf("no active OpenBao member: %w", errors.Join(engine.ErrUnavailable, last))
}
