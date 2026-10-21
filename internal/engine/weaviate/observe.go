// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvspec"
	"slices"
)

func (p *Provider) native(ctx context.Context, rc *engine.Context, s *state, names ...string) (*wvapi.Client, error) {
	return p.nativeClient(ctx, rc, s, nil, names...)
}

func (p *Provider) nativeClient(ctx context.Context, rc *engine.Context, s *state, expiredPins map[string][]byte, names ...string) (*wvapi.Client, error) {
	ca, e := secret(ctx, rc, secretCA)
	if e != nil {
		return nil, e
	}
	// A user-provided CA may have been replaced before rotate-tls. Trust the
	// still-active owned configurations as well until the last old leaf rolls.
	if rc.Cluster.Spec.TLS.CA == "userProvided" {
		for _, m := range s.Members {
			raw, err := secret(ctx, rc, "config/"+m.Name)
			if missing(err) {
				continue
			}
			if err != nil {
				return nil, err
			}
			var active agentapi.MemberConfig
			if err = json.Unmarshal(raw, &active); err != nil {
				return nil, err
			}
			if active.TLS.CAPEM != "" && !bytes.Contains(ca, []byte(active.TLS.CAPEM)) {
				ca = append(ca, '\n')
				ca = append(ca, active.TLS.CAPEM...)
			}
		}
	}
	key, e := secret(ctx, rc, secretAdmin)
	if e != nil {
		return nil, e
	}
	cfg := wvapi.Config{CAPEM: ca, APIKey: string(key), Scheme: "https", Timeout: engine.ObservationTimeout}
	cfg.ExpiredLeafPins = expiredPins
	if len(names) == 0 {
		for _, m := range ordered(s) {
			if !m.Removing && (m.Stage == "deployed" || m.Stage == "ready") {
				names = append(names, m.Name)
			}
		}
	}
	mode := ""
	if len(names) > 0 {
		if m := s.find(names[0]); m != nil {
			mode = m.TLSMode
		}
	}
	if mode == "disabled" {
		cfg.Scheme = "http"
	}
	for _, name := range names {
		if m := s.find(name); m != nil && m.TLSMode != mode {
			continue // Rolling TLS-mode changes may temporarily mix listeners.
		}
		cfg.Endpoints = append(cfg.Endpoints, name+":8080")
	}
	if rc.Dialer != nil {
		cfg.DialContext = rc.Dialer.DialContext
	}
	return wvapi.New(cfg)
}
func (p *Provider) memberReady(ctx context.Context, rc *engine.Context, m *member) (agentapi.Status, wvspec.ReadinessResponse, error) {
	var st agentapi.Status
	var ready wvspec.ReadinessResponse
	ctx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	if rc.Agents == nil {
		return st, ready, engine.ErrUnavailable
	}
	a, e := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
	if e != nil {
		return st, ready, e
	}
	defer a.Close()
	st, e = a.Status(ctx)
	if e != nil {
		return st, ready, e
	}
	if e = a.Engine(ctx, "weaviate", "readiness", wvspec.ReadinessRequest{}, &ready); e != nil {
		return st, ready, e
	}
	if st.Error != "" || st.ConfigVersion < m.ConfigVersion || ready.ConfigVersion < m.ConfigVersion || !ready.Ready || ready.Version != m.DesiredVersion || st.Processes["weaviate"].State != "running" {
		return st, ready, fmt.Errorf("member %s has not acknowledged native readiness for its active configuration", m.Name)
	}
	return st, ready, nil
}

// expiredActiveLeaves reads active configurations, never desired certificate
// secrets. A healthy endpoint cannot hide another member's expired active leaf.
func expiredActiveLeaves(ctx context.Context, rc *engine.Context, s *state) (map[string][]byte, error) {
	var expired map[string][]byte
	for _, m := range s.Members {
		raw, err := secret(ctx, rc, "config/"+m.Name)
		if missing(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var active agentapi.MemberConfig
		if err = json.Unmarshal(raw, &active); err != nil {
			return nil, err
		}
		if active.TLS.Mode == "disabled" {
			continue
		}
		block, _ := pem.Decode([]byte(active.TLS.CertPEM))
		if block == nil {
			continue
		}
		leaf, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, err
		}
		if !now(rc).Before(leaf.NotAfter) {
			if expired == nil {
				expired = make(map[string][]byte)
			}
			expired[m.Name] = leaf.Raw
		}
	}
	return expired, nil
}

func (p *Provider) clusterReady(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	expired, err := expiredActiveLeaves(ctx, rc, s)
	if err != nil {
		return false, err
	}
	for _, m := range s.Members {
		if expired[m.Name] != nil {
			return false, fmt.Errorf("native certificate expired for member %s", m.Name)
		}
	}
	return p.observeClusterReady(ctx, rc, s, nil)
}

// observeClusterReady is called with pins only by validated replacement-leaf
// rolling gates. All ordinary readiness gates use clusterReady.
func (p *Provider) observeClusterReady(ctx context.Context, rc *engine.Context, s *state, expiredPins map[string][]byte) (bool, error) {
	if len(s.Members) == 0 {
		return false, nil
	}
	for _, m := range ordered(s) {
		if m.Removing || m.Stage != "deployed" && m.Stage != "ready" {
			return false, nil
		}
		if _, _, e := p.memberReady(ctx, rc, m); e != nil {
			return false, e
		}
	}
	c, e := p.nativeClient(ctx, rc, s, expiredPins)
	if e != nil {
		return false, e
	}
	defer c.Close()
	nodes, e := c.Nodes(ctx)
	if e != nil {
		return false, e
	}
	seen := map[string]bool{}
	for _, n := range nodes {
		if s.find(n.Name) != nil {
			if n.Status != "HEALTHY" {
				return false, nil
			}
			seen[n.Name] = true
		}
	}
	for _, m := range s.Members {
		if !seen[m.Name] {
			return false, nil
		}
	}
	stats, e := c.ClusterStatistics(ctx)
	if e != nil {
		return false, e
	}
	return validateClusterStatistics(s, stats)
}

func validateClusterStatistics(s *state, stats wvapi.ClusterStatistics) (bool, error) {
	if !stats.Synchronized {
		return false, nil
	}
	if len(stats.Statistics) != len(s.Members) {
		return false, nil
	}
	seen := map[string]bool{}
	for _, st := range stats.Statistics {
		if s.find(st.Name) == nil {
			continue
		}
		var leader string
		_ = json.Unmarshal(st.LeaderID, &leader)
		if !st.Ready || !st.DBLoaded || !st.Open || !st.Bootstrapped || st.Status != "HEALTHY" || !slices.Contains(s.Voters, leader) || st.IsVoter != slices.Contains(s.Voters, st.Name) {
			return false, nil
		}
		var configuration []struct {
			ID       string `json:"id"`
			Suffrage int    `json:"suffrage"`
		}
		if e := json.Unmarshal(st.Raft.LatestConfiguration, &configuration); e != nil {
			return false, e
		}
		voters := []string{}
		for _, peer := range configuration {
			if peer.Suffrage == 0 {
				voters = append(voters, peer.ID)
			}
		}
		if len(voters) != len(s.Voters) {
			return false, nil
		}
		for _, name := range s.Voters {
			if !slices.Contains(voters, name) {
				return false, nil
			}
		}
		seen[st.Name] = true
	}
	for _, m := range s.Members {
		if !seen[m.Name] {
			return false, nil
		}
	}
	return true, nil
}
