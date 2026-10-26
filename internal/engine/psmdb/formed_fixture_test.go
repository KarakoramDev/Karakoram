// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package psmdb

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"net"
	"net/url"
	"testing"
	"time"
)

// seedFormedServices makes native-stage fixtures include the durable platform
// resources that must already exist when a database has reached that stage.
func seedFormedServices(t *testing.T, p *Provider, rc *engine.Context, f *fakeInfra, s *internalState) {
	t.Helper()
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	for _, m := range s.Members {
		if m.Removing {
			continue
		}
		token, err := rc.Secrets.GetAgentToken(t.Context(), rc.Cluster.Ref(), m.Name)
		if err != nil {
			t.Fatal(err)
		}
		specs := []infra.ServiceSpec{serviceSpec(f.Layout(), rc.Cluster, m, token.Token)}
		if m.Volume != "" {
			specs = append(specs, engine.VolumeServiceSpec(rc.Cluster.Name, m.Name, m.Volume, token.Token, m.Disk))
		}
		op, err := f.ImportServices(t.Context(), specs)
		if err != nil {
			t.Fatal(err)
		}
		m.ServiceID, m.VolumeID = op.Services[m.Name], op.Services[m.Volume]
	}
}

func healthyTestReplication(_ context.Context, _ *engine.Context, s *internalState, m *member) (mongoadmin.Status, error) {
	rs := mongoadmin.Status{Set: m.Set}
	for i, n := range s.setMembers(m.Set) {
		state := "SECONDARY"
		if i == 0 {
			state = "PRIMARY"
		}
		rs.Members = append(rs.Members, mongoadmin.Member{Name: n.Name + ":27017", State: state, Health: 1, OptimeDate: time.Now()})
	}
	return rs, nil
}

// One HTTPS fixture represents the explicitly inventoried instance, not DNS.
type pinnedFixtureDialer struct{ testDialer }

func (d pinnedFixtureDialer) Dial(_ context.Context, _ domain.ClusterRef, _ string) (*agentapi.Client, error) {
	endpoint, err := url.Parse(d.url)
	if err != nil {
		return nil, err
	}
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: d.url, Token: "test-token", CAPEM: d.ca, DialInstanceContext: func(ctx context.Context, network, logical, instance string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, endpoint.Host)
	}})
}
