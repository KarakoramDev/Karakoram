// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"net"
	"strconv"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

var _ engine.DataPlane = (*Provider)(nil)

func connection(c *domain.Cluster, s *internalState) domain.Connection {
	out := domain.Connection{Hosts: []string{}, PublicHosts: c.Status.Connection.PublicHosts, Endpoints: []domain.Endpoint{}}
	for _, m := range s.runningMembers() {
		host := net.JoinHostPort(m.Name, strconv.Itoa(8200))
		uri := "https://" + host
		out.Hosts = append(out.Hosts, host)
		if out.URI == "" {
			out.URI = uri
		}
		out.Endpoints = append(out.Endpoints, domain.Endpoint{Name: "https", URL: uri})
	}
	for _, host := range out.PublicHosts {
		out.Endpoints = append(out.Endpoints, domain.Endpoint{Name: "https", URL: "https://" + host, Public: true})
	}
	return out
}
func (p *Provider) Connection(ctx context.Context, rc *engine.Context) (domain.ConnectionDetails, error) {
	s, err := loadState(rc.Cluster)
	if err != nil {
		return domain.ConnectionDetails{}, err
	}
	ca, err := secret(ctx, rc, "tls/ca.crt")
	if err != nil {
		return domain.ConnectionDetails{}, err
	}
	return domain.ConnectionDetails{Connection: connection(rc.Cluster, s), CACertificate: string(ca), Credentials: []domain.ConnectionCredential{}}, nil
}
func (*Provider) SystemUsers(context.Context, *engine.Context) ([]domain.SystemUser, error) {
	return []domain.SystemUser{}, nil
}
func (p *Provider) Placement(ctx context.Context, rc *engine.Context) (domain.Placement, error) {
	s, e := loadState(rc.Cluster)
	if e != nil {
		return domain.Placement{}, e
	}
	out := domain.Placement{Groups: map[string][]string{}}
	for _, m := range ordered(s) {
		fingerprint := ""
		if rc.Backend.Capabilities().Placement {
			containers, err := rc.Backend.Containers(ctx, m.Name)
			if err == nil {
				for _, container := range containers {
					if container.PlacementGroup != "" {
						fingerprint = container.PlacementGroup
						break
					}
				}
			}
		} else {
			a, err := p.agent(ctx, rc, m.Name)
			if err == nil {
				st, err := a.Status(ctx)
				a.Close()
				if err == nil {
					fingerprint = st.HostFingerprint
				}
			}
		}
		if fingerprint == "" {
			fingerprint = "unknown"
		}
		out.Groups[fingerprint] = append(out.Groups[fingerprint], m.Name)
	}
	out.Warning = "Best-effort observations only; host separation is not guaranteed."
	return out, nil
}
func (p *Provider) MemberLogs(ctx context.Context, rc *engine.Context, name, process string, lines int) ([]domain.MemberLogLine, error) {
	s, e := loadState(rc.Cluster)
	if e != nil {
		return nil, e
	}
	m := s.find(name)
	if m == nil {
		return nil, engine.ErrNotFound
	}
	if process != "openbao" && process != "agent" {
		return nil, engine.Invalid("unsupported log process")
	}
	if lines < 1 || lines > 10000 {
		return nil, engine.Invalid("log lines must be 1..10000")
	}
	if m.Stage == "planned" || m.Stage == "imported" {
		return nil, engine.ErrConflict
	}
	a, e := p.agent(ctx, rc, name)
	if e != nil {
		return nil, e
	}
	defer a.Close()
	logs, e := a.Logs(ctx, process, lines)
	if e != nil {
		return nil, e
	}
	out := make([]domain.MemberLogLine, 0, len(logs.Lines))
	for _, x := range logs.Lines {
		out = append(out, domain.MemberLogLine{Timestamp: x.Timestamp, Member: name, Process: process, Stream: x.Stream, Message: x.Message})
	}
	return out, nil
}
func (*Provider) ListDatabaseUsers(ctx context.Context, rc *engine.Context) ([]domain.DatabaseUser, error) {
	return nil, engine.ErrNotSupported
}
func (*Provider) GetDatabaseUser(ctx context.Context, rc *engine.Context, db, name string) (*domain.DatabaseUser, error) {
	return nil, engine.ErrNotSupported
}
func (*Provider) CreateDatabaseUser(ctx context.Context, rc *engine.Context, u domain.DatabaseUser, password string) (string, error) {
	return "", engine.ErrNotSupported
}
func (*Provider) UpdateDatabaseUser(ctx context.Context, rc *engine.Context, u domain.DatabaseUser, password string) error {
	return engine.ErrNotSupported
}
func (*Provider) DeleteDatabaseUser(ctx context.Context, rc *engine.Context, db, name string) error {
	return engine.ErrNotSupported
}
func (*Provider) ListDatabaseRoles(ctx context.Context, rc *engine.Context) ([]domain.DatabaseRole, error) {
	return nil, engine.ErrNotSupported
}
func (*Provider) GetDatabaseRole(ctx context.Context, rc *engine.Context, db, name string) (*domain.DatabaseRole, error) {
	return nil, engine.ErrNotSupported
}
func (*Provider) CreateDatabaseRole(ctx context.Context, rc *engine.Context, r domain.DatabaseRole) error {
	return engine.ErrNotSupported
}
func (*Provider) UpdateDatabaseRole(ctx context.Context, rc *engine.Context, r domain.DatabaseRole) error {
	return engine.ErrNotSupported
}
func (*Provider) DeleteDatabaseRole(ctx context.Context, rc *engine.Context, db, name string) error {
	return engine.ErrNotSupported
}
func (*Provider) Balancer(ctx context.Context, rc *engine.Context) (domain.BalancerStatus, error) {
	return domain.BalancerStatus{}, engine.ErrNotSupported
}
func (*Provider) SetBalancer(ctx context.Context, rc *engine.Context, enabled bool) error {
	return engine.ErrNotSupported
}
func (*Provider) PITRWindow(ctx context.Context, rc *engine.Context) ([]domain.TimeWindow, error) {
	return nil, engine.ErrNotSupported
}
