// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"net/url"
)

func (*Provider) Connection(ctx context.Context, rc *engine.Context) (domain.ConnectionDetails, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return domain.ConnectionDetails{}, err
	}
	pw, err := secret(ctx, rc, "users/neo4j")
	if err != nil {
		return domain.ConnectionDetails{}, err
	}
	ca, err := secret(ctx, rc, "tls/ca.crt")
	if err != nil {
		return domain.ConnectionDetails{}, err
	}
	conn, err := discovery(ctx, rc, s)
	if err != nil {
		return domain.ConnectionDetails{}, err
	}
	u, err := url.Parse(conn.URI)
	if err != nil {
		return domain.ConnectionDetails{}, err
	}
	u.User = url.UserPassword("neo4j", string(pw))
	return domain.ConnectionDetails{Connection: conn, CACertificate: string(ca), Credentials: []domain.ConnectionCredential{{Username: "neo4j", Password: string(pw), Database: "neo4j", URI: u.String()}}}, nil
}
func (*Provider) SystemUsers(context.Context, *engine.Context) ([]domain.SystemUser, error) {
	return []domain.SystemUser{{Name: "neo4j", Roles: []domain.RoleRef{}}}, nil
}
func (*Provider) MemberLogs(ctx context.Context, rc *engine.Context, name, process string, lines int) ([]domain.MemberLogLine, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return nil, err
	}
	if s.Member == nil || name != s.Member.Name {
		return nil, engine.ErrNotFound
	}
	if process != "neo4j" {
		return nil, engine.ErrInvalid
	}
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), name)
	if err != nil {
		return nil, err
	}
	defer a.Close()
	logs, err := a.Logs(ctx, process, lines)
	if err != nil {
		return nil, err
	}
	out := make([]domain.MemberLogLine, 0, len(logs.Lines))
	for _, line := range logs.Lines {
		out = append(out, domain.MemberLogLine{Timestamp: line.Timestamp, Member: name, Process: line.Process, Stream: line.Stream, Message: line.Message})
	}
	return out, nil
}
func (p *Provider) Placement(ctx context.Context, rc *engine.Context) (domain.Placement, error) {
	status, err := p.Status(ctx, rc)
	return status.Placement, err
}
func (*Provider) ListDatabaseUsers(context.Context, *engine.Context) ([]domain.DatabaseUser, error) {
	return nil, engine.ErrNotSupported
}
func (*Provider) GetDatabaseUser(context.Context, *engine.Context, string, string) (*domain.DatabaseUser, error) {
	return nil, engine.ErrNotSupported
}
func (*Provider) CreateDatabaseUser(context.Context, *engine.Context, domain.DatabaseUser, string) (string, error) {
	return "", engine.ErrNotSupported
}
func (*Provider) UpdateDatabaseUser(context.Context, *engine.Context, domain.DatabaseUser, string) error {
	return engine.ErrNotSupported
}
func (*Provider) DeleteDatabaseUser(context.Context, *engine.Context, string, string) error {
	return engine.ErrNotSupported
}
func (*Provider) ListDatabaseRoles(context.Context, *engine.Context) ([]domain.DatabaseRole, error) {
	return nil, engine.ErrNotSupported
}
func (*Provider) GetDatabaseRole(context.Context, *engine.Context, string, string) (*domain.DatabaseRole, error) {
	return nil, engine.ErrNotSupported
}
func (*Provider) CreateDatabaseRole(context.Context, *engine.Context, domain.DatabaseRole) error {
	return engine.ErrNotSupported
}
func (*Provider) UpdateDatabaseRole(context.Context, *engine.Context, domain.DatabaseRole) error {
	return engine.ErrNotSupported
}
func (*Provider) DeleteDatabaseRole(context.Context, *engine.Context, string, string) error {
	return engine.ErrNotSupported
}
func (*Provider) Balancer(context.Context, *engine.Context) (domain.BalancerStatus, error) {
	return domain.BalancerStatus{}, engine.ErrNotSupported
}
func (*Provider) SetBalancer(context.Context, *engine.Context, bool) error {
	return engine.ErrNotSupported
}
func (*Provider) PITRWindow(context.Context, *engine.Context) ([]domain.TimeWindow, error) {
	return nil, engine.ErrNotSupported
}

var _ engine.DataPlane = (*Provider)(nil)
