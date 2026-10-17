// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"net/url"
	"slices"
)

func (p *Core) Connection(ctx context.Context, rc *engine.Context) (domain.ConnectionDetails, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return domain.ConnectionDetails{}, err
	}
	password, err := secret(ctx, rc, "users/postgres")
	if err != nil {
		return domain.ConnectionDetails{}, err
	}
	ca, err := secret(ctx, rc, "tls/ca.crt")
	if err != nil {
		return domain.ConnectionDetails{}, err
	}
	conn, err := publicConnection(ctx, rc, discovery(rc, s))
	if err != nil {
		return domain.ConnectionDetails{}, err
	}
	u, err := url.Parse(conn.URI)
	if err != nil {
		return domain.ConnectionDetails{}, err
	}
	u.User = url.UserPassword("postgres", string(password))
	return domain.ConnectionDetails{Connection: conn, CACertificate: string(ca), Credentials: []domain.ConnectionCredential{{Username: "postgres", Password: string(password), Database: "postgres", URI: u.String()}}}, nil
}
func (*Core) SystemUsers(context.Context, *engine.Context) ([]domain.SystemUser, error) {
	users := make([]domain.SystemUser, 0, len(systemNames))
	for _, name := range systemNames {
		users = append(users, domain.SystemUser{Name: name, Roles: []domain.RoleRef{}})
	}
	return users, nil
}
func (*Core) MemberLogs(ctx context.Context, rc *engine.Context, name, process string, lines int) ([]domain.MemberLogLine, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return nil, err
	}
	if s.find(name) == nil {
		return nil, engine.ErrNotFound
	}
	if !slices.Contains([]string{"postgres", "patroni", "etcd", "proxysql"}, process) {
		return nil, engine.ErrInvalid
	}
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), name)
	if err != nil {
		return nil, err
	}
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
func (p *Core) Placement(ctx context.Context, rc *engine.Context) (domain.Placement, error) {
	status, err := p.Status(ctx, rc)
	return status.Placement, err
}
func (*Core) ListDatabaseUsers(context.Context, *engine.Context) ([]domain.DatabaseUser, error) {
	return nil, engine.ErrNotSupported
}
func (*Core) GetDatabaseUser(context.Context, *engine.Context, string, string) (*domain.DatabaseUser, error) {
	return nil, engine.ErrNotSupported
}
func (*Core) CreateDatabaseUser(context.Context, *engine.Context, domain.DatabaseUser, string) (string, error) {
	return "", engine.ErrNotSupported
}
func (*Core) UpdateDatabaseUser(context.Context, *engine.Context, domain.DatabaseUser, string) error {
	return engine.ErrNotSupported
}
func (*Core) DeleteDatabaseUser(context.Context, *engine.Context, string, string) error {
	return engine.ErrNotSupported
}
func (*Core) ListDatabaseRoles(context.Context, *engine.Context) ([]domain.DatabaseRole, error) {
	return nil, engine.ErrNotSupported
}
func (*Core) GetDatabaseRole(context.Context, *engine.Context, string, string) (*domain.DatabaseRole, error) {
	return nil, engine.ErrNotSupported
}
func (*Core) CreateDatabaseRole(context.Context, *engine.Context, domain.DatabaseRole) error {
	return engine.ErrNotSupported
}
func (*Core) UpdateDatabaseRole(context.Context, *engine.Context, domain.DatabaseRole) error {
	return engine.ErrNotSupported
}
func (*Core) DeleteDatabaseRole(context.Context, *engine.Context, string, string) error {
	return engine.ErrNotSupported
}
func (*Core) Balancer(context.Context, *engine.Context) (domain.BalancerStatus, error) {
	return domain.BalancerStatus{}, engine.ErrNotSupported
}
func (*Core) SetBalancer(context.Context, *engine.Context, bool) error { return engine.ErrNotSupported }
