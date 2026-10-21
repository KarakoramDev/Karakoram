// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"net"
	"net/url"
	"strconv"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

var _ engine.DataPlane = (*Provider)(nil)

func connection(c *domain.Cluster, s *internalState) domain.Connection {
	out := domain.Connection{Hosts: []string{}, PublicHosts: c.Status.Connection.PublicHosts, Endpoints: []domain.Endpoint{}}
	for _, m := range s.runningMembers() {
		mode := m.TLSMode
		if mode == "" {
			mode = c.Spec.TLS.Mode
		}
		secure := mode != "disabled" && mode != ""
		scheme, port, mscheme, mport := "amqp", 5672, "http", 15672
		if secure {
			scheme, port, mscheme, mport = "amqps", 5671, "https", 15671
		}
		host := net.JoinHostPort(m.Name, strconv.Itoa(port))
		out.Hosts = append(out.Hosts, host)
		endpoint := scheme + "://" + host + "/%2F"
		if out.URI == "" {
			out.URI = endpoint
		}
		out.Endpoints = append(out.Endpoints, domain.Endpoint{Name: scheme, URL: endpoint}, domain.Endpoint{Name: "management", URL: mscheme + "://" + net.JoinHostPort(m.Name, strconv.Itoa(mport))})
		if mode == "preferTLS" {
			out.Endpoints = append(out.Endpoints, domain.Endpoint{Name: "amqp", URL: "amqp://" + net.JoinHostPort(m.Name, "5672") + "/%2F"})
		}
	}
	scheme := "amqp"
	if c.Spec.TLS.Mode != "disabled" && c.Spec.TLS.Mode != "" {
		scheme = "amqps"
	}
	for _, host := range out.PublicHosts {
		out.Endpoints = append(out.Endpoints, domain.Endpoint{Name: scheme, URL: scheme + "://" + host + "/%2F", Public: true})
	}
	for _, ep := range c.Status.Connection.Endpoints {
		if ep.Public && ep.Name == "management" {
			out.Endpoints = append(out.Endpoints, ep)
		}
	}
	return out
}

func (p *Provider) Connection(ctx context.Context, rc *engine.Context) (domain.ConnectionDetails, error) {
	s, e := loadState(rc.Cluster)
	if e != nil {
		return domain.ConnectionDetails{}, e
	}
	pw, e := secret(ctx, rc, "users/admin")
	if e != nil {
		return domain.ConnectionDetails{}, e
	}
	conn := connection(rc.Cluster, s)
	out := domain.ConnectionDetails{Connection: conn, Credentials: []domain.ConnectionCredential{}}
	secure := false
	for _, ep := range conn.Endpoints {
		secure = secure || ep.Name == "amqps"
	}
	if secure {
		ca, e := secret(ctx, rc, "tls/ca.crt")
		if e != nil {
			return domain.ConnectionDetails{}, e
		}
		out.CACertificate = string(ca)
	}
	for _, ep := range conn.Endpoints {
		if ep.Name != "amqp" && ep.Name != "amqps" {
			continue
		}
		u, e := url.Parse(ep.URL)
		if e != nil {
			return domain.ConnectionDetails{}, e
		}
		u.User = url.UserPassword("admin", string(pw))
		out.Credentials = append(out.Credentials, domain.ConnectionCredential{Username: "admin", Password: string(pw), Database: "/", URI: u.String()})
	}
	return out, nil
}
func (*Provider) SystemUsers(_ context.Context, rc *engine.Context) ([]domain.SystemUser, error) {
	users := []domain.SystemUser{{Name: "karakoram", Roles: []domain.RoleRef{{Role: "administrator", DB: "/"}}}, {Name: "admin", Roles: []domain.RoleRef{{Role: "administrator", DB: "/"}}}}
	if rc != nil {
		for _, name := range dataAccessUsers(rc.Cluster) {
			users = append(users, domain.SystemUser{Name: name, Roles: []domain.RoleRef{{Role: "monitoring"}}})
		}
	}
	return users, nil
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
				st, err := engine.ObserveAgentStatus(ctx, a, "RabbitMQ", m.Name)
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
	return engine.DescribePlacement(out.Groups, rc.Backend.Capabilities().Placement), nil
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
	if process != "rabbitmq-server" && process != "agent" {
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
