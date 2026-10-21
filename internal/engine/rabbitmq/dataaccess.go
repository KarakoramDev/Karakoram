// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strconv"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/mgmt"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

var _ engine.DataMcpProvider = (*Provider)(nil)

func (*Provider) DataMcp(c *domain.Cluster) (engine.DataMcpBackend, error) {
	backend := engine.DataMcpBackend{Kind: engine.DataMcpNative, Upstream: "native-rabbitmq"}
	if c == nil {
		return backend, invalid("cluster is nil")
	}
	if c.Spec.DataMcp == nil {
		return backend, nil
	}
	scope := c.Spec.DataMcp.Scope
	if len(scope.Databases)+len(scope.Schemas)+len(scope.IndexPatterns) != 0 {
		return backend, invalid("RabbitMQ data MCP supports only vhosts scope")
	}
	for _, v := range scope.Vhosts {
		if err := accessName(v); err != nil {
			return backend, err
		}
	}
	return backend, nil
}

func dataAccessUsers(c *domain.Cluster) []string {
	ro, rw := engine.DataAccessLevels(c)
	var users []string
	if ro {
		users = append(users, engine.DataAccessReadUser)
	}
	if rw {
		users = append(users, engine.DataAccessReadWriteUser)
	}
	return users
}

// Reconcile through the managed administrator after cookie-authenticated access
// repair. Enumerating vhosts on each pass also grants access to newly created
// vhosts when no explicit scope is set. Only reserved identities are mutated.
func (p *Provider) reconcileDataAccess(ctx context.Context, rc *engine.Context, s *internalState) error {
	if !s.Bootstrapped || len(s.runningMembers()) == 0 {
		return nil
	}
	for _, m := range s.Members {
		if m.ChangeAction == "password" {
			return nil
		}
	}
	desired := dataAccessUsers(rc.Cluster)
	needed := len(desired) > 0
	for _, u := range []string{engine.DataAccessReadUser, engine.DataAccessReadWriteUser} {
		_, err := secret(ctx, rc, "users/"+u)
		if err == nil {
			needed = true
		} else if !missing(err) {
			return err
		}
	}
	if !needed {
		return nil
	}
	client, err := p.adminClient(ctx, rc)
	if err != nil {
		return err
	}
	defer client.Close()
	return reconcileDataAccessUsers(ctx, rc, client, desired)
}

func reconcileDataAccessUsers(ctx context.Context, rc *engine.Context, client *mgmt.Client, desired []string) error {
	permissions, err := client.ListPermissions(ctx)
	if err != nil {
		return err
	}
	var vhosts []mgmt.Vhost
	if len(desired) > 0 {
		vhosts, err = client.ListVhosts(ctx)
		if err != nil {
			return err
		}
	}
	for _, user := range []string{engine.DataAccessReadUser, engine.DataAccessReadWriteUser} {
		if !slices.Contains(desired, user) {
			if err := client.DeleteUser(ctx, user); err != nil && !missing(err) {
				return err
			}
			if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), "users/"+user); err != nil && !missing(err) {
				return err
			}
			continue
		}
		if err := ensureSecret(ctx, rc, "users/"+user, secrets.Password); err != nil {
			return err
		}
		password, err := secret(ctx, rc, "users/"+user)
		if err != nil {
			return err
		}
		pw := string(password)
		if err := client.PutUser(ctx, user, mgmt.UserWrite{Password: &pw, Tags: []string{"monitoring"}}); err != nil {
			return err
		}
		scope := rc.Cluster.Spec.DataMcp.Scope.Vhosts
		for _, permission := range permissions {
			if permission.User == user && len(scope) > 0 && !slices.Contains(scope, permission.Vhost) {
				if err := client.DeletePermission(ctx, permission.Vhost, user); err != nil && !missing(err) {
					return err
				}
			}
		}
		for _, vhost := range vhosts {
			if len(scope) > 0 && !slices.Contains(scope, vhost.Name) {
				continue
			}
			write := "^$"
			if user == engine.DataAccessReadWriteUser {
				write = ".*"
			}
			want := mgmt.Permission{Vhost: vhost.Name, User: user, Configure: "^$", Write: write, Read: ".*"}
			if slices.Contains(permissions, want) {
				continue
			}
			if err := client.PutPermission(ctx, want); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Provider) DataAccessCredentials(ctx context.Context, rc *engine.Context, level engine.DataAccessLevel) (engine.DataAccessCredentials, error) {
	var out engine.DataAccessCredentials
	if !level.Valid() {
		return out, invalid("invalid data access level")
	}
	if !slices.Contains(dataAccessUsers(rc.Cluster), engine.DataAccessUser(level)) {
		return out, fmt.Errorf("%w: data access level is disabled", engine.ErrConflict)
	}
	s, err := loadState(rc.Cluster)
	if err != nil {
		return out, err
	}
	out.Username = engine.DataAccessUser(level)
	pw, err := secret(ctx, rc, "users/"+out.Username)
	if err != nil {
		return out, err
	}
	out.Password = string(pw)
	// Do not mix TLS epochs during a rolling change: every returned host uses
	// the desired transport and has passed a fresh broker/agent health check.
	out.TLS = rc.Cluster.Spec.TLS.Mode != "" && rc.Cluster.Spec.TLS.Mode != "disabled"
	scheme, port := "http", 15672
	if out.TLS {
		scheme, port = "https", 15671
		out.CAPEM, err = secret(ctx, rc, "tls/ca.crt")
		if err != nil {
			return out, err
		}
	}
	for _, m := range s.runningMembers() {
		mode := m.TLSMode
		if mode == "" {
			mode = rc.Cluster.Spec.TLS.Mode
		}
		if (mode != "" && mode != "disabled") != out.TLS {
			continue
		}
		healthy, err := p.healthy(ctx, rc, m)
		if err != nil || !healthy {
			continue
		}
		out.Hosts = append(out.Hosts, scheme+"://"+net.JoinHostPort(m.Name, strconv.Itoa(port)))
	}
	if len(out.Hosts) == 0 {
		return engine.DataAccessCredentials{}, fmt.Errorf("%w: no healthy RabbitMQ members", engine.ErrConflict)
	}
	endpoint, err := url.Parse(out.Hosts[0])
	if err != nil {
		return out, err
	}
	endpoint.User = url.UserPassword(out.Username, out.Password)
	out.URI = endpoint.String()
	return out, nil
}
