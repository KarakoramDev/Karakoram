// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"net/url"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/users"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

var _ engine.DataMcpProvider = (*Provider)(nil)

// DataMcp selects the private MongoDB companion and validates its database scope.
func (*Provider) DataMcp(c *domain.Cluster) (engine.DataMcpBackend, error) {
	backend := engine.DataMcpBackend{Kind: engine.DataMcpCompanion, Upstream: "mongodb-mcp-server"}
	if c == nil {
		return backend, invalid("cluster is nil")
	}
	if c.Spec.DataMcp == nil {
		return backend, nil
	}
	scope := c.Spec.DataMcp.Scope
	if len(scope.Schemas)+len(scope.IndexPatterns)+len(scope.Vhosts) != 0 {
		return backend, invalid("data MCP MongoDB scope supports only databases")
	}
	_, err := users.DataAccessRoles(engine.DataAccessRead, scope.Databases)
	return backend, err
}

// DataAccessCredentials returns sealed credentials only after native provisioning.
// The password is part of the companion credential fingerprint: rotation replaces
// this secret without reloading database members, which do not consume it.
func (p *Provider) DataAccessCredentials(ctx context.Context, rc *engine.Context, level engine.DataAccessLevel) (engine.DataAccessCredentials, error) {
	var out engine.DataAccessCredentials
	if !level.Valid() {
		return out, invalid("invalid data access level %q", level)
	}
	if _, err := p.DataMcp(rc.Cluster); err != nil {
		return out, err
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return out, err
	}
	ro, rw := engine.DataAccessLevels(rc.Cluster)
	name := engine.DataAccessUser(level)
	if (level == engine.DataAccessRead && !ro) || (level == engine.DataAccessReadWrite && !rw) || s.DataAccessUsers[name] == nil {
		return out, engine.ErrNotFound
	}
	pw, err := secret(ctx, rc, "users/"+name)
	if err != nil {
		return out, err
	}
	ca, err := secret(ctx, rc, "tls/ca.crt")
	if err != nil {
		return out, err
	}
	conn := connection(rc.Cluster, s)
	uri, err := url.Parse(conn.URI)
	if err != nil {
		return out, err
	}
	q := uri.Query()
	tls := s.TLSMode != "disabled" && s.TLSMode != "allowTLS"
	q.Set("tls", "false")
	if tls {
		q.Set("tls", "true")
	}
	if level == engine.DataAccessRead {
		q.Set("readPreference", "secondaryPreferred")
	}
	uri.RawQuery = q.Encode()
	uri.User = url.UserPassword(name, string(pw))
	return engine.DataAccessCredentials{Username: name, Password: string(pw), Hosts: conn.Hosts, Database: "admin", URI: uri.String(), TLS: tls, CAPEM: ca}, nil
}

func (p *Provider) reconcileDataAccessUsers(ctx context.Context, rc *engine.Context, s *internalState) error {
	if _, err := p.DataMcp(rc.Cluster); err != nil {
		return err
	}
	ro, rw := engine.DataAccessLevels(rc.Cluster)
	// Disabled clusters without retained identities need no extra native connection.
	pending := ro || rw || len(s.DataAccessUsers) != 0
	if !pending {
		// Retain retryability when a failed create persisted a secret but no marker.
		for _, level := range []engine.DataAccessLevel{engine.DataAccessRead, engine.DataAccessReadWrite} {
			_, err := secret(ctx, rc, "users/"+engine.DataAccessUser(level))
			if err == nil {
				pending = true
			} else if !missing(err) {
				return err
			}
		}
		if !pending {
			return nil
		}
	}
	client, err := p.userClient(ctx, rc) // mongos for sharded clusters, never a shard.
	if err != nil {
		return err
	}
	defer client.Disconnect(ctx)
	var databases []string
	if rc.Cluster.Spec.DataMcp != nil {
		databases = rc.Cluster.Spec.DataMcp.Scope.Databases
	}
	for _, level := range []engine.DataAccessLevel{engine.DataAccessRead, engine.DataAccessReadWrite} {
		name := engine.DataAccessUser(level)
		key := "users/" + name
		enabled := ro
		if level == engine.DataAccessReadWrite {
			enabled = rw
		}
		if !enabled {
			if err := users.DropDataAccessUser(ctx, client, level); err != nil {
				return err
			}
			if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); err != nil && !missing(err) {
				return err
			}
			delete(s.DataAccessUsers, name)
			delete(s.LastRotatedAt, name)
			continue
		}
		if err := ensureSecret(ctx, rc, key, secrets.Password); err != nil {
			return err
		}
		password, err := secret(ctx, rc, key)
		if err != nil {
			return err
		}
		if err := users.EnsureDataAccessUser(ctx, client, level, databases, string(password)); err != nil {
			return err
		}
		roles, _ := users.DataAccessRoles(level, databases)
		if s.DataAccessUsers == nil {
			s.DataAccessUsers = map[string][]domain.RoleRef{}
		}
		s.DataAccessUsers[name] = roles
	}
	return nil
}

func isDataAccessUser(name string) bool {
	return name == engine.DataAccessReadUser || name == engine.DataAccessReadWriteUser
}
