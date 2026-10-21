// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"slices"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

const (
	DataAccessReadRole        = "karakoram_mcp_ro"
	DataAccessReadWriteRole   = "karakoram_mcp_rw"
	secretDataAccessRead      = "users/" + engine.DataAccessReadUser
	secretDataAccessReadWrite = "users/" + engine.DataAccessReadWriteUser
)

var _ engine.DataMcpProvider = (*Provider)(nil)

// DataMcp selects the native gateway and validates Elasticsearch-only scope.
func (*Provider) DataMcp(c *domain.Cluster) (engine.DataMcpBackend, error) {
	backend := engine.DataMcpBackend{Kind: engine.DataMcpNative, Upstream: "native-elasticsearch"}
	if c == nil || c.Spec.DataMcp == nil {
		return backend, nil
	}
	scope := c.Spec.DataMcp.Scope
	if len(scope.Databases) != 0 || len(scope.Schemas) != 0 || len(scope.Vhosts) != 0 {
		return backend, invalid("dataMcp.scope: Elasticsearch accepts only indexPatterns")
	}
	for _, pattern := range scope.IndexPatterns {
		if !engine.ValidElasticsearchDataIndexPattern(pattern) {
			return backend, invalid("dataMcp.scope.indexPatterns: invalid Elasticsearch index pattern")
		}
	}
	return backend, nil
}

// DataAccessRoleDefinition grants document access, never index or user management.
// Restricted-index exclusion is defense in depth: Elasticsearch's '*' also
// matches non-restricted dot indices. The native gateway must exclude every dot
// index independently, including when resolving wildcards and aliases.
func DataAccessRoleDefinition(patterns []string, level engine.DataAccessLevel) Role {
	names := slices.Clone(patterns)
	if len(names) == 0 {
		names = []string{"*"}
	}
	privileges := []string{"read", "view_index_metadata", "monitor"}
	if level == engine.DataAccessReadWrite {
		privileges = append(privileges, "write")
	}
	return Role{Cluster: []string{"monitor"}, Indices: []IndexPrivileges{{Names: names, Privileges: privileges, AllowRestrictedIndices: false}}}
}

func (p *Provider) ensureDataAccess(ctx context.Context, rc *engine.Context, client *esapi.Client) error {
	if _, err := p.DataMcp(rc.Cluster); err != nil {
		return err
	}
	read, write := engine.DataAccessLevels(rc.Cluster)
	for _, entry := range []struct {
		level     engine.DataAccessLevel
		role, key string
		wanted    bool
	}{
		{engine.DataAccessRead, DataAccessReadRole, secretDataAccessRead, read},
		{engine.DataAccessReadWrite, DataAccessReadWriteRole, secretDataAccessReadWrite, write},
	} {
		user := engine.DataAccessUser(entry.level)
		if !entry.wanted {
			if err := client.DeleteUser(ctx, user); err != nil && !esapi.IsNotFound(err) {
				return err
			}
			if err := client.DeleteRole(ctx, entry.role); err != nil && !esapi.IsNotFound(err) {
				return err
			}
			if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), entry.key); err != nil && !missing(err) {
				return err
			}
			continue
		}
		if err := ensureSecret(ctx, rc, entry.key, secrets.Password); err != nil {
			return err
		}
		password, err := secret(ctx, rc, entry.key)
		if err != nil {
			return err
		}
		if err := client.PutRole(ctx, entry.role, roleToES(DataAccessRoleDefinition(rc.Cluster.Spec.DataMcp.Scope.IndexPatterns, entry.level))); err != nil {
			return err
		}
		enabled := true
		if err := client.PutUser(ctx, user, esapi.User{Password: string(password), Roles: []string{entry.role}, Enabled: &enabled}); err != nil {
			return err
		}
	}
	return nil
}

// DataAccessCredentials returns private HTTPS endpoints and a scoped native identity.
func (p *Provider) DataAccessCredentials(ctx context.Context, rc *engine.Context, level engine.DataAccessLevel) (engine.DataAccessCredentials, error) {
	var credentials engine.DataAccessCredentials
	if !level.Valid() {
		return credentials, invalid("invalid data access level")
	}
	read, write := engine.DataAccessLevels(rc.Cluster)
	if !read || level == engine.DataAccessReadWrite && !write {
		return credentials, invalid("data access level is not enabled")
	}
	if _, err := p.DataMcp(rc.Cluster); err != nil {
		return credentials, err
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return credentials, err
	}
	for _, m := range s.members(kindNode) {
		if !m.Removing && (m.Stage == stageJoined || m.Stage == stageDeployed) {
			credentials.Hosts = append(credentials.Hosts, "https://"+m.Name+":9200")
		}
	}
	if len(credentials.Hosts) == 0 {
		return engine.DataAccessCredentials{}, errWaiting
	}
	credentials.Username = engine.DataAccessUser(level)
	password, err := secret(ctx, rc, "users/"+credentials.Username)
	if err != nil {
		return engine.DataAccessCredentials{}, err
	}
	credentials.Password = string(password)
	credentials.CAPEM, err = secret(ctx, rc, secretCA)
	if err != nil {
		return engine.DataAccessCredentials{}, err
	}
	credentials.TLS = true
	return credentials, nil
}
