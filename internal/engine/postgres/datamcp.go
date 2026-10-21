package postgres

import (
	"context"
	"net/url"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

var _ engine.DataMcpProvider = (*Core)(nil)

func (*Core) DataMcp(c *domain.Cluster) (engine.DataMcpBackend, error) {
	if c != nil && c.Spec.DataMcp != nil {
		scope := c.Spec.DataMcp.Scope
		if len(scope.IndexPatterns) != 0 || len(scope.Vhosts) != 0 {
			return engine.DataMcpBackend{}, engine.Invalid("PostgreSQL data MCP supports only databases and schemas")
		}
		for _, names := range [][]string{scope.Databases, scope.Schemas} {
			for _, name := range names {
				if name == "" || len(name) > 63 || strings.ContainsRune(name, 0) {
					return engine.DataMcpBackend{}, engine.Invalid("invalid PostgreSQL data MCP scope identifier")
				}
			}
		}
		for _, name := range scope.Schemas {
			if name == "information_schema" || strings.HasPrefix(name, "pg_") {
				return engine.DataMcpBackend{}, engine.Invalid("system schemas cannot be scoped for data MCP")
			}
		}
	}
	return engine.DataMcpBackend{Kind: engine.DataMcpCompanion, Upstream: "pgedge-postgres-mcp"}, nil
}

func dataRoleName(level engine.DataAccessLevel) string {
	if level == engine.DataAccessReadWrite {
		return "kk_mcp_rw"
	}
	return "kk_mcp_ro"
}

func (p *Core) dataRoles(ctx context.Context, rc *engine.Context) (*pgspec.DataAccessRoles, error) {
	if _, err := p.DataMcp(rc.Cluster); err != nil {
		return nil, err
	}
	ro, rw := engine.DataAccessLevels(rc.Cluster)
	out := &pgspec.DataAccessRoles{Databases: []string{"postgres"}, GlobalRead: true}
	revision, err := p.accessRevision(ctx, rc)
	if err != nil {
		return nil, err
	}
	out.Revision = revision
	if spec := rc.Cluster.Spec.DataMcp; spec != nil {
		if len(spec.Scope.Databases) > 0 {
			out.Databases = append([]string(nil), spec.Scope.Databases...)
			out.GlobalRead = false
		}
		out.Schemas = append([]string(nil), spec.Scope.Schemas...)
		out.GlobalRead = out.GlobalRead && len(out.Schemas) == 0
	}
	for _, level := range []engine.DataAccessLevel{engine.DataAccessRead, engine.DataAccessReadWrite} {
		enabled := ro
		if level == engine.DataAccessReadWrite {
			enabled = rw
		}
		r := pgspec.DataAccessRole{Credentials: pgspec.Credentials{Username: dataRoleName(level)}, Enabled: enabled, Write: level == engine.DataAccessReadWrite, Settings: map[string]string{"statement_timeout": "30s", "idle_in_transaction_session_timeout": "60s"}}
		if !r.Write {
			r.Settings["default_transaction_read_only"] = "on"
		}
		if enabled {
			b, err := secret(ctx, rc, "users/"+engine.DataAccessUser(level))
			if err != nil {
				return nil, err
			}
			r.Password = string(b)
		}
		out.Roles = append(out.Roles, r)
	}
	return out, nil
}

func ensureDataSecrets(ctx context.Context, rc *engine.Context) error {
	ro, rw := engine.DataAccessLevels(rc.Cluster)
	for _, level := range []engine.DataAccessLevel{engine.DataAccessRead, engine.DataAccessReadWrite} {
		enabled := ro
		if level == engine.DataAccessReadWrite {
			enabled = rw
		}
		key := "users/" + engine.DataAccessUser(level)
		if !enabled {
			if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); err != nil && !missing(err) {
				return err
			}
			continue
		}
		if _, err := secret(ctx, rc, key); missing(err) {
			password, e := secrets.Password()
			if e != nil {
				return e
			}
			if e = putSecret(ctx, rc, key, []byte(password)); e != nil {
				return e
			}
		} else if err != nil {
			return err
		}
	}
	return nil
}

// DataAccessCredentials uses project-network DNS, never public endpoint routing.
// Direct multi-host URIs prefer a standby for reads and require a writer for RW.
// PUBLIC CONNECT/object grants remain inherited PostgreSQL privileges: database
// scoping is not an isolation boundary for objects already readable by PUBLIC.
func (p *Core) DataAccessCredentials(ctx context.Context, rc *engine.Context, level engine.DataAccessLevel) (engine.DataAccessCredentials, error) {
	ro, rw := engine.DataAccessLevels(rc.Cluster)
	if !level.Valid() || !ro || level == engine.DataAccessReadWrite && !rw {
		return engine.DataAccessCredentials{}, engine.ErrNotSupported
	}
	roles, err := p.dataRoles(ctx, rc)
	if err != nil {
		return engine.DataAccessCredentials{}, err
	}
	r := roles.Roles[0]
	if level == engine.DataAccessReadWrite {
		r = roles.Roles[1]
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return engine.DataAccessCredentials{}, err
	}
	out := engine.DataAccessCredentials{Username: r.Username, Password: r.Password, Database: roles.Databases[0], TLS: rc.Cluster.Spec.TLS.Mode != "disabled"}
	proxy := false
	for _, m := range s.Members {
		if m.Role == "proxy" && !m.Removing {
			proxy = true
			break
		}
	}
	if proxy {
		port := "5432"
		if level == engine.DataAccessRead {
			port = "5433"
		}
		out.Hosts = []string{rc.Cluster.Name + "px:" + port}
	} else {
		for _, m := range s.data() {
			if !m.Removing {
				out.Hosts = append(out.Hosts, m.Name+":5432")
			}
		}
	}
	if len(out.Hosts) == 0 {
		return engine.DataAccessCredentials{}, engine.ErrUnavailable
	}
	mode := "disable"
	if out.TLS {
		mode = "verify-full"
		out.CAPEM, err = secret(ctx, rc, "tls/ca.crt")
		if err != nil {
			return engine.DataAccessCredentials{}, err
		}
	}
	u := url.URL{Scheme: "postgresql", User: url.UserPassword(out.Username, out.Password), Host: strings.Join(out.Hosts, ","), Path: "/" + out.Database, RawPath: "/" + url.PathEscape(out.Database)}
	q := url.Values{"sslmode": {mode}}
	if !proxy {
		attrs := "prefer-standby"
		if level == engine.DataAccessReadWrite {
			attrs = "read-write"
		}
		q.Set("target_session_attrs", attrs)
	}
	u.RawQuery = q.Encode()
	out.URI = u.String()
	return out, nil
}
