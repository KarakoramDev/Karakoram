package engine

import (
	"context"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// DataAccessLevel selects a least-privilege database identity managed by
// Karakoram for tools that read (or explicitly write) application data.
type DataAccessLevel string

const (
	// DataAccessRead is a read-only identity enforced by the database itself.
	DataAccessRead DataAccessLevel = "read"
	// DataAccessReadWrite may change data but never administers users,
	// roles or cluster configuration.
	DataAccessReadWrite DataAccessLevel = "readWrite"
)

// Valid reports whether l is a known level.
func (l DataAccessLevel) Valid() bool {
	return l == DataAccessRead || l == DataAccessReadWrite
}

// Data-access system user names shared by every engine.
const (
	DataAccessReadUser      = "karakoram-mcp-ro"
	DataAccessReadWriteUser = "karakoram-mcp-rw"
)

// DataAccessUser returns the logical system user name for l (the name used
// by system-user listing and rotation). Engines whose identifier rules
// differ map it to their own username, reported in
// DataAccessCredentials.Username and domain.SystemUser.Username.
func DataAccessUser(l DataAccessLevel) string {
	if l == DataAccessReadWrite {
		return DataAccessReadWriteUser
	}
	return DataAccessReadUser
}

// DataAccessLevels reports which data-access identities c needs. Providers
// provision exactly these in Sync and remove the others (dropping the
// database user and its secret). Features other than the data MCP endpoint
// that need the identities extend this single condition.
func DataAccessLevels(c *domain.Cluster) (read, readWrite bool) {
	if c == nil {
		return false, false
	}
	s := c.Spec.DataMcp
	return s.DataMcpEnabled(), s.AllowsWrite()
}

// DataAccessCredentials describe how a control-plane component reaches a
// cluster's data endpoint with a Karakoram-managed data-access identity.
// Hosts are logical host:port addresses dialled through Context.Dialer
// (nil means the namespace network is directly reachable). Values are
// secret and must never be returned through an API or logged.
type DataAccessCredentials struct {
	Username string
	Password string
	Hosts    []string
	// Database is the default database for engines that need one.
	Database string
	// URI is an engine-native connection string including credentials
	// (mongodb://, postgresql://, https:// for Elasticsearch and the
	// RabbitMQ management API).
	URI   string
	TLS   bool
	CAPEM []byte
}

// DataAccessProvider is implemented by providers that provision the
// read-only and opt-in read-write data-access system users. The read
// identity always exists while data access is enabled; the read-write
// identity exists only when the cluster opted into writes.
type DataAccessProvider interface {
	DataAccessCredentials(ctx context.Context, rc *Context, level DataAccessLevel) (DataAccessCredentials, error)
}

// DataMcpKind selects how a cluster's data MCP endpoint is served.
type DataMcpKind string

const (
	// DataMcpCompanion runs a pinned upstream MCP server in a private
	// per-cluster companion service reached through its agent.
	DataMcpCompanion DataMcpKind = "companion"
	// DataMcpNative serves Karakoram's in-process Go tools.
	DataMcpNative DataMcpKind = "native"
)

// DataMcpBackend declares the data MCP implementation of an engine.
// Upstream names the tool manifest (and, for companions, the native
// artifact) used by the gateway.
type DataMcpBackend struct {
	Kind     DataMcpKind
	Upstream string
}

// DataMcpProvider is implemented by providers that support spec.dataMcp.
// DataMcp validates the engine-specific parts of c.Spec.DataMcp (scope
// entries) and returns the backend; a returned error is an admission error.
type DataMcpProvider interface {
	DataAccessProvider
	DataMcp(c *domain.Cluster) (DataMcpBackend, error)
}
