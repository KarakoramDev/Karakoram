// Package datamcp implements the per-cluster data MCP gateway served at
// /mcp/clusters/{namespace}/{cluster}. The gateway is an MCP-aware proxy: it
// authenticates every request with Karakoram credentials, filters and
// classifies tools through pinned manifests, runs its own fail-closed
// confirmation, wraps returned data as untrusted, rate-limits and audits
// every tools/call. Tools come from an Upstream: a pinned MCP server in the
// cluster's private companion service, or an in-process native toolset.
package datamcp

import (
	"context"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// Data-access RBAC resource and actions.
const (
	RBACResource = "cluster-data"
	ActionRead   = "read"
	ActionWrite  = "write"
)

// Caller is the authenticated principal of one gateway request.
type Caller struct {
	// Actor is the audit attribution (auth.Principal.Actor()).
	Actor string
	// Subjects are the RBAC subjects of the principal.
	Subjects []string
	// TokenID identifies the API token; empty for non-token principals,
	// which never receive data access.
	TokenID string
	// Scope carries the token restrictions; nil means an unrestricted
	// token, which never receives data access.
	Scope *TokenScope
}

// TokenScope is the subset of restricted-token fields the gateway uses. It
// mirrors domain.TokenRestrictions of the restricted API tokens; the server
// binding converts one into the other.
type TokenScope struct {
	ReadOnly   bool
	Namespaces []string
	// Clusters lists "namespace/name" entries.
	Clusters []string
	// Permissions lists "resource:action" entries. Data access requires an
	// explicit cluster-data:read or cluster-data:write entry.
	Permissions []string
}

// Authenticator resolves the caller of r or returns an error that the
// gateway reports as 401 without detail.
type Authenticator func(r *http.Request) (Caller, error)

// Authorizer answers owner RBAC checks (rbac.Enforcer.Can).
type Authorizer func(subjects []string, resource, action, object string) bool

// WriteSwitch reports whether the installation allows MCP write tools
// (KARAKORAM_MCP=readwrite). It is consulted on every call.
type WriteSwitch func() bool

// Clusters loads a cluster for the gateway.
type Clusters interface {
	GetCluster(ctx context.Context, ref domain.ClusterRef) (*domain.Cluster, error)
}

// Auditor durably records one audit event.
type Auditor func(ctx context.Context, e domain.Event) error

// Upstream is one tool source for one cluster and access level. Native
// toolsets and proxied MCP servers implement it.
type Upstream interface {
	// ListTools returns every tool the upstream offers; the gateway
	// filters them through the manifest.
	ListTools(ctx context.Context) ([]*mcp.Tool, error)
	// CallTool invokes name with already validated arguments.
	CallTool(ctx context.Context, name string, args map[string]any) (*mcp.CallToolResult, error)
	// Close releases sessions and connections.
	Close() error
}

// UpstreamFactory opens an upstream for a cluster at an access level. The
// server composes it from the provider's DataMcpBackend (companion client or
// native toolset); the gateway caches results per cluster, level and
// credential fingerprint and closes them when superseded.
type UpstreamFactory interface {
	// Manifest names the manifest the cluster's upstream is served with.
	Manifest(ctx context.Context, c *domain.Cluster) (string, error)
	// Fingerprint changes whenever an open upstream must be replaced
	// (spec generation, rotated credentials, companion endpoint).
	Fingerprint(ctx context.Context, c *domain.Cluster, level engine.DataAccessLevel) (string, error)
	Open(ctx context.Context, c *domain.Cluster, level engine.DataAccessLevel) (Upstream, error)
}

// NativeInput opens an in-process native toolset (Elasticsearch,
// RabbitMQ). Credentials belong to the data-access identity of Level;
// Dialer reaches the namespace network (nil dials directly).
type NativeInput struct {
	Credentials engine.DataAccessCredentials
	Dialer      engine.ContextDialer
	Scope       domain.DataMcpScope
	Level       engine.DataAccessLevel
}

// ConfirmRequest describes one call awaiting a gateway-run confirmation.
type ConfirmRequest struct {
	Cluster  domain.ClusterRef
	TokenID  string
	Tool     string
	Class    ToolClass
	ArgsHash string
	// Summary is a short, data-free description shown to the user.
	Summary string
}

// Confirmer obtains an explicit user confirmation bound to the request. It
// must fail closed: a client that cannot elicit, a declined or cancelled
// prompt, a timeout or a mismatched binding all return an error, and the
// call is refused.
type Confirmer interface {
	Confirm(ctx context.Context, session *mcp.ServerSession, req ConfirmRequest) error
}
