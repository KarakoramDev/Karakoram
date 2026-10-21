package domain

// Data MCP access modes.
const (
	DataMcpAccessRead      = "read"
	DataMcpAccessReadWrite = "readWrite"
)

// DataMcpSpec configures the per-cluster data MCP endpoint. A nil spec or
// Enabled=false keeps the endpoint and its database identities absent.
type DataMcpSpec struct {
	Enabled bool `json:"enabled"`
	// Access is "read" (default) or "readWrite". Writes additionally need a
	// token with cluster-data:write, the matching RBAC grant and the
	// installation-level write switch.
	Access string       `json:"access,omitempty"`
	Scope  DataMcpScope `json:"scope,omitempty"`
	Tools  DataMcpTools `json:"tools,omitempty"`
	// AuditQueryText opts into recording tool argument text in audit
	// details; by default only an argument hash is recorded.
	AuditQueryText bool `json:"auditQueryText,omitempty"`
	// Resources sizes the companion service for engines served by an
	// upstream MCP server; nil selects the provider default.
	Resources *Resources `json:"resources,omitempty"`
}

// DataMcpScope limits the data reachable through the endpoint. Empty lists
// mean every non-system object; each engine accepts only its own fields.
type DataMcpScope struct {
	Databases     []string `json:"databases,omitempty"`
	Schemas       []string `json:"schemas,omitempty"`
	IndexPatterns []string `json:"indexPatterns,omitempty"`
	Vhosts        []string `json:"vhosts,omitempty"`
}

// DataMcpTools adjusts the manifest-derived tool set.
type DataMcpTools struct {
	// Deny hides and refuses the named tools.
	Deny []string `json:"deny,omitempty"`
	// ConfirmWrites requires confirmation for write tools as well as for
	// destructive tools.
	ConfirmWrites bool `json:"confirmWrites,omitempty"`
}

// DataMcpEnabled reports whether s enables the endpoint.
func (s *DataMcpSpec) DataMcpEnabled() bool { return s != nil && s.Enabled }

// AllowsWrite reports whether the cluster opted into write tools.
func (s *DataMcpSpec) AllowsWrite() bool {
	return s.DataMcpEnabled() && s.Access == DataMcpAccessReadWrite
}

// DataMcpStatus reports the observed data MCP endpoint state.
type DataMcpStatus struct {
	// Phase is Disabled, Provisioning, Ready or Degraded.
	Phase string `json:"phase"`
	// Kind is "companion" or "native".
	Kind string `json:"kind,omitempty"`
	// Upstream and UpstreamVersion identify the served tool manifest.
	Upstream        string `json:"upstream,omitempty"`
	UpstreamVersion string `json:"upstreamVersion,omitempty"`
	// Access is the cluster-level access currently provisioned.
	Access  string `json:"access,omitempty"`
	Message string `json:"message,omitempty"`
}

// Data MCP status phases.
const (
	DataMcpPhaseDisabled     = "Disabled"
	DataMcpPhaseProvisioning = "Provisioning"
	DataMcpPhaseReady        = "Ready"
	DataMcpPhaseDegraded     = "Degraded"
)
