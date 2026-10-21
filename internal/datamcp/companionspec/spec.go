// Package companionspec is the control-plane to agent payload of the data
// MCP companion (agentapi.MemberConfig.EngineSpec when Engine is "datamcp").
package companionspec

// Engine is the agent adapter name; its routes live below /v1/datamcp/.
const Engine = "datamcp"

// Upstream names (also manifest and native artifact names).
const (
	UpstreamMongoDB  = "mongodb-mcp-server"
	UpstreamPostgres = "pgedge-postgres-mcp"
)

// Process names supervised by the adapter, one per access level.
const (
	ProcessRead      = "mcp-ro"
	ProcessReadWrite = "mcp-rw"
)

// Route path segments: the gateway's MCP client targets
// https://<member>:9443/v1/datamcp/<RouteRead|RouteReadWrite>.
const (
	RouteRead      = "ro"
	RouteReadWrite = "rw"
)

// Spec is the complete desired configuration of a companion member.
type Spec struct {
	Upstream string `json:"upstream"`
	Version  string `json:"version"`
	// InstallDir is the prepared artifact directory
	// (/opt/karakoram/<artifact>-<version>).
	InstallDir string `json:"installDir"`
	// Processes lists the upstream processes to run; a process absent from
	// the list is stopped.
	Processes []Process `json:"processes"`
}

// Process is one upstream process bound to one data-access identity.
type Process struct {
	// Level is "read" or "readWrite".
	Level string `json:"level"`
	// Port is the loopback port the upstream listens on.
	Port int `json:"port"`
	// Database connection of the data-access identity. Secret.
	Username string   `json:"username"`
	Password string   `json:"password"`
	Hosts    []string `json:"hosts"`
	Database string   `json:"database,omitempty"`
	// URI is the engine-native connection string (MongoDB).
	URI   string `json:"uri,omitempty"`
	TLS   bool   `json:"tls"`
	CAPEM string `json:"caPem,omitempty"`
	// Limits passed to the upstream.
	MaxDocuments int `json:"maxDocuments,omitempty"`
	MaxTimeMS    int `json:"maxTimeMs,omitempty"`
}
