package datamcp

// ToolClass classifies a tool for filtering, confirmation and audit.
type ToolClass string

const (
	// ClassRead tools never change data or server state.
	ClassRead ToolClass = "read"
	// ClassWrite tools change data or state (including consuming reads
	// such as a RabbitMQ message peek); they need effective write access.
	ClassWrite ToolClass = "write"
	// ClassDestructive tools need effective write access and a
	// gateway-run confirmation that fails closed.
	ClassDestructive ToolClass = "destructive"
	// ClassDeny tools are never listed or callable.
	ClassDeny ToolClass = "deny"
)

// ArgRule constrains one top-level tool argument before forwarding.
type ArgRule struct {
	Name string `json:"name"`
	// Max clamps a numeric argument; Default fills an absent numeric
	// argument (applied before Max).
	Max     *float64 `json:"max,omitempty"`
	Default *float64 `json:"default,omitempty"`
	// DefaultString fills an absent string argument before Allow/Deny checks.
	DefaultString *string `json:"defaultString,omitempty"`
	// Allow restricts a string argument to the listed values; Deny refuses
	// the listed values. Both are exact, case-sensitive matches.
	Allow []string `json:"allow,omitempty"`
	Deny  []string `json:"deny,omitempty"`
	// Forbidden refuses the call when the argument is present.
	Forbidden bool `json:"forbidden,omitempty"`
}

// ToolRule is the manifest entry of one upstream tool.
type ToolRule struct {
	Class ToolClass `json:"class"`
	Args  []ArgRule `json:"args,omitempty"`
	// WriteAlias exposes a read tool on the read/write identity as destructive.
	WriteAlias string `json:"writeAlias,omitempty"`
}

// Manifest pins the reviewed tool set of one upstream version. Tools absent
// from the manifest are denied.
type Manifest struct {
	Upstream string `json:"upstream"`
	Version  string `json:"version"`
	// WrapsUntrusted is true when the upstream already wraps returned data
	// in its own untrusted-data boundary.
	WrapsUntrusted bool                `json:"wrapsUntrusted,omitempty"`
	Tools          map[string]ToolRule `json:"tools"`
}
