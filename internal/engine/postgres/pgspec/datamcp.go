package pgspec

// DataAccessRoles is authoritative, including disabled identities. Nil on Spec
// preserves compatibility with member payloads predating data access.
type DataAccessRoles struct {
	// Revision changes when the managed application role catalog changes.
	Revision  string           `json:"revision,omitempty"`
	Roles     []DataAccessRole `json:"roles"`
	Databases []string         `json:"databases"`
	Schemas   []string         `json:"schemas,omitempty"`
	// GlobalRead is permitted only when neither databases nor schemas is scoped.
	GlobalRead bool `json:"globalRead,omitempty"`
}

// DataAccessRole describes an engine-owned LOGIN without ownership or DDL grants.
type DataAccessRole struct {
	Credentials
	Enabled bool `json:"enabled"`
	Write   bool `json:"write,omitempty"`
	// Settings are role-level GUC defaults, not a substitute for SQL privileges.
	Settings map[string]string `json:"settings,omitempty"`
}
