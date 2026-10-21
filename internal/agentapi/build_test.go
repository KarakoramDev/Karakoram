// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agentapi

import (
	"encoding/json"
	"testing"
)

func TestAgentStatusBuildWireCompatibility(t *testing.T) {
	var legacy Status
	if err := json.Unmarshal([]byte(`{"engineVersion":"8.0"}`), &legacy); err != nil || legacy.Agent != (AgentBuild{}) {
		t.Fatal(legacy, err)
	}
	original := Status{Agent: AgentBuild{Version: "v1", Commit: "commit", Build: "binary-identity"}}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Status
	if err = json.Unmarshal(raw, &decoded); err != nil || decoded.Agent != original.Agent {
		t.Fatal(decoded, err)
	}
}
