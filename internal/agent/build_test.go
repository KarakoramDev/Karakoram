// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/version"
)

func TestAgentStatusReportsExecutableBuild(t *testing.T) {
	a, _, _ := fakeNode(t, "/bin/true")
	response := requestNode(a, "GET", "/v1/status", "", "token")
	var status agentapi.Status
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if response.Code != 200 || status.Agent.Build == "" || status.Agent.Build != version.BuildIdentity() || status.Agent.Version != version.Version || status.Agent.Commit != version.Commit {
		t.Fatal(response.Code, status.Agent)
	}
}
