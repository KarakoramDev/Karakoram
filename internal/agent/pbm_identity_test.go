// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"net/url"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestPBMURIUsesRestoreScopedCredentialUsername(t *testing.T) {
	cfg := agentapi.MemberConfig{SystemUsers: map[string]agentapi.Credentials{"backup": {Username: "restore_scoped", Password: "target-only-password", Database: "admin"}}}
	uri, err := backupURI(cfg, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(uri)
	if err != nil {
		t.Fatal(err)
	}
	if u.User.Username() != "restore_scoped" || u.Query().Get("authSource") != "admin" {
		t.Fatal("PBM ignored restore-scoped identity")
	}
	cfg.SystemUsers["backup"] = agentapi.Credentials{Password: "target-only-password"}
	uri, err = backupURI(cfg, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	u, err = url.Parse(uri)
	if err != nil || u.User.Username() != "backup" {
		t.Fatal("legacy backup identity changed")
	}
}
