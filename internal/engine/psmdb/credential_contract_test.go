// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package psmdb

import (
	"encoding/base64"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestProviderGeneratedCredentialContracts(t *testing.T) {
	p, rc, _, _ := setup(t)
	recorded := enginetest.RecordCredentials(rc)
	s := &internalState{Members: []*member{{Name: "demom0"}}}
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	// SCRAM/SASLprep compatible; product minimum 8, no documented native maximum.
	// https://www.mongodb.com/docs/manual/core/security-scram/
	pw := enginetest.CredentialLimit{Min: 8, Alphabet: enginetest.Alphanumeric}
	recorded.Assert(t, map[string]enginetest.CredentialLimit{
		"users/clusterAdmin": pw, "users/userAdmin": pw, "users/clusterMonitor": pw,
		"users/backup": pw, "users/databaseAdmin": pw,
		"members/demom0/users/backup": pw, "members/demom0/users/clusterMonitor": pw,
		// MongoDB keyfile: 6..1024 base64 characters.
		// https://www.mongodb.com/docs/manual/core/security-internal-authentication/
		"keyfile": {Min: 6, Max: 1024, Alphabet: enginetest.Alphanumeric + "+/="},
	}, 0)
	if _, err := base64.StdEncoding.DecodeString(string(recorded.Values["keyfile"])); err != nil {
		t.Fatal("keyfile is not base64", err)
	}
}

func TestProviderGeneratedDataCredentialContracts(t *testing.T) {
	p, rc := healingCluster(t, 3)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.TLSMode = "disabled"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	server := nativeHealingFixture(t, rc, s)
	server.command = func(cmd bson.Raw) bson.M {
		if cmd.Lookup("usersInfo").Type != 0 {
			return bson.M{"ok": 1, "users": bson.A{}}
		}
		return bson.M{"ok": 1}
	}
	rc.Cluster.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true, Access: domain.DataMcpAccessReadWrite}
	recorded := enginetest.RecordCredentials(rc)
	if err := p.reconcileDataAccessUsers(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	// Same MongoDB SCRAM contract as the mandatory system users above.
	pw := enginetest.CredentialLimit{Min: 8, Alphabet: enginetest.Alphanumeric}
	recorded.AssertValues(t, map[string]enginetest.CredentialLimit{
		"users/karakoram-mcp-ro": pw, "users/karakoram-mcp-rw": pw,
	}, 0)
}
