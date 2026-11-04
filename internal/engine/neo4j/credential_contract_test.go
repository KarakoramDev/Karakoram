// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
)

// Default dbms.security.auth_minimum_password_length = 8; no native maximum.
// https://neo4j.com/docs/operations-manual/current/configuration/configuration-settings/
var generatedPasswordContract = enginetest.CredentialLimit{Min: 8, Alphabet: enginetest.Alphanumeric}

func TestProviderGeneratedCredentialContracts(t *testing.T) {
	_, rc, _, _ := setup(t)
	recorded := enginetest.RecordCredentials(rc)
	s := &state{Member: &member{Name: "demom0"}}
	if err := ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	// Bootstrap and access/rotation paths share the same native password limit.
	recorded.Assert(t, map[string]enginetest.CredentialLimit{
		"users/neo4j": generatedPasswordContract,
	}, 0)
}

func TestGeneratedApplicationCredentialContracts(t *testing.T) {
	p, rc, _, native := setup(t)
	converge(t, p, rc)
	for _, operation := range []string{"create", "password-reset"} {
		t.Run(operation, func(t *testing.T) {
			var password string
			var err error
			if operation == "create" {
				password, err = p.CreateUser(t.Context(), rc, "alice", "")
			} else {
				password, err = p.SetUserPassword(t.Context(), rc, "alice", "")
			}
			if err != nil {
				t.Fatal(err)
			}
			generatedPasswordContract.Assert(t, "returned "+operation, password)
			generatedPasswordContract.Assert(t, "native "+operation, native.userPasswords["alice"])
			if password != native.userPasswords["alice"] {
				t.Fatal("native request and returned password differ")
			}
		})
	}
}

func TestGeneratedRotationCredentialContracts(t *testing.T) {
	p, rc, _, native := setup(t)
	converge(t, p, rc)
	recorded := enginetest.RecordCredentials(rc)
	runOp(t, p, rc, &domain.Operation{ID: "contract", Kind: domain.OperationRotateSystemUser, Payload: json.RawMessage(`{"user":"neo4j"}`)})
	recorded.AssertValues(t, map[string]enginetest.CredentialLimit{
		"operations/contract/password": generatedPasswordContract,
		"users/neo4j":                  generatedPasswordContract,
	}, 0)
	generatedPasswordContract.Assert(t, "native rotated administrator", native.password)
	if native.password != string(recorded.Values["users/neo4j"]) {
		t.Fatal("native and persisted rotation credentials differ")
	}
}
