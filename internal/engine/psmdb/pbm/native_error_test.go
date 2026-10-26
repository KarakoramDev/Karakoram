// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/redact"
)

const sensitiveNativeError = `apply oplog admin.system.users: {"credentials":{"SCRAM-SHA-1":{"salt":"fixture-salt","storedKey":"fixture-stored","serverKey":"fixture-server"},"SCRAM-SHA-256":{"salt":"fixture-salt2"}},"roles":[]} password:"prefix\"fixture-password" secret-access-key:"fixture-s3" mongodb+srv://fixture-user:fixture-uri@host/db pwd:'prefix''fixture-yaml' {"pass\u0077ord":"fixture-unicode"} | applyOps: (AuthenticationFailed) Authentication failed`

func assertNativeSecretsAbsent(t *testing.T, message string) {
	t.Helper()
	for _, secret := range []string{"fixture-salt", "fixture-stored", "fixture-server", "fixture-password", "fixture-s3", "fixture-uri", "fixture-user", "fixture-yaml", "fixture-unicode"} {
		if strings.Contains(message, secret) {
			t.Fatalf("native credential material escaped redaction: %s", secret)
		}
	}
}

func TestSafeNativeErrorRedactsCredentialDocumentsAndVerifierFields(t *testing.T) {
	for _, input := range []string{
		sensitiveNativeError,
		strings.ReplaceAll(sensitiveNativeError, `"`, `\"`),
		`apply oplog: credentials:{SCRAM-SHA-256:{salt:fixture-salt,storedKey:fixture-stored,serverKey:fixture-server}} | (AuthenticationFailed)`,
		`apply oplog: {salt:"fixture-salt",storedKey:"fixture-stored",serverKey:"fixture-server"} | (AuthenticationFailed)`,
		`apply oplog: {salt:BinData(0, "fixture-salt"),storedKey:Binary{Data:"fixture-stored"},serverKey:"fixture-server"} | (AuthenticationFailed)`,
		`apply oplog: credentials:{"salt":"fixture-salt","storedKey":"fixture-stored"`,
	} {
		got := redact.NativeError(input)
		assertNativeSecretsAbsent(t, got)
		if !strings.Contains(got, "apply oplog") || !strings.Contains(got, "[redacted]") {
			t.Fatal("lost safe diagnostic context", got)
		}
	}
	const auth = `unable to authenticate using mechanism "SCRAM-SHA-256": (AuthenticationFailed) Authentication failed`
	if redact.NativeError(auth) != auth {
		t.Fatal("SASL mechanism was mistaken for credential data", redact.NativeError(auth))
	}
}

func TestRestoreNativeErrorIsRedactedBeforePersistence(t *testing.T) {
	env, agent, _, op := restoreStartupSetup(t, false)
	nested := sensitiveNativeError
	for range 4 {
		data, _ := json.Marshal(map[string]string{"error": nested})
		nested = string(data)
	}
	agent.restoreDescribeOutput, _ = json.Marshal(map[string]string{"status": "error", "error": nested})
	res := drive(t, env, op, RunRestore, true)
	assertNativeSecretsAbsent(t, string(res.Checkpoint))
	r, err := env.RC.Store.GetRestore(t.Context(), env.RC.Cluster.Ref(), "clone")
	if err != nil {
		t.Fatal(err)
	}
	assertNativeSecretsAbsent(t, r.Error)
	if !strings.Contains(r.Error, "AuthenticationFailed") || !strings.Contains(r.Error, "admin.system.users") {
		t.Fatal("native error class or namespace was lost", r.Error)
	}
}

func TestPBMCommandFailureIsRedactedBeforeReturning(t *testing.T) {
	env, agent, _, _ := restoreStartupSetup(t, false)
	nested := sensitiveNativeError
	for range 4 {
		data, _ := json.Marshal(map[string]string{"error": nested})
		nested = string(data)
	}
	agent.restoreDescribeError = nested
	_, err := command(t.Context(), env, "m0", "", "describe-restore", nil, "native")
	if err == nil {
		t.Fatal("missing native failure")
	}
	assertNativeSecretsAbsent(t, err.Error())
	if !restoreAuthenticationFailure(err) {
		t.Fatal("redaction removed positive server rejection evidence", err)
	}
}
