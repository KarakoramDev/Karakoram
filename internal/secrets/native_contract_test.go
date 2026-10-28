// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package secrets_test

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbkeys"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

// This matrix tests the shared production generators used by the named engine
// callsites, not mocks or hand-written example credentials. MySQL's ensureSecrets
// tests additionally cover selection/persistence of its narrower GR generator.
// Zero max means the cited native contract specifies no maximum, NOT unlimited
// acceptance of every possible string. User-configured password policies are not
// the shipped defaults and cannot be guaranteed by a random generator.
func TestGeneratedCredentialsRespectNativeContracts(t *testing.T) {
	const alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const b64 = alpha + "+/="
	tests := []struct {
		name     string
		generate func() (string, error)
		min, max int
		alphabet string
		prefix   string
	}{
		// MySQL/PG product validators: internal/engine/{mysql,postgres}/access.go.
		// MySQL caching_sha2 accepts ordinary UTF-8 passwords; alphanumeric is a
		// safe subset. https://dev.mysql.com/doc/refman/8.4/en/caching-sha2-pluggable-authentication.html
		{"mysql users and pxc users", secrets.Password, 12, 1024, alpha, ""},
		// The distinct START limit must not be inferred from SQL account limits:
		// https://dev.mysql.com/doc/mysql-errors/8.4/en/server-error-reference.html
		// ER_GROUP_REPLICATION_PASSWORD_LENGTH (3972), also present in 8.0.
		{"mysql group recovery", func() (string, error) { return secrets.RandomString(32) }, 12, 32, alpha, ""},
		// https://www.postgresql.org/docs/current/sql-createrole.html (PASSWORD).
		// Includes Citus/Timescale and the PostgreSQL credentials supplied through
		// bindings to Keycloak, OpenBao, Temporal and Supabase (not new passwords
		// generated independently by those consumers).
		{"postgres and bound postgres consumers", secrets.Password, 12, 1024, alpha, ""},
		// SCRAM SASLprep; product minimum is 8, see psmdb/users/mapping.go.
		// https://www.mongodb.com/docs/manual/core/security-scram/
		{"mongodb psmdb users", secrets.Password, 8, 0, alpha, ""},
		// Native membership key: 6..1024 base64 characters, independent of users.
		// https://www.mongodb.com/docs/manual/core/security-internal-authentication/
		{"mongodb psmdb membership key", secrets.Keyfile, 6, 1024, b64, ""},
		// Default RabbitMQ validator accepts all values; no native minimum/max
		// is invented here. config.go/dataaccess.go/operations.go use Password.
		// https://www.rabbitmq.com/docs/passwords
		{"rabbitmq users", secrets.Password, 1, 0, alpha, ""},
		// Native minimum 6; product access.go imposes the stronger minimum 8.
		// https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-put-user.html
		{"elasticsearch users", secrets.Password, 8, 0, alpha, ""},
		// Kibana encryption keys require >=32 characters (three separate keys).
		// https://www.elastic.co/docs/reference/kibana/configuration-reference/security-settings
		{"kibana encryption keys", func() (string, error) { return secrets.RandomString(64) }, 32, 0, alpha, ""},
		// Default dbms.security.auth_minimum_password_length is 8.
		// https://neo4j.com/docs/operations-manual/current/configuration/configuration-settings/
		{"neo4j users", secrets.Password, 8, 0, alpha, ""},
		// provider.go:secret uses Token for the bootstrap admin credential;
		// bound DB credentials are PostgreSQL, covered above. Default Keycloak
		// password policies are realm-configurable, not an intrinsic fixed max.
		// https://www.keycloak.org/docs/latest/server_admin/#password-policies
		{"keycloak bootstrap admin", secrets.Token, 1, 0, alpha + "_", "kk_"},
		// Configured API keys and cluster basic-auth secrets are opaque strings;
		// secrets.go uses Password. Do not assume UUID/base64 encoding is needed.
		// https://docs.weaviate.io/deploy/configuration/authentication
		{"weaviate api and cluster keys", secrets.Password, 1, 0, alpha, ""},
		// Supabase SQL roles use the same PostgreSQL generator, access.go.
		{"supabase postgres roles", secrets.Password, 12, 1024, alpha, ""},
		// OpenBao's root/unseal credentials are returned by native initialization,
		// NOT generated with secrets.Token. Temporal also creates no native user
		// password: its persistence credentials come from PostgreSQL bindings.
		// These cases cover only their Karakoram agent bearer credentials, as
		// for every other engine (engine/*/secrets.go or components.go).
		{"openbao agent bearer", secrets.Token, 51, 51, alpha + "_", "kk_"},
		{"temporal agent bearer", secrets.Token, 51, 51, alpha + "_", "kk_"},
		{"all engines agent bearer", secrets.Token, 51, 51, alpha + "_", "kk_"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for range 16 {
				value, err := tc.generate()
				if err != nil {
					t.Fatal(err)
				}
				if len(value) < tc.min || tc.max > 0 && len(value) > tc.max || strings.Trim(value, tc.alphabet) != "" || !strings.HasPrefix(value, tc.prefix) {
					t.Fatal("generated credential violates native length/charset contract")
				}
			}
		})
	}
}

// These are real Supabase generated values, not a generic password proxy.
// Contracts: https://supabase.com/docs/guides/self-hosting/docker (secrets),
// and the version-pinned service wiring in engine/supabase/components.go.
func TestSupabaseGeneratedServiceKeyContracts(t *testing.T) {
	k, err := sbkeys.Generate(rand.Reader, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, value string
		length      int
		hex         bool
	}{
		{"jwt secret", k.JWTSecret, 44, false}, // >=32 characters for HS256.
		{"secret key base", k.SecretKeyBase, 64, false},
		{"realtime db encryption", k.RealtimeDBEncKey, 16, true}, // AES-128 key string.
		{"vault encryption", k.VaultEncKey, 32, true},            // 32-byte pgsodium key string.
		{"pgmeta crypto", k.PGMetaCryptoKey, 32, false},
		{"logflare public", k.LogflarePublicToken, 32, false},
		{"logflare private", k.LogflarePrivateToken, 32, false},
		{"dashboard", k.DashboardPassword, 32, false},
		{"s3 access id", k.StorageS3AccessKeyID, 32, true},
		{"s3 access secret", k.StorageS3SecretAccessKey, 64, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.value) != tc.length {
				t.Fatal("generated service key length changed")
			}
			if tc.hex {
				_, err = hex.DecodeString(tc.value)
			} else {
				_, err = base64.StdEncoding.DecodeString(tc.value)
			}
			if err != nil {
				t.Fatal("generated service key charset changed")
			}
		})
	}
}
