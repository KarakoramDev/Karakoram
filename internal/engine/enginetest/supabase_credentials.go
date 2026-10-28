// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package enginetest

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbkeys"
)

// SupabaseCredentialContracts checks every persisted field, not a password proxy.
// Formats: https://supabase.com/docs/guides/self-hosting/self-hosted-auth-keys
// Component key sizes: https://supabase.com/docs/guides/self-hosting/docker
// P-256 JWK coordinates/scalars: RFC 7518 section 6.2 (32 octets, base64url).
func SupabaseCredentialContracts(t *testing.T, keys sbkeys.Keys) {
	t.Helper()
	type encoded struct {
		length int
		hex    bool
	}
	// These are component output contracts, not invented universal password caps.
	encodings := map[string]encoded{
		"JWTSecret": {44, false}, "SecretKeyBase": {64, false},
		"RealtimeDBEncKey": {16, true}, "VaultEncKey": {32, true},
		"PGMetaCryptoKey": {32, false}, "LogflarePublicToken": {32, false},
		"LogflarePrivateToken": {32, false}, "DashboardPassword": {32, false},
		"StorageS3AccessKeyID": {32, true}, "StorageS3SecretAccessKey": {64, true},
	}
	jwtAlgorithms := map[string]string{"AnonKey": "HS256", "ServiceRoleKey": "HS256", "AnonJWT": "ES256", "ServiceRoleJWT": "ES256"}
	opaquePrefixes := map[string]string{"PublishableKey": "sb_publishable_", "SecretKey": "sb_secret_"}
	value := reflect.ValueOf(keys)
	for i := range value.NumField() {
		name := value.Type().Field(i).Name
		t.Run(name, func(t *testing.T) {
			if spec, ok := encodings[name]; ok {
				text := value.Field(i).String()
				if len(text) != spec.length {
					t.Errorf("length %d, want %d", len(text), spec.length)
				}
				var err error
				if spec.hex {
					_, err = hex.DecodeString(text)
					if text != strings.ToLower(text) {
						t.Error("hex must be lowercase")
					}
				} else {
					_, err = base64.StdEncoding.Strict().DecodeString(text)
				}
				if err != nil {
					t.Error("invalid component key encoding", err)
				}
				return
			}
			if alg, ok := jwtAlgorithms[name]; ok {
				parts := strings.Split(value.Field(i).String(), ".")
				if len(parts) != 3 {
					t.Fatal("JWT must have three compact segments")
				}
				decoded := make([][]byte, 3)
				for j, part := range parts {
					decoded[j] = decodeURL(t, part, 0)
				}
				var header struct {
					Alg string `json:"alg"`
					Typ string `json:"typ"`
				}
				if err := json.Unmarshal(decoded[0], &header); err != nil {
					t.Fatal(err)
				}
				if header.Alg != alg || header.Typ != "JWT" {
					t.Errorf("wrong JWT header: %+v", header)
				}
				size := 32
				if alg == "ES256" {
					size = 64
				}
				if len(decoded[2]) != size {
					t.Errorf("signature length %d, want %d", len(decoded[2]), size)
				}
				var claims struct {
					Role string `json:"role"`
				}
				if err := json.Unmarshal(decoded[1], &claims); err != nil {
					t.Fatal(err)
				}
				role := "anon"
				if strings.HasPrefix(name, "ServiceRole") {
					role = "service_role"
				}
				if claims.Role != role {
					t.Error("wrong JWT role")
				}
				return
			}
			if prefix, ok := opaquePrefixes[name]; ok {
				text := value.Field(i).String()
				if !strings.HasPrefix(text, prefix) || len(text) != len(prefix)+22+1+8 {
					t.Fatal("wrong opaque API-key prefix/length")
				}
				random, checksum := text[len(prefix):len(prefix)+22], text[len(prefix)+23:]
				CredentialLimit{22, 22, Alphanumeric + "_-", ""}.Assert(t, "random part", random)
				CredentialLimit{8, 8, Alphanumeric + "_-", ""}.Assert(t, "checksum", checksum)
				if text[len(prefix)+22] != '_' {
					t.Error("missing checksum separator")
				}
				sum := sha256.Sum256([]byte("supabase-self-hosted|" + prefix + random))
				if checksum != base64.RawURLEncoding.EncodeToString(sum[:])[:8] {
					t.Error("incorrect API-key checksum")
				}
				return
			}
			if name == "JWTKeys" || name == "JWKS" {
				raw := value.Field(i).Interface().(json.RawMessage)
				var jwks []map[string]json.RawMessage
				if name == "JWTKeys" {
					if err := json.Unmarshal(raw, &jwks); err != nil {
						t.Fatal(err)
					}
				} else {
					var doc struct {
						Keys []map[string]json.RawMessage `json:"keys"`
					}
					if err := json.Unmarshal(raw, &doc); err != nil {
						t.Fatal(err)
					}
					jwks = doc.Keys
				}
				if len(jwks) != 2 {
					t.Fatal("expected EC and legacy symmetric JWKs")
				}
				for j, jwk := range jwks {
					text := func(field string) string {
						var out string
						if err := json.Unmarshal(jwk[field], &out); err != nil {
							t.Fatalf("JWK %d missing/invalid %s", j, field)
						}
						return out
					}
					if j == 0 {
						if text("kty") != "EC" || text("crv") != "P-256" || text("alg") != "ES256" {
							t.Fatal("wrong EC key contract")
						}
						for _, field := range []string{"x", "y"} {
							decodeURL(t, text(field), 32)
						}
						if name == "JWTKeys" {
							decodeURL(t, text("d"), 32)
						} else if _, ok := jwk["d"]; ok {
							t.Error("JWKS contains private scalar")
						}
					} else {
						if text("kty") != "oct" || text("alg") != "HS256" {
							t.Fatal("wrong legacy key contract")
						}
						if string(decodeURL(t, text("k"), len(keys.JWTSecret))) != keys.JWTSecret {
							t.Error("legacy JWK secret mismatch")
						}
					}
				}
				return
			}
			t.Errorf("generated Supabase credential %q has no declared contract", name)
		})
	}
}

func decodeURL(t *testing.T, text string, bytes int) []byte {
	t.Helper()
	if text == "" || strings.Trim(text, Alphanumeric+"_-") != "" {
		t.Fatal("expected nonempty unpadded base64url")
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(text)
	if err != nil {
		t.Fatal("invalid base64url", err)
	}
	if bytes > 0 && (len(raw) != bytes || len(text) != base64.RawURLEncoding.EncodedLen(bytes)) {
		t.Errorf("base64url length %d (%d bytes), want %d bytes", len(text), len(raw), bytes)
	}
	return raw
}
