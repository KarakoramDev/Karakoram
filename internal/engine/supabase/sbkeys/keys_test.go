// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package sbkeys

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type deterministic struct{ counter byte }

func (r *deterministic) Read(p []byte) (int, error) {
	for i := range p {
		r.counter++
		p[i] = r.counter
	}
	return len(p), nil
}
func TestGenerate(t *testing.T) {
	now := time.Unix(1700000000, 0)
	k, err := Generate(&deterministic{}, now)
	if err != nil {
		t.Fatal(err)
	}
	lengths := map[string]struct {
		s string
		n int
	}{"jwt": {k.JWTSecret, 44}, "secret base": {k.SecretKeyBase, 64}, "realtime": {k.RealtimeDBEncKey, 16}, "vault": {k.VaultEncKey, 32}, "meta": {k.PGMetaCryptoKey, 32}, "public log": {k.LogflarePublicToken, 32}, "private log": {k.LogflarePrivateToken, 32}, "dashboard": {k.DashboardPassword, 32}, "s3 id": {k.StorageS3AccessKeyID, 32}, "s3 secret": {k.StorageS3SecretAccessKey, 64}}
	for name, v := range lengths {
		if len(v.s) != v.n {
			t.Errorf("%s length %d != %d", name, len(v.s), v.n)
		}
	}
	raw, _ := base64.StdEncoding.DecodeString(k.JWTSecret)
	if len(raw) < 32 {
		t.Fatal("JWT entropy")
	}
	for _, s := range []string{k.RealtimeDBEncKey, k.VaultEncKey, k.StorageS3AccessKeyID, k.StorageS3SecretAccessKey} {
		if _, err := hex.DecodeString(s); err != nil {
			t.Fatal(err)
		}
	}
	var jwks struct {
		Keys []map[string]any `json:"keys"`
	}
	if err := json.Unmarshal(k.JWKS, &jwks); err != nil {
		t.Fatal(err)
	}
	if len(jwks.Keys) != 2 {
		t.Fatal("missing keys")
	}
	ec := jwks.Keys[0]
	if _, ok := ec["d"]; ok {
		t.Fatal("private EC scalar leaked")
	}
	x, _ := base64.RawURLEncoding.DecodeString(ec["x"].(string))
	y, _ := base64.RawURLEncoding.DecodeString(ec["y"].(string))
	public := &ecdsa.PublicKey{Curve: elliptic.P256(), X: new(big.Int).SetBytes(x), Y: new(big.Int).SetBytes(y)}
	symmetric, _ := base64.RawURLEncoding.DecodeString(jwks.Keys[1]["k"].(string))
	if string(symmetric) != k.JWTSecret {
		t.Fatal("legacy JWK incorrectly interprets secret")
	}
	cases := []struct {
		token, role, algorithm string
		key                    any
	}{{k.AnonKey, "anon", "HS256", []byte(k.JWTSecret)}, {k.ServiceRoleKey, "service_role", "HS256", []byte(k.JWTSecret)}, {k.AnonJWT, "anon", "ES256", public}, {k.ServiceRoleJWT, "service_role", "ES256", public}}
	for _, tc := range cases {
		parsed, err := jwt.Parse(tc.token, func(token *jwt.Token) (any, error) { return tc.key, nil }, jwt.WithValidMethods([]string{tc.algorithm}), jwt.WithTimeFunc(func() time.Time { return now }), jwt.WithIssuer("supabase"))
		if err != nil {
			t.Fatal(err)
		}
		claims := parsed.Claims.(jwt.MapClaims)
		if len(claims) != 4 || claims["role"] != tc.role || int64(claims["iat"].(float64)) != now.Unix() || int64(claims["exp"].(float64)) != now.Add(lifetime).Unix() {
			t.Fatal("incorrect role claims")
		}
		if parsed.Header["typ"] != "JWT" {
			t.Fatal("incorrect header")
		}
	}
	var private []map[string]any
	if err := json.Unmarshal(k.JWTKeys, &private); err != nil || private[0]["d"] == nil {
		t.Fatal("missing Auth signing scalar")
	}
	for _, key := range []string{k.PublishableKey, k.SecretKey} {
		i := strings.LastIndexByte(key, '_')
		sum := sha256.Sum256([]byte("supabase-self-hosted|" + key[:i]))
		if key[i+1:] != b64(sum[:])[:8] {
			t.Fatal("opaque checksum")
		}
	}
}
func TestGoldenLegacy(t *testing.T) {
	got, err := LegacyJWT("01234567890123456789012345678901", "anon", time.Unix(1700000000, 0))
	if err != nil {
		t.Fatal(err)
	}
	const want = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYW5vbiIsImlzcyI6InN1cGFiYXNlIiwiaWF0IjoxNzAwMDAwMDAwLCJleHAiOjE4NTc2ODAwMDB9.8LEEiPjisPQF2HSVpypt7Ct-vYHMnDLn7IkH07IwmQ4"
	if got != want {
		t.Fatal("upstream JWT golden mismatch")
	}
	verified, err := jwt.Parse(got, func(*jwt.Token) (any, error) { return []byte("01234567890123456789012345678901"), nil }, jwt.WithTimeFunc(func() time.Time { return time.Unix(1700000000, 0) }))
	if err != nil || !verified.Valid {
		t.Fatal(err)
	}
}
func TestGoldenOpaque(t *testing.T) {
	got, err := OpaqueKey(bytes.NewReader(make([]byte, 17)), "sb_publishable_")
	if err != nil {
		t.Fatal(err)
	}
	if got != "sb_publishable_AAAAAAAAAAAAAAAAAAAAAA_qcQJXLgl" {
		t.Fatal("upstream opaque key golden mismatch")
	}
}
func TestErrors(t *testing.T) {
	if _, err := Generate(bytes.NewReader(nil), time.Time{}); err != io.EOF {
		t.Fatalf("entropy error %v", err)
	}
	if _, err := Generate(nil, time.Time{}); err == nil {
		t.Fatal("nil entropy")
	}
	if _, err := OpaqueKey(&deterministic{}, "other"); err == nil {
		t.Fatal("invalid prefix")
	}
	if _, err := LegacyJWT("short", "anon", time.Time{}); err == nil {
		t.Fatal("short secret")
	}
	if _, err := LegacyJWT(strings.Repeat("x", 32), "admin", time.Time{}); err == nil {
		t.Fatal("invalid role")
	}
}
