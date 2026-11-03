// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package sbkeys

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
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
	public := parseJWKPublic(t, x, y)
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

// These vectors were captured from Generate before migrating away from the
// big.Int/ScalarBaseMult implementation. Scalars 1, 2, and 43 have leading
// zeros; scalar 43 also has a leading-zero public y coordinate. The persisted
// format is JSON JWK, not PEM, and kid is a random UUID rather than a thumbprint.
func TestGoldenECMigration(t *testing.T) {
	const kid = "11111111-1111-4111-9111-111111111111"
	const header = "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IjExMTExMTExLTExMTEtNDExMS05MTExLTExMTExMTExMTExMSJ9"
	const anonPayload = "eyJyb2xlIjoiYW5vbiIsImlzcyI6InN1cGFiYXNlIiwiaWF0IjoxNzAwMDAwMDAwLCJleHAiOjE4NTc2ODAwMDB9"
	const servicePayload = "eyJyb2xlIjoic2VydmljZV9yb2xlIiwiaXNzIjoic3VwYWJhc2UiLCJpYXQiOjE3MDAwMDAwMDAsImV4cCI6MTg1NzY4MDAwMH0"
	const publicFormat = `{"alg":"ES256","crv":"P-256","ext":true,"key_ops":["verify"],"kid":"11111111-1111-4111-9111-111111111111","kty":"EC","use":"sig","x":"%s","y":"%s"}`
	const privateFormat = `{"alg":"ES256","crv":"P-256","d":"%s","ext":true,"key_ops":["sign","verify"],"kid":"11111111-1111-4111-9111-111111111111","kty":"EC","use":"sig","x":"%s","y":"%s"}`
	const oct = `{"alg":"HS256","k":"UWtKQ1FrSkNRa0pDUWtKQ1FrSkNRa0pDUWtKQ1FrSkNRa0pDUWtKQ1FrST0","kty":"oct"}`
	vectors := []struct {
		scalar                                   byte
		d, x, y, anonSignature, serviceSignature string
	}{
		{1,
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAE",
			"axfR8uEsQkf4vOblY6RA8ncDfYEt6zOg9KE5RdiYwpY",
			"T-NC4v4af5uO5-tKfA-eFivOM1drMV7Oy7ZAaDe_UfU",
			"sCZpZvopuB-EZ9uQstqSJqaF-xzP0N0zXRIy_Y_XJplAG3ui3Mu7DqrAiEi2-OfXkJkZ0Of3wPYJ5QK5vxNojA",
			"5KLRjUB9vZ1jpVR8q4rxDdWPZofLp5zLJwWOlR8SmXvKQZQI6lNV_lU9E0rUJm7eXDQhpxnINNHM5BPN6uxxlw"},
		{2,
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAI",
			"fPJ7GI0DT36KUjgDBLUaw8CJaeJ38hs1pgtI_EdmmXg",
			"B3dVENuO0EApPZrGn3Qw27p9reY86YIpngS3nSJ4c9E",
			"ufHjUOf7QMtnFiIxqRh3K4m-Tg5-6PGIPfxLNG-_fdi5O8Hb2IEbXYETC7vJNrVAfhneJiTrOEDmFGknNPrfvQ",
			"4fRhdywKz-Q1j27BVv3T12otRdYVwbQL7J6Au4qwWhfZHCK_FwI1qZnvFKvIQSb8bA_V4NOOLHACZmuySqxNtg"},
		{43,
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACs",
			"mGriUG8f8QTQQjCGHY9LSY9LxMbQCbMPdUTcEpuC0o0",
			"ADzMwKZGDgrjKKTZfTx7YdhvxiicGJ8lJREMRBuwfpc",
			"uogbFK0aepVOTESHL9BALYMmS0qartvLbTogH_sAqwXTIy8v7sYzcjFOh4jR-igdmHXt-zhkH7lzXYyLEk_-lw",
			"vpzz1hDH_OT0XgQ2ykjvL2bderwBaMimjAHwnF__BBiLnkulAAGZ9phEcxKeKuYY6MP1JZLbdomOZYUAVlDw8w"},
	}
	now := time.Unix(1700000000, 0)
	for _, v := range vectors {
		t.Run(fmt.Sprint(v.scalar), func(t *testing.T) {
			// 248 bytes for other credentials, 34 for opaque keys, then the
			// fixed-width scalar and UUID; signing entropy remains randomized.
			input := bytes.Repeat([]byte{0x42}, 282)
			d := make([]byte, 32)
			d[31] = v.scalar
			input = append(input, d...)
			input = append(input, bytes.Repeat([]byte{0x11}, 16)...)
			k, err := Generate(io.MultiReader(bytes.NewReader(input), rand.Reader), now)
			if err != nil {
				t.Fatal(err)
			}
			wantJWKS := `{"keys":[` + fmt.Sprintf(publicFormat, v.x, v.y) + "," + oct + "]}"
			wantPrivate := "[" + fmt.Sprintf(privateFormat, v.d, v.x, v.y) + "," + oct + "]"
			if string(k.JWKS) != wantJWKS || string(k.JWTKeys) != wantPrivate {
				t.Fatalf("JWK serialization changed:\nJWKS=%s\nJWTKeys=%s", k.JWKS, k.JWTKeys)
			}

			// Read the captured private JWK exactly as an existing stored key.
			var persisted []struct {
				D string `json:"d"`
				X string `json:"x"`
				Y string `json:"y"`
			}
			if err := json.Unmarshal([]byte(wantPrivate), &persisted); err != nil {
				t.Fatal(err)
			}
			private, err := ecdsa.ParseRawPrivateKey(elliptic.P256(), decodeBase64(t, persisted[0].D))
			if err != nil {
				t.Fatal(err)
			}
			raw, err := private.Bytes()
			if err != nil || !bytes.Equal(raw, d) {
				t.Fatalf("persisted scalar changed: %x, %v", raw, err)
			}
			oldPublic := parseJWKPublic(t, decodeBase64(t, persisted[0].X), decodeBase64(t, persisted[0].Y))
			point, err := private.PublicKey.Bytes()
			if err != nil {
				t.Fatal(err)
			}
			newPublic, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), point)
			if err != nil {
				t.Fatal(err)
			}
			if !oldPublic.Equal(newPublic) {
				t.Fatal("derived public key differs from captured coordinates")
			}
			for _, tc := range []struct {
				role, payload, oldSignature, generated string
			}{
				{"anon", anonPayload, v.anonSignature, k.AnonJWT},
				{"service_role", servicePayload, v.serviceSignature, k.ServiceRoleJWT},
			} {
				data := header + "." + tc.payload
				resigned, err := internalJWT(rand.Reader, private, kid, tc.role, now)
				if err != nil {
					t.Fatal(err)
				}
				for _, token := range []string{tc.generated, resigned} {
					if strings.Join(strings.Split(token, ".")[:2], ".") != data {
						t.Fatal("JWT header or payload changed")
					}
					verifyMigrationJWT(t, token, oldPublic, now)
				}
				// Old signatures verify against the newly derived public key;
				// new signatures above verify against the captured old key.
				verifyMigrationJWT(t, data+"."+tc.oldSignature, newPublic, now)
			}
		})
	}
}

func decodeBase64(t *testing.T, s string) []byte {
	t.Helper()
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func parseJWKPublic(t *testing.T, x, y []byte) *ecdsa.PublicKey {
	t.Helper()
	point := append([]byte{4}, x...)
	point = append(point, y...)
	key, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), point)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func verifyMigrationJWT(t *testing.T, token string, public *ecdsa.PublicKey, now time.Time) {
	t.Helper()
	parsed, err := jwt.Parse(token, func(*jwt.Token) (any, error) { return public, nil },
		jwt.WithValidMethods([]string{"ES256"}), jwt.WithIssuer("supabase"),
		jwt.WithTimeFunc(func() time.Time { return now }))
	if err != nil || !parsed.Valid {
		t.Fatalf("cross-verifying JWT: %v", err)
	}
}
