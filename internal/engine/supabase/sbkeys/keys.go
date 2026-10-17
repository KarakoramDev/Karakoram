// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package sbkeys generates Supabase credentials without persistence or logging.
// All returned values, including JWKS (which contains a symmetric verification
// key), are secrets and must be kept in the encrypted SecretStore. JWKS is for
// private service configuration, not a public discovery endpoint.
package sbkeys

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"time"
)

const lifetime = 5 * 365 * 24 * time.Hour

// Keys is a complete generated credential set. JWTKeys is Auth's private JWK
// array; JWKS contains the public EC key and legacy symmetric key for services.
// Neither document is safe to publish. DashboardUsername is deliberately not
// generated: the provider chooses the non-secret dashboard account name.
type Keys struct {
	JWTSecret                string
	AnonKey                  string
	ServiceRoleKey           string
	PublishableKey           string
	SecretKey                string
	AnonJWT                  string
	ServiceRoleJWT           string
	JWTKeys                  json.RawMessage
	JWKS                     json.RawMessage
	SecretKeyBase            string
	RealtimeDBEncKey         string
	VaultEncKey              string
	PGMetaCryptoKey          string
	LogflarePublicToken      string
	LogflarePrivateToken     string
	DashboardPassword        string
	StorageS3AccessKeyID     string
	StorageS3SecretAccessKey string
}

// Generate consumes cryptographically secure randomness supplied by the caller
// (normally crypto/rand.Reader) and a caller-owned clock. It has no side effects.
func Generate(random io.Reader, now time.Time) (Keys, error) {
	var k Keys
	fields := []struct {
		dst *string
		n   int
		hex bool
	}{
		{&k.JWTSecret, 32, false}, {&k.SecretKeyBase, 48, false},
		{&k.RealtimeDBEncKey, 8, true}, {&k.VaultEncKey, 16, true},
		{&k.PGMetaCryptoKey, 24, false}, {&k.LogflarePublicToken, 24, false},
		{&k.LogflarePrivateToken, 24, false}, {&k.DashboardPassword, 24, false},
		{&k.StorageS3AccessKeyID, 16, true}, {&k.StorageS3SecretAccessKey, 32, true},
	}
	for _, f := range fields {
		b, err := entropy(random, f.n)
		if err != nil {
			return Keys{}, err
		}
		if f.hex {
			*f.dst = hex.EncodeToString(b)
		} else {
			*f.dst = base64.StdEncoding.EncodeToString(b)
		}
	}
	var err error
	k.AnonKey, err = LegacyJWT(k.JWTSecret, "anon", now)
	if err != nil {
		return Keys{}, err
	}
	k.ServiceRoleKey, err = LegacyJWT(k.JWTSecret, "service_role", now)
	if err != nil {
		return Keys{}, err
	}
	k.PublishableKey, err = OpaqueKey(random, "sb_publishable_")
	if err != nil {
		return Keys{}, err
	}
	k.SecretKey, err = OpaqueKey(random, "sb_secret_")
	if err != nil {
		return Keys{}, err
	}
	// Explicit scalar sampling makes injected readers deterministic, unlike
	// ecdsa.GenerateKey's optional extra random read. Rejection avoids bias.
	curve := elliptic.P256()
	var d *big.Int
	for {
		b, e := entropy(random, 32)
		if e != nil {
			return Keys{}, e
		}
		d = new(big.Int).SetBytes(b)
		if d.Sign() > 0 && d.Cmp(curve.Params().N) < 0 {
			break
		}
	}
	x, y := curve.ScalarBaseMult(d.Bytes())
	private := &ecdsa.PrivateKey{PublicKey: ecdsa.PublicKey{Curve: curve, X: x, Y: y}, D: d}
	id, err := entropy(random, 16)
	if err != nil {
		return Keys{}, err
	}
	id[6] = (id[6] & 15) | 64
	id[8] = (id[8] & 63) | 128
	h := hex.EncodeToString(id)
	kid := h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
	public := map[string]any{"kty": "EC", "kid": kid, "use": "sig", "key_ops": []string{"verify"}, "alg": "ES256", "ext": true, "crv": "P-256", "x": b64(x.FillBytes(make([]byte, 32))), "y": b64(y.FillBytes(make([]byte, 32)))}
	oct := map[string]any{"kty": "oct", "k": b64([]byte(k.JWTSecret)), "alg": "HS256"}
	k.JWKS, err = json.Marshal(map[string]any{"keys": []any{public, oct}})
	if err != nil {
		return Keys{}, err
	}
	public["d"] = b64(d.FillBytes(make([]byte, 32)))
	public["key_ops"] = []string{"sign", "verify"}
	k.JWTKeys, err = json.Marshal([]any{public, oct})
	if err != nil {
		return Keys{}, err
	}
	k.AnonJWT, err = internalJWT(random, private, kid, "anon", now)
	if err != nil {
		return Keys{}, err
	}
	k.ServiceRoleJWT, err = internalJWT(random, private, kid, "service_role", now)
	if err != nil {
		return Keys{}, err
	}
	return k, nil
}

// LegacyJWT signs the upstream legacy header and role claims with the literal
// configured secret string, NOT the bytes obtained by base64-decoding it.
func LegacyJWT(secret, role string, now time.Time) (string, error) {
	if len(secret) < 32 {
		return "", errors.New("JWT secret must contain at least 32 bytes")
	}
	payload, err := claims(role, now)
	if err != nil {
		return "", err
	}
	data := b64([]byte(`{"alg":"HS256","typ":"JWT"}`)) + "." + b64(payload)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(data))
	return data + "." + b64(mac.Sum(nil)), nil
}

// OpaqueKey implements the upstream self-hosted 22-character random body and
// eight-character SHA256 checksum. Only the two upstream prefixes are accepted.
func OpaqueKey(random io.Reader, prefix string) (string, error) {
	if prefix != "sb_publishable_" && prefix != "sb_secret_" {
		return "", errors.New("invalid opaque key prefix")
	}
	raw, err := entropy(random, 17)
	if err != nil {
		return "", err
	}
	body := prefix + b64(raw)[:22]
	sum := sha256.Sum256([]byte("supabase-self-hosted|" + body))
	return body + "_" + b64(sum[:])[:8], nil
}
func claims(role string, now time.Time) ([]byte, error) {
	if role != "anon" && role != "service_role" {
		return nil, errors.New("invalid API key role")
	}
	return json.Marshal(struct {
		Role    string `json:"role"`
		Issuer  string `json:"iss"`
		Issued  int64  `json:"iat"`
		Expires int64  `json:"exp"`
	}{role, "supabase", now.Unix(), now.Add(lifetime).Unix()})
}
func internalJWT(random io.Reader, key *ecdsa.PrivateKey, kid, role string, now time.Time) (string, error) {
	payload, err := claims(role, now)
	if err != nil {
		return "", err
	}
	header, _ := json.Marshal(struct {
		Algorithm string `json:"alg"`
		Type      string `json:"typ"`
		ID        string `json:"kid"`
	}{"ES256", "JWT", kid})
	data := b64(header) + "." + b64(payload)
	digest := sha256.Sum256([]byte(data))
	r, s, err := ecdsa.Sign(random, key, digest[:])
	if err != nil {
		return "", err
	}
	sig := make([]byte, 64)
	r.FillBytes(sig[:32])
	s.FillBytes(sig[32:])
	return data + "." + b64(sig), nil
}
func entropy(random io.Reader, n int) ([]byte, error) {
	if random == nil {
		return nil, errors.New("random reader is required")
	}
	b := make([]byte, n)
	_, err := io.ReadFull(random, b)
	return b, err
}
func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
