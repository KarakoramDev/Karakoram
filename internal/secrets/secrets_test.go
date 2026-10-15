// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package secrets

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestSealing(t *testing.T) {
	if _, err := New("short"); err == nil {
		t.Fatal("accepted short key")
	}
	s, err := New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.SealFor("one", []byte("credential"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.SealFor("one", []byte("credential"))
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("nonce reused")
	}
	plain, err := s.OpenFor("one", a)
	if err != nil || string(plain) != "credential" {
		t.Fatalf("roundtrip %q %v", plain, err)
	}
	if _, err = s.OpenFor("two", a); err == nil {
		t.Fatal("identity substitution accepted")
	}
	wrong, _ := New(strings.Repeat("y", 32))
	if _, err = wrong.OpenFor("one", a); err == nil {
		t.Fatal("wrong key accepted")
	}
	raw, _ := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(a, prefix))
	raw[len(raw)-1] ^= 1
	for _, value := range []string{"kk:v2:unknown", prefix, prefix + "?", prefix + base64.RawStdEncoding.EncodeToString(raw)} {
		if _, err = s.OpenFor("one", value); err == nil {
			t.Fatalf("accepted %s", value)
		}
	}
	empty, err := s.Seal(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Open(empty); err != nil {
		t.Fatal(err)
	}
}
func TestGenerators(t *testing.T) {
	p, err := Password()
	if err != nil || len(p) != 40 {
		t.Fatalf("password %v", err)
	}
	token, err := Token()
	if err != nil || !strings.HasPrefix(token, "kk_") || len(token) != 51 {
		t.Fatalf("token %v", err)
	}
	key, err := Keyfile()
	if err != nil || len(key) != 756 {
		t.Fatalf("keyfile %v", err)
	}
	if _, err = base64.StdEncoding.DecodeString(key); err != nil {
		t.Fatal(err)
	}
	if _, err = RandomString(0); err == nil {
		t.Fatal("zero length accepted")
	}
	if Hash(token) == Hash(p) || len(Hash(token)) != 64 {
		t.Fatal("hash")
	}
}

func TestIdempotencyHashesUseSeparatedMasterKeySubkeys(t *testing.T) {
	first, err := New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	same, err := New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	other, err := New(strings.Repeat("y", 32))
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"password":"chosen-test-password"}`)
	digest := first.IdempotencyBodyHash(body)
	if !strings.HasPrefix(digest, "hmac-sha256:v1:") || digest == "hmac-sha256:v1:"+Hash(string(body)) {
		t.Fatal("body digest is not a versioned keyed hash")
	}
	if digest != same.IdempotencyBodyHash(body) {
		t.Fatal("same master key cannot match a retry")
	}
	if digest == other.IdempotencyBodyHash(body) || digest == first.IdempotencyIdentityHash(body) {
		t.Fatal("master key or purpose did not separate digests")
	}
	if digest == first.IdempotencyBodyHash([]byte(`{"password":"different-test-password"}`)) {
		t.Fatal("different bodies matched")
	}
	if first.IdempotencyIdentityHash(body) != same.IdempotencyIdentityHash(body) ||
		first.IdempotencyIdentityHash(body) == other.IdempotencyIdentityHash(body) {
		t.Fatal("identity digest does not use the installation key")
	}
}
