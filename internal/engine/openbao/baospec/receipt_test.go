// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package baospec

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestReceiptRoundTripAndBinding(t *testing.T) {
	private, public, err := NewRecipientKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	plaintext := []byte(`{"keys":["a"],"root_token":"s.secret"}`)
	env, err := SealReceipt(rand.Reader, public, "vault", "vaultm0", "0123456789abcdef", plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(env.Ciphertext, []byte("s.secret")) {
		t.Fatal("ciphertext contains plaintext")
	}
	got, err := OpenReceipt(private, "vault", "vaultm0", env)
	if err != nil || !bytes.Equal(got, plaintext) {
		t.Fatalf("open = %q, %v", got, err)
	}
	if _, err := OpenReceipt(private, "vault", "vaultm1", env); err == nil {
		t.Fatal("receipt opened for another member")
	}
	if _, err := OpenReceipt(private, "other", "vaultm0", env); err == nil {
		t.Fatal("receipt opened for another cluster")
	}
	swapped := env
	swapped.ReceiptID = "fedcba9876543210"
	if _, err := OpenReceipt(private, "vault", "vaultm0", swapped); err == nil {
		t.Fatal("receipt opened under another receipt id")
	}
	otherPrivate, _, err := NewRecipientKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReceipt(otherPrivate, "vault", "vaultm0", env); err == nil {
		t.Fatal("receipt opened with another key")
	}
	tampered := env
	tampered.Ciphertext = append([]byte(nil), env.Ciphertext...)
	tampered.Ciphertext[0] ^= 1
	if _, err := OpenReceipt(private, "vault", "vaultm0", tampered); err == nil {
		t.Fatal("tampered receipt opened")
	}
}

func TestReceiptRejectsInvalidInput(t *testing.T) {
	_, public, err := NewRecipientKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SealReceipt(rand.Reader, public, "c", "m", "short", []byte("x")); err == nil {
		t.Fatal("short receipt id accepted")
	}
	if _, err := SealReceipt(rand.Reader, public[:5], "c", "m", "0123456789abcdef", []byte("x")); err == nil {
		t.Fatal("bad public key accepted")
	}
	if _, err := SealReceipt(rand.Reader, public, "c", "m", "0123456789abcdef", nil); err == nil {
		t.Fatal("empty plaintext accepted")
	}
}
