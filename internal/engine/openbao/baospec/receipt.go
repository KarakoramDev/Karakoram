// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package baospec

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"regexp"
)

// ReceiptScheme identifies the envelope construction: ephemeral X25519 ECDH
// with the recipient key, HKDF-SHA256 key derivation and AES-256-GCM.
const ReceiptScheme = "x25519-hkdf-sha256-aes256gcm/v1"

// MaxReceiptPlaintext bounds the sealed initialization material.
const MaxReceiptPlaintext = 64 << 10

var receiptIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{16,64}$`)

// Envelope is an encrypted initialization receipt. It is safe to store on the
// member volume and to return over the agent API.
type Envelope struct {
	Scheme             string `json:"scheme"`
	ReceiptID          string `json:"receiptId"`
	EphemeralPublicKey []byte `json:"ephemeralPublicKey"`
	Nonce              []byte `json:"nonce"`
	Ciphertext         []byte `json:"ciphertext"`
}

// ValidReceiptID reports whether id is an acceptable receipt identifier.
func ValidReceiptID(id string) bool { return receiptIDPattern.MatchString(id) }

// NewRecipientKey returns a fresh X25519 private key and its public key bytes.
func NewRecipientKey(random io.Reader) (private []byte, public []byte, err error) {
	key, err := ecdh.X25519().GenerateKey(random)
	if err != nil {
		return nil, nil, err
	}
	return key.Bytes(), key.PublicKey().Bytes(), nil
}

// SealReceipt encrypts plaintext to recipientPublicKey, bound to cluster,
// member and receiptID.
func SealReceipt(random io.Reader, recipientPublicKey []byte, cluster, member, receiptID string, plaintext []byte) (Envelope, error) {
	if !ValidReceiptID(receiptID) {
		return Envelope{}, errors.New("invalid receipt id")
	}
	if len(plaintext) == 0 || len(plaintext) > MaxReceiptPlaintext {
		return Envelope{}, errors.New("invalid receipt plaintext size")
	}
	recipient, err := ecdh.X25519().NewPublicKey(recipientPublicKey)
	if err != nil {
		return Envelope{}, fmt.Errorf("recipient public key: %w", err)
	}
	ephemeral, err := ecdh.X25519().GenerateKey(random)
	if err != nil {
		return Envelope{}, err
	}
	shared, err := ephemeral.ECDH(recipient)
	if err != nil {
		return Envelope{}, err
	}
	env := Envelope{Scheme: ReceiptScheme, ReceiptID: receiptID, EphemeralPublicKey: ephemeral.PublicKey().Bytes()}
	aead, err := receiptAEAD(shared, env.EphemeralPublicKey, recipientPublicKey, receiptID)
	if err != nil {
		return Envelope{}, err
	}
	env.Nonce = make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(random, env.Nonce); err != nil {
		return Envelope{}, err
	}
	env.Ciphertext = aead.Seal(nil, env.Nonce, plaintext, receiptAAD(cluster, member, receiptID))
	return env, nil
}

// OpenReceipt decrypts env with recipientPrivateKey for cluster and member.
func OpenReceipt(recipientPrivateKey []byte, cluster, member string, env Envelope) ([]byte, error) {
	if env.Scheme != ReceiptScheme {
		return nil, fmt.Errorf("unsupported receipt scheme %q", env.Scheme)
	}
	if !ValidReceiptID(env.ReceiptID) {
		return nil, errors.New("invalid receipt id")
	}
	private, err := ecdh.X25519().NewPrivateKey(recipientPrivateKey)
	if err != nil {
		return nil, fmt.Errorf("recipient private key: %w", err)
	}
	ephemeral, err := ecdh.X25519().NewPublicKey(env.EphemeralPublicKey)
	if err != nil {
		return nil, fmt.Errorf("receipt ephemeral key: %w", err)
	}
	shared, err := private.ECDH(ephemeral)
	if err != nil {
		return nil, err
	}
	aead, err := receiptAEAD(shared, env.EphemeralPublicKey, private.PublicKey().Bytes(), env.ReceiptID)
	if err != nil {
		return nil, err
	}
	if len(env.Nonce) != aead.NonceSize() || len(env.Ciphertext) > MaxReceiptPlaintext+aead.Overhead() {
		return nil, errors.New("malformed receipt")
	}
	plaintext, err := aead.Open(nil, env.Nonce, env.Ciphertext, receiptAAD(cluster, member, env.ReceiptID))
	if err != nil {
		return nil, errors.New("receipt authentication failed")
	}
	return plaintext, nil
}

func receiptAEAD(shared, ephemeralPublic, recipientPublic []byte, receiptID string) (cipher.AEAD, error) {
	salt := make([]byte, 0, len(ephemeralPublic)+len(recipientPublic))
	salt = append(append(salt, ephemeralPublic...), recipientPublic...)
	key, err := hkdf.Key(sha256.New, shared, salt, "karakoram/openbao/init-receipt/v1\x00"+receiptID, 32)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func receiptAAD(cluster, member, receiptID string) []byte {
	return []byte(ReceiptScheme + "\x00" + cluster + "\x00" + member + "\x00" + receiptID)
}
