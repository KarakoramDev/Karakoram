// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"

	"golang.org/x/crypto/hkdf"
)

const prefix = "kk:v1:"

// Sealer encrypts versioned, authenticated secrets using a derived AES-256 key.
type Sealer struct{ aead cipher.AEAD }

// New derives a sealer from a master key of at least 32 characters.
func New(masterKey string) (*Sealer, error) {
	if utf8.RuneCountInString(masterKey) < 32 {
		return nil, errors.New("KARAKORAM_MASTER_KEY must contain at least 32 characters")
	}
	key := make([]byte, 32)
	if _, err := io.ReadFull(hkdf.New(sha256.New, []byte(masterKey), []byte("karakoram/secrets/v1"), []byte("AES-256-GCM")), key); err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Sealer{aead: aead}, nil
}

// FromEnv loads the encryption master key from KARAKORAM_MASTER_KEY.
func FromEnv() (*Sealer, error) { return New(os.Getenv("KARAKORAM_MASTER_KEY")) }

// Seal encrypts a secret without an additional storage identity.
func (s *Sealer) Seal(plaintext []byte) (string, error) { return s.SealFor("", plaintext) }

// Open authenticates and decrypts a versioned secret.
func (s *Sealer) Open(value string) ([]byte, error) { return s.OpenFor("", value) }

// SealFor binds a ciphertext to its storage identity to prevent substitution.
func (s *Sealer) SealFor(identity string, plaintext []byte) (string, error) {
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := s.aead.Seal(nonce, nonce, plaintext, []byte(prefix+identity))
	return prefix + base64.RawStdEncoding.EncodeToString(sealed), nil
}

// OpenFor decrypts a secret only for its bound storage identity.
func (s *Sealer) OpenFor(identity, value string) ([]byte, error) {
	if !strings.HasPrefix(value, prefix) {
		return nil, errors.New("unsupported sealed-secret version")
	}
	data, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(value, prefix))
	if err != nil {
		return nil, errors.New("invalid sealed-secret encoding")
	}
	n := s.aead.NonceSize()
	if len(data) < n+s.aead.Overhead() {
		return nil, errors.New("truncated sealed secret")
	}
	plain, err := s.aead.Open(nil, data[:n], data[n:], []byte(prefix+identity))
	if err != nil {
		return nil, errors.New("sealed-secret authentication failed")
	}
	return plain, nil
}

// RandomString generates unbiased alphanumeric characters using crypto/rand.
func RandomString(length int) (string, error) {
	if length < 1 {
		return "", fmt.Errorf("length must be positive")
	}
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	out := make([]byte, length)
	buf := make([]byte, length+16)
	n := 0
	for n < length {
		if _, err := rand.Read(buf); err != nil {
			return "", err
		}
		for _, v := range buf {
			if v >= 248 {
				continue
			}
			out[n] = alphabet[int(v)%len(alphabet)]
			n++
			if n == length {
				break
			}
		}
	}
	return string(out), nil
}

// Password generates a cryptographically random 40-character password.
func Password() (string, error) { return RandomString(40) }

// Token generates a cryptographically random kk_-prefixed API token.
func Token() (string, error) {
	s, err := RandomString(48)
	if err != nil {
		return "", err
	}
	return "kk_" + s, nil
}

// Keyfile generates exactly 756 base64 characters for MongoDB internal authentication.
func Keyfile() (string, error) {
	data := make([]byte, 567)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

// Hash returns the hexadecimal SHA-256 digest used for token lookup.
func Hash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
