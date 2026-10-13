// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package auth

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/rbac"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"golang.org/x/crypto/hkdf"
)

// RecoverAdmin applies operator-supplied local administrator recovery before the
// server accepts traffic. An empty password disables recovery; an empty name
// selects admin. The master key derives a domain-separated fingerprint key, so
// the persisted receipt cannot be used to guess passwords without that key.
//
// The singleton server must be the only writer. The receipt is written last:
// after an interrupted application the next startup repeats the recovery, never
// treating partially recovered credentials, policy or revocations as complete.
// Errors must abort startup. No recovery secret is logged or recorded in events.
func (s *Service) RecoverAdmin(ctx context.Context, name, password, masterKey string) (bool, error) {
	if password == "" {
		return false, nil
	}
	if name == "" {
		name = "admin"
	}
	if domain.ValidateAccountName(name) != nil {
		return false, fmt.Errorf("%w: invalid recovery account", ErrInvalid)
	}
	if err := ValidatePassword(password); err != nil {
		return false, err
	}
	if utf8.RuneCountInString(masterKey) < 32 {
		return false, errors.New("recovery requires the original master key of at least 32 characters")
	}
	var key [32]byte
	if _, err := io.ReadFull(hkdf.New(sha256.New, []byte(masterKey), []byte("karakoram/auth/recovery/v1"), []byte("HMAC-SHA256")), key[:]); err != nil {
		return false, err
	}
	mac := hmac.New(sha256.New, key[:])
	// Valid account names cannot contain NUL, making the tuple unambiguous.
	_, _ = io.WriteString(mac, name)
	_, _ = mac.Write([]byte{0})
	_, _ = io.WriteString(mac, password)
	fingerprint := hex.EncodeToString(mac.Sum(nil))
	s.mu.Lock()
	defer s.mu.Unlock()
	setting := "auth/admin-recovery/" + name
	raw, err := s.o.Store.GetSetting(ctx, setting)
	if err == nil {
		var previous string
		if err = json.Unmarshal(raw, &previous); err != nil {
			return false, errors.New("invalid administrator recovery receipt")
		}
		if hmac.Equal([]byte(previous), []byte(fingerprint)) {
			return false, nil
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return false, err
	}
	a, err := s.o.Store.GetAccount(ctx, name)
	missing := errors.Is(err, store.ErrNotFound)
	if err != nil && !missing {
		return false, err
	}
	if !missing && a.OIDC {
		return false, fmt.Errorf("%w: recovery requires a local account; choose another recovery account", ErrInvalid)
	}
	hash, err := HashPassword(password)
	if err != nil {
		return false, err
	}
	if missing {
		a = &domain.Account{Name: name, DisplayName: "Recovered administrator", CreatedAt: s.now()}
	}
	a.PasswordHash = hash
	a.PasswordChangedAt = s.now()
	a.MustChangePassword = true
	a.Disabled = false
	// Drop prior role subjects so an unrelated role's deny cannot block recovery.
	a.Roles = []string{"role:admin"}
	if missing {
		err = s.o.Store.CreateAccount(ctx, a)
	} else {
		err = s.o.Store.UpdateAccount(ctx, a)
	}
	if err != nil {
		return false, err
	}
	// Explicit removal prevents even future-dated credentials from surviving a
	// recovery and includes each session's current and used refresh-token hashes.
	sessions, err := s.o.Store.ListSessions(ctx)
	if err != nil {
		return false, err
	}
	for _, session := range sessions {
		if session.Owner == name {
			if err = s.o.Store.DeleteSession(ctx, session.ID); err != nil {
				return false, err
			}
		}
	}
	tokens, err := s.o.Store.ListApiTokens(ctx)
	if err != nil {
		return false, err
	}
	for _, token := range tokens {
		if token.Owner == name {
			if err = s.o.Store.DeleteApiToken(ctx, token.ID); err != nil {
				return false, err
			}
		}
	}
	if err = s.recoverAdminPolicy(ctx, name); err != nil {
		return false, err
	}
	event := domain.Event{Resource: "accounts", Target: name, Type: "Warning", Reason: "AdminAccessRecovered", Actor: "system:startup", Message: "Local administrator access recovered for " + name + "; password change required", CreatedAt: s.now()}
	if err = s.o.Store.AppendEvent(ctx, &event); err != nil {
		return false, err
	}
	raw, err = json.Marshal(fingerprint)
	if err != nil {
		return false, err
	}
	if err = s.o.Store.SetSetting(ctx, setting, raw); err != nil {
		return false, err
	}
	if s.o.Logger != nil {
		s.o.Logger.Warn("Local administrator access recovered; password change required", "account", name)
	}
	return true, nil
}

func (s *Service) recoverAdminPolicy(ctx context.Context, name string) error {
	policy, err := rbac.New(s.o.Store)
	if err != nil {
		return err
	}
	rules, err := s.o.Store.ListPolicyRules(ctx)
	if err != nil {
		return err
	}
	user := "user:" + name
	var text bytes.Buffer
	writer := csv.NewWriter(&text)
	var row [6]string
	for _, rule := range rules {
		// Reset only this user's bindings and the built-in admin role's
		// outgoing inheritance/denies. Preserve unrelated policy verbatim.
		if rule.Values[0] == user || rule.Values[0] == "role:admin" {
			if rule.PType == "g" || (rule.PType == "p" && rule.Values[4] == "deny") {
				continue
			}
		}
		// Replace, rather than duplicate, the built-in unrestricted grant.
		if rule.PType == "p" && rule.Values[0] == "role:admin" &&
			rule.Values[1] == "*" && rule.Values[2] == "*" && rule.Values[3] == "*" {
			continue
		}
		row[0] = rule.PType
		copy(row[1:], rule.Values)
		if err = writer.Write(row[:len(rule.Values)+1]); err != nil {
			return err
		}
	}
	if err = writer.Write([]string{"p", "role:admin", "*", "*", "*", "allow"}); err != nil {
		return err
	}
	if err = writer.Write([]string{"g", user, "role:admin"}); err != nil {
		return err
	}
	writer.Flush()
	if err = writer.Error(); err != nil {
		return err
	}
	return policy.ReplacePolicy(ctx, text.String())
}
