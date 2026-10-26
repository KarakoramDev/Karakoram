// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package auth

import (
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestBusyUsageMaintenanceDoesNotBypassTokenLifecycle(t *testing.T) {
	s := setup(t)
	if _, err := s.CreateAccount(t.Context(), domain.Account{Name: "usage-owner"}, password); err != nil {
		t.Fatal(err)
	}
	// Occupy the best-effort admission slot, as when a previous touch waits
	// for the writer. Authentication must still read authoritative validity.
	s.tokenUsage <- struct{}{}
	defer func() { <-s.tokenUsage }()
	for _, mode := range []string{"expiry", "revocation", "disabled-owner"} {
		t.Run(mode, func(t *testing.T) {
			value, token, err := s.CreateToken(t.Context(), "usage-owner", mode, nil)
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest("GET", "https://example.test/api/v1/clusters", nil)
			r.Header.Set("Authorization", "Bearer "+value)
			if _, err := s.Authenticate(r); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "expiry":
				stored, readErr := s.o.Store.GetApiToken(t.Context(), token.ID)
				if readErr != nil {
					t.Fatal(readErr)
				}
				expired := time.Now().Add(-time.Hour)
				stored.ExpiresAt = &expired
				err = s.o.Store.UpdateApiToken(t.Context(), stored)
			case "revocation":
				err = s.o.Store.DeleteApiToken(t.Context(), token.ID)
			case "disabled-owner":
				account, readErr := s.o.Store.GetAccount(t.Context(), "usage-owner")
				if readErr != nil {
					t.Fatal(readErr)
				}
				account.Disabled = true
				err = s.o.Store.UpdateAccount(t.Context(), account)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Authenticate(r); !errors.Is(err, ErrUnauthenticated) {
				t.Fatalf("%s bypassed while usage maintenance was busy: %v", mode, err)
			}
		})
	}
}
