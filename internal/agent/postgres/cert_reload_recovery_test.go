// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"errors"
	"testing"
)

func TestPatroniTLSReloadPrecedesAuthenticatedRequest(t *testing.T) {
	for _, name := range []string{"expired", "future-not-before"} {
		t.Run(name, func(t *testing.T) {
			stale := true
			signalCalls, requests := 0, 0
			err := reloadPatroniConfig([]string{"server.pem", "server.key"}, func() error {
				signalCalls++
				stale = false // Patroni replaces its HTTPS context from the new local files.
				return nil
			}, func() error {
				requests++
				if stale {
					return errors.New("old Patroni HTTPS leaf is not currently valid")
				}
				return nil
			})
			if err != nil || signalCalls != 1 || requests != 1 {
				t.Fatalf("reload=%d requests=%d err=%v", signalCalls, requests, err)
			}
		})
	}
}

func TestPatroniReloadDoesNotBypassSignalFailure(t *testing.T) {
	refused := errors.New("process is no longer running")
	requested := false
	err := reloadPatroniConfig([]string{"server.pem"}, func() error { return refused }, func() error { requested = true; return nil })
	if !errors.Is(err, refused) || requested {
		t.Fatalf("failed native reload was ignored: request=%v err=%v", requested, err)
	}
}
