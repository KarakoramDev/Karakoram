//go:build linux

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func TestProxyServingPeerUID(t *testing.T) {
	for _, tc := range []struct {
		name      string
		cred      *unix.Ucred
		wantReady bool
	}{
		{"agent", &unix.Ucred{Uid: uint32(os.Geteuid())}, true},
		{"foreign", &unix.Ucred{Uid: uint32(os.Geteuid()) ^ 1}, false},
		{"missing", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := proxyServingPeerUID(tc.cred); (err == nil) != tc.wantReady {
				t.Fatalf("peer credential check = %v", err)
			}
		})
	}
}
