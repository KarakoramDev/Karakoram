//go:build !linux

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import "testing"

func TestProxyServingPeerFailsClosedWithoutLinux(t *testing.T) {
	if err := proxyServingPeer(nil); err == nil {
		t.Fatal("proxy serving peer authentication unavailable but admitted readiness")
	}
}
