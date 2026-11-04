//go:build !linux

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"errors"
	"net"
)

// Never admit a proxy using an unauthenticated readiness socket on platforms
// without the Linux peer-credential check.
func proxyServingPeer(*net.UnixConn) error {
	return errors.New("proxy serving peer authentication requires Linux")
}
