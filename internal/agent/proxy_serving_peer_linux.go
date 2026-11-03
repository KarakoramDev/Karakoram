//go:build linux

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"errors"
	"fmt"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

func proxyServingPeerUID(cred *unix.Ucred) error {
	if cred == nil || cred.Uid != uint32(os.Geteuid()) {
		return errors.New("proxy serving listener is not owned by the agent effective UID")
	}
	return nil
}

func proxyServingPeer(conn *net.UnixConn) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return fmt.Errorf("proxy serving peer credentials: %w", err)
	}
	var cred *unix.Ucred
	var credentialErr error
	if err := raw.Control(func(fd uintptr) {
		cred, credentialErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return fmt.Errorf("proxy serving peer credentials: %w", err)
	}
	if credentialErr != nil {
		return fmt.Errorf("proxy serving peer credentials: %w", credentialErr)
	}
	return proxyServingPeerUID(cred)
}
