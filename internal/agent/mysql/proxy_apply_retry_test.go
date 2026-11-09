// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mysql

import (
	"errors"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
)

func TestProxyApplyAdminStartupRetry(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6032}, Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}
	var retry *agent.RetryableApplyError
	if err := proxyApplyError(refused); !errors.As(err, &retry) || !errors.Is(err, syscall.ECONNREFUSED) || retry.MaxDelay != 5*time.Second {
		t.Fatalf("admin startup race was not retained with a short bound: %v", err)
	}
	for _, err := range []error{nil, errors.New("access denied"), errors.New("invalid proxy configuration"), syscall.EACCES, syscall.ETIMEDOUT} {
		if got := proxyApplyError(err); got != err {
			t.Fatalf("non-startup result changed: %v => %v", err, got)
		}
	}
}
