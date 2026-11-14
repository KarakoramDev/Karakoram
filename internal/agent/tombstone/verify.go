// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package tombstone

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"os/exec"
	"strings"
	"time"
)

const verificationBudget = 100 * time.Millisecond

type verifier struct {
	run  func(context.Context, string, ...string) ([]byte, error)
	dial func(context.Context, string, string) (net.Conn, error)
	now  func() time.Time
}

// Verify uses libc's NSS path, not Go's resolver, and proves both rejection targets.
func Verify(ctx context.Context, hosts []string) error {
	v := verifier{
		run: func(ctx context.Context, command string, args ...string) ([]byte, error) {
			cmd := exec.CommandContext(ctx, command, args...)
			cmd.Env = []string{"PATH=/usr/sbin:/usr/bin:/sbin:/bin", "LC_ALL=C"}
			return cmd.Output()
		},
		dial: (&net.Dialer{}).DialContext,
		now:  time.Now,
	}
	return v.verify(ctx, hosts)
}

func (v verifier) verify(ctx context.Context, hosts []string) error {
	for _, host := range hosts {
		if !ValidHost(host) {
			return fmt.Errorf("invalid retired hostname %q", host)
		}
		probe, cancel := context.WithTimeout(ctx, 2*time.Second)
		start := v.now()
		output, err := v.run(probe, "/usr/bin/getent", "ahosts", host)
		elapsed := v.now().Sub(start)
		cancel()
		if err != nil {
			return fmt.Errorf("resolve %s: %w", host, err)
		}
		if elapsed > verificationBudget {
			return fmt.Errorf("resolve %s took %s (limit %s)", host, elapsed, verificationBudget)
		}
		if err := sentinelAnswers(output); err != nil {
			return fmt.Errorf("resolve %s: %w", host, err)
		}
	}
	if len(hosts) == 0 {
		return nil
	}
	for _, address := range []string{SentinelIPv4, SentinelIPv6} {
		probe, cancel := context.WithTimeout(ctx, verificationBudget)
		start := v.now()
		conn, err := v.dial(probe, "tcp", net.JoinHostPort(address, "3306"))
		elapsed := v.now().Sub(start)
		timedOut := probe.Err() != nil
		cancel()
		if conn != nil {
			_ = conn.Close()
		}
		if err == nil {
			return fmt.Errorf("sentinel %s accepted TCP", address)
		}
		if timedOut || elapsed > verificationBudget {
			return fmt.Errorf("sentinel %s did not reject TCP quickly: %w", address, err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return nil
}

func sentinelAnswers(output []byte) error {
	found := false
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) < 2 {
			return fmt.Errorf("malformed getent answer")
		}
		address, err := netip.ParseAddr(fields[0])
		if err != nil || (address.Unmap().String() != SentinelIPv4 && address.String() != SentinelIPv6) {
			return fmt.Errorf("non-sentinel answer %q", fields[0])
		}
		found = true
	}
	// AI_ADDRCONFIG may omit a family; every answer must nevertheless be managed.
	if !found {
		return fmt.Errorf("no getent addresses")
	}
	return nil
}
