// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package tombstone

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidHost(t *testing.T) {
	for _, s := range []string{"a", "member-01", strings.Repeat("a", 63)} {
		if !ValidHost(s) {
			t.Fatal(s)
		}
	}
	for _, s := range []string{"", "A", "-a", "a.b", "a b", "a\n", strings.Repeat("a", 64)} {
		if ValidHost(s) {
			t.Fatal(s)
		}
	}
}

func TestVerifyNativeResolver(t *testing.T) {
	for _, tc := range []struct {
		name, output     string
		lookup, connect  time.Duration
		accept, runError bool
		want             bool
	}{
		{name: "both", output: SentinelIPv4 + " STREAM host\n" + SentinelIPv6 + " STREAM host\n", want: true},
		{name: "addrconfig", output: SentinelIPv4 + " STREAM host\n", want: true},
		{name: "dead duplicate", output: SentinelIPv4 + " STREAM host\n10.1.2.3 STREAM host\n"},
		{name: "empty"},
		{name: "malformed", output: "garbage"},
		{name: "slow lookup", output: SentinelIPv4 + " STREAM host", lookup: 101 * time.Millisecond},
		{name: "slow dial", output: SentinelIPv4 + " STREAM host", connect: 101 * time.Millisecond},
		{name: "accepting sentinel", output: SentinelIPv4 + " STREAM host", accept: true},
		{name: "exec error", runError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			at := time.Unix(1, 0)
			calls := 0
			v := verifier{now: func() time.Time { return at }, run: func(ctx context.Context, command string, args ...string) ([]byte, error) {
				if command != "/usr/bin/getent" || strings.Join(args, " ") != "ahosts retired" {
					t.Fatal(command, args)
				}
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > 2*time.Second {
					t.Fatal("missing exec deadline")
				}
				at = at.Add(tc.lookup)
				if tc.runError {
					return nil, errors.New("exec failed")
				}
				return []byte(tc.output), nil
			}, dial: func(_ context.Context, network, address string) (net.Conn, error) {
				calls++
				at = at.Add(tc.connect)
				if network != "tcp" || (address != net.JoinHostPort(SentinelIPv4, "3306") && address != net.JoinHostPort(SentinelIPv6, "3306")) {
					t.Fatal(network, address)
				}
				if tc.accept {
					return nil, nil
				}
				return nil, errors.New("network unreachable")
			}}
			err := v.verify(t.Context(), []string{"retired"})
			if (err == nil) != tc.want {
				t.Fatal(err)
			}
			if tc.want && calls != 2 {
				t.Fatal("both sentinel families must be probed", calls)
			}
		})
	}
}

func TestVerifyIgnoresHostilePATH(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "executed")
	if err := os.WriteFile(filepath.Join(dir, "getent"), []byte("#!/bin/sh\n: > "+marker+"\nprintf '239.192.0.1 STREAM localhost\\n'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	t.Setenv("LC_ALL", "hostile")
	if err := Verify(t.Context(), []string{"localhost"}); err == nil {
		t.Fatal("hostile getent proof accepted")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("PATH getent executed", err)
	}
}
