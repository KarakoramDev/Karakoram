// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
)

func fakeCLI(t *testing.T, tool, body string) {
	t.Helper()
	dir := os.Getenv("KARAKORAM_RABBITMQ_BIN")
	if dir == "" {
		dir = t.TempDir()
		t.Setenv("KARAKORAM_RABBITMQ_BIN", dir)
	}
	if err := os.WriteFile(filepath.Join(dir, tool), []byte("#!/bin/sh\n"+body), 0700); err != nil {
		t.Fatal(err)
	}
}
func TestPasswordsUseStdin(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("KARAKORAM_RABBITMQ_BIN", dir)
	fakeCLI(t, "rabbitmqctl", `printf '%s\n' "$@" > "$CAPTURE/args"
cat > "$CAPTURE/stdin"
printf 'sensitive failure must not escape' >&2
exit 1
`)
	e := New()
	e.env = []string{"CAPTURE=" + dir}
	err := e.password(context.Background(), "change_password", "admin", "super-secret")
	if err == nil || strings.Contains(err.Error(), "sensitive") || strings.Contains(err.Error(), "super-secret") {
		t.Fatal(err)
	}
	args, err := os.ReadFile(filepath.Join(dir, "args"))
	if err != nil {
		t.Fatal(err)
	}
	if string(args) != "change_password\nadmin\n" {
		t.Fatal(string(args))
	}
	input, err := os.ReadFile(filepath.Join(dir, "stdin"))
	if err != nil || string(input) != "super-secret\n" {
		t.Fatal(string(input), err)
	}
	for _, password := range []string{"", "password\ncommand", "password\r", "password\x00"} {
		if e.password(context.Background(), "change_password", "admin", password) == nil {
			t.Fatal("invalid password accepted")
		}
	}
}
func TestCommandTimeoutAndOutputLimit(t *testing.T) {
	t.Setenv("KARAKORAM_RABBITMQ_BIN", t.TempDir())
	fakeCLI(t, "rabbitmqctl", "sleep 30\n")
	start := time.Now()
	_, err := command(context.Background(), nil, "rabbitmqctl", nil, 30*time.Millisecond, "version")
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 2*time.Second {
		t.Fatal(err, time.Since(start))
	}
	var b boundedBuffer
	data := make([]byte, (2<<20)+1)
	n, err := b.Write(data)
	if err != nil || n != len(data) || !b.overflow || b.Len() != 2<<20 {
		t.Fatal(n, err, b.Len())
	}
}
func TestQuorumCriticalDoesNotSuppressCLIError(t *testing.T) {
	t.Setenv("KARAKORAM_RABBITMQ_BIN", t.TempDir())
	e := New()
	fakeCLI(t, "rabbitmq-queues", "printf 'Erlang warning\\n' >&2\nprintf '%s' '{\"result\":\"error\",\"queues\":[{\"name\":\"q\",\"virtual_host\":\"/\",\"type\":\"rabbit_quorum_queue\"}]}' >&2\nexit 69\n")
	result, err := e.execute(context.Background(), nil, "quorum-critical", &rmqspec.EmptyRequest{})
	if err != nil || len(result.(rmqspec.QuorumCriticalResponse).Blockers) != 1 {
		t.Fatal(result, err)
	}
	fakeCLI(t, "rabbitmq-queues", "printf '%s' '{\"result\":\"error\",\"message\":\"broker unavailable\"}' >&2\nexit 69\n")
	if _, err = e.execute(context.Background(), nil, "quorum-critical", &rmqspec.EmptyRequest{}); err == nil {
		t.Fatal("unavailable broker misreported as quorum critical")
	}
	fakeCLI(t, "rabbitmq-queues", "printf '%s' '{\"result\":\"ok\"}'\n")
	result, err = e.execute(context.Background(), nil, "quorum-critical", &rmqspec.EmptyRequest{})
	if err != nil || len(result.(rmqspec.QuorumCriticalResponse).Blockers) != 0 {
		t.Fatal(result, err)
	}
}
func TestMemberSpecValidation(t *testing.T) {
	good := testSpec()
	if _, err := spec(memberConfig(good)); err != nil {
		t.Fatal(err)
	}
	cases := []rmqspec.MemberSpec{}
	s := good
	s.NodeName = "rabbit@no-dashes"
	cases = append(cases, s)
	s = good
	s.Peers = []string{"rabbit@another"}
	cases = append(cases, s)
	s = good
	s.Cookie = "short"
	cases = append(cases, s)
	s = good
	s.AMQPPort = -1
	cases = append(cases, s)
	s = good
	s.Plugins = []string{"rabbitmq_management", "rabbitmq_prometheus", "arbitrary_plugin"}
	cases = append(cases, s)
	s = good
	s.DefaultQueueType = "classic"
	cases = append(cases, s)
	s = good
	s.InterNodeTLS = true
	cases = append(cases, s)
	s = good
	s.PrometheusPort = 0
	cases = append(cases, s)
	s = good
	s.ManagementPort = 0
	cases = append(cases, s)
	s = good
	s.AMQPPort = 0
	cases = append(cases, s)
	for _, s := range cases {
		if _, err := spec(memberConfig(s)); err == nil {
			t.Fatalf("accepted invalid member spec: %#v", s)
		}
	}
}
