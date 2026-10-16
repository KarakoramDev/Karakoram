// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestCgroupSchedulerLimits(t *testing.T) {
	for _, tc := range []struct {
		quota          string
		fallback, want int
	}{{"50000 100000", 168, 1}, {"100000 100000", 168, 1}, {"200000 100000", 168, 2}, {"250000 100000", 168, 3}, {"800000 100000", 168, 4}, {"max 100000", 168, 4}, {"", 2, 2}, {"broken", 168, 4}, {"100000 0", 168, 4}, {"-1 100000", 0, 1}} {
		if got := schedulersFromQuota(tc.quota, tc.fallback); got != tc.want {
			t.Fatalf("quota %q: got %d, want %d", tc.quota, got, tc.want)
		}
	}
	s := testSpec()
	_, env := render("/data", s, memberConfig(s))
	n := cpuSchedulers()
	flags := fmt.Sprintf("+S %d:%d +SDcpu %d:%d +SDio 2 +A 4", n, n, n, n)
	for _, entry := range []string{"ERL_FLAGS=" + flags, "RABBITMQ_CTL_ERL_ARGS=" + flags + " -proto_dist inet6_tcp", "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=-proto_dist inet6_tcp"} {
		if !slices.Contains(env, entry) {
			t.Fatal(env)
		}
	}
}

func TestDistributionAddressFamilies(t *testing.T) {
	for _, ipv4 := range []bool{false, true} {
		for _, tls := range []bool{false, true} {
			t.Run(fmt.Sprintf("ipv4=%t/tls=%t", ipv4, tls), func(t *testing.T) {
				s := testSpec()
				s.IPv4Distribution, s.InterNodeTLS = ipv4, tls
				files, env := render("/data", s, memberConfig(s))
				proto := "inet6_tcp"
				if ipv4 {
					proto = "inet_tcp"
				}
				if tls {
					proto = "inet6_tls"
					if ipv4 {
						proto = "inet_tls"
					}
				}
				want := "-proto_dist " + proto
				if tls {
					want += " -ssl_dist_optfile /data/karakoram/inter_node_tls.config"
				}
				server, ctl := false, false
				for _, entry := range env {
					if strings.HasPrefix(entry, "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=") {
						server = true
						if entry != "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="+want {
							t.Fatalf("server distribution arguments do not select %s", proto)
						}
					}
					if strings.HasPrefix(entry, "RABBITMQ_CTL_ERL_ARGS=") {
						ctl = true
						if !strings.HasSuffix(entry, " "+want) || strings.Count(entry, "-proto_dist") != 1 {
							t.Fatalf("CLI distribution arguments do not select %s once", proto)
						}
					}
				}
				if !server || !ctl {
					t.Fatal("both broker and CLI must receive distribution arguments")
				}
				for _, file := range files {
					if file.name == "inter_node_tls.config" || file.name == "advanced.config" {
						if strings.Contains(file.content, "inet6") {
							t.Fatal("TLS options must not force IPv6 independently of distribution arguments")
						}
					}
				}
			})
		}
	}
}
func TestCLIEnvironmentDoesNotInheritSecrets(t *testing.T) {
	t.Setenv("KARAKORAM_AGENT_TOKEN", "must-not-leak")
	t.Setenv("UNRELATED_SECRET", "also-private")
	t.Setenv("KARAKORAM_RABBITMQ_BIN", t.TempDir())
	fakeCLI(t, "rabbitmqctl", "exec /usr/bin/env\n")
	for _, environment := range [][]string{nil, {"PATH=/usr/bin:/bin", "HOME=/data/karakoram/home"}} {
		out, err := command(context.Background(), environment, "rabbitmqctl", nil, time.Second, "version")
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(out), "KARAKORAM_AGENT_TOKEN") || strings.Contains(string(out), "UNRELATED_SECRET") {
			t.Fatal("inherited private environment", string(out))
		}
	}
	_, env := render("/data", testSpec(), memberConfig(testSpec()))
	for _, entry := range env {
		if strings.HasPrefix(entry, "KARAKORAM_") {
			t.Fatal(entry)
		}
	}
}
func TestStreamListenersAndOsirisTLS(t *testing.T) {
	for _, mode := range []string{"disabled", "preferTLS", "requireTLS"} {
		t.Run(mode, func(t *testing.T) {
			s := testSpec()
			s.Plugins = append(slices.Clone(s.Plugins), "rabbitmq_stream")
			s.StreamPort = 5552
			c := memberConfig(s)
			c.TLS.Mode = mode
			c.TLS.CAPEM = "CA"
			c.TLS.CertPEM = "CERT"
			c.TLS.KeyPEM = "KEY"
			if mode != "disabled" {
				s.StreamTLSPort = 5551
			}
			if mode == "requireTLS" {
				s.StreamPort = 0
				s.InterNodeTLS = true
			}
			c.EngineSpec = nil
			files, env := render("/data", s, c)
			config := files[0].content
			tcp := "stream.listeners.tcp.default = 5552\n"
			if mode == "requireTLS" {
				tcp = "stream.listeners.tcp = none\n"
			}
			if !strings.Contains(config, tcp) {
				t.Fatal(config)
			}
			if mode != "disabled" {
				if !strings.Contains(config, "stream.listeners.ssl.default = 5551\n") || !strings.Contains(config, "ssl_options.keyfile = /data/karakoram/tls/server.key\n") {
					t.Fatal(config)
				}
			} else if strings.Contains(config, "stream.listeners.ssl") {
				t.Fatal(config)
			}
			advanced := ""
			for _, file := range files {
				if file.name == "advanced.config" {
					advanced = file.content
				}
			}
			if s.InterNodeTLS {
				for _, part := range []string{"{replication_transport,ssl}", "{replication_server_ssl_options,", "{replication_client_ssl_options,", "{verify,verify_peer}", "{fail_if_no_peer_cert,true}", "/data/karakoram/tls/server.key"} {
					if !strings.Contains(advanced, part) {
						t.Fatal(advanced)
					}
				}
				for _, entry := range env {
					if strings.HasPrefix(entry, "ERL_FLAGS=") && strings.Contains(entry, "-ssl_dist_optfile") {
						t.Fatal("discovery peers would inherit duplicate TLS optfile", entry)
					}
					if (strings.HasPrefix(entry, "RABBITMQ_CTL_ERL_ARGS=") || strings.HasPrefix(entry, "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=")) && !strings.Contains(entry, "-proto_dist inet6_tls -ssl_dist_optfile /data/karakoram/inter_node_tls.config") {
						t.Fatal(entry)
					}
				}
			} else if advanced != "[].\n" {
				t.Fatal(advanced)
			}
		})
	}
}
func TestQueueMembershipResults(t *testing.T) {
	for _, out := range []string{`[]`, `[{"vhost":"/","name":"q","size":3,"result":"ok"}]`} {
		if err := checkQueueResults([]byte(out)); err != nil {
			t.Fatal(err)
		}
	}
	for _, out := range []string{`null`, `{}`, `[{"name":"q"}]`, `[{"name":"q","result":"error: non_stable_members"}]`, `[{"result":"error: the last node cannot be removed"}]`, `[{"result":"ok"},{"result":"error: the operation timed out and may not have been completed"}]`} {
		if checkQueueResults([]byte(out)) == nil {
			t.Fatal("accepted failed membership change", out)
		}
	}
}
func TestEightPeerReplacementSpec(t *testing.T) {
	s := testSpec()
	for _, name := range []string{"aa", "bb", "cc", "dd", "ee", "ff", "gg"} {
		s.Peers = append(s.Peers, "rabbit@"+name)
	}
	if _, err := spec(memberConfig(s)); err != nil {
		t.Fatal(err)
	}
	s.Peers = append(s.Peers, "rabbit@hh")
	if _, err := spec(memberConfig(s)); err == nil {
		t.Fatal("accepted nine peers")
	}
}
