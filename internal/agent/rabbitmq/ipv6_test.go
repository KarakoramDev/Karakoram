// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"github.com/KarakoramDev/Karakoram/internal/configtest"
	"strings"
	"testing"
)

func TestIPv6NativeConfig(t *testing.T) {
	for _, tls := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "tls"}[tls], func(t *testing.T) {
			s := testSpec()
			s.NodeName = "rabbit@" + configtest.Member
			s.Peers = []string{s.NodeName, "rabbit@" + configtest.Peer}
			s.IPv4Distribution = false
			s.InterNodeTLS = tls
			if tls {
				s.ManagementTLSPort = 15671
			}
			files, env := render(t.TempDir(), s, memberConfig(s))
			for _, file := range files {
				configtest.CheckIPv6Rendered(t, map[string][]byte{file.name: []byte(file.content)}, nil, nil, map[string]string{
					"prometheus.tcp.ip = 127.0.0.1": "node-local metrics endpoint",
				})
			}
			configtest.CheckIPv6Rendered(t, nil, nil, env, nil)
			values := configtest.Settings(strings.Join(env, "\n"), "=")
			want := "inet6_tcp"
			if tls {
				want = "inet6_tls"
			}
			if !strings.Contains(values["RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS"], "-proto_dist "+want) {
				t.Fatal("distribution is not IPv6-capable")
			}
			var text string
			for _, f := range files {
				if f.name == "rabbitmq.conf" {
					text = f.content
				}
			}
			c := configtest.Settings(text, "=")
			if c["listeners.tcp.default"] != "5672" || c["management.tcp.port"] != "15672" {
				t.Fatal("native wildcard listeners missing")
			}
			// A port-only RabbitMQ listener means all available interfaces, not the
			// IPv4 wildcard. Distribution selects inet6 explicitly above.
			listeners := []string{c["management.tcp.ip"]}
			if tls {
				listeners = append(listeners, c["management.ssl.ip"])
			}
			configtest.CheckIPv6NativeConfig(t, configtest.NetworkConfig{Listeners: listeners, AdvertisedHosts: []string{strings.TrimPrefix(values["RABBITMQ_NODENAME"], "rabbit@"), strings.TrimPrefix(c["cluster_formation.classic_config.nodes.2"], "rabbit@")}})
		})
	}
	s := testSpec()
	s.IPv4Distribution = true
	files, _ := render(t.TempDir(), s, memberConfig(s))
	if !strings.Contains(files[0].content, "management.tcp.ip = 0.0.0.0\n") {
		t.Fatal("IPv4 backend management listener changed")
	}
}

func TestIPv4NativeConfig(t *testing.T) {
	s := testSpec()
	s.NodeName, s.Peers = "rabbit@"+configtest.Member, []string{"rabbit@" + configtest.Peer}
	s.IPv4Distribution, s.InterNodeTLS, s.ManagementTLSPort = true, true, 15671
	files, env := render(t.TempDir(), s, memberConfig(s))
	v := configtest.Settings(files[0].content, "=")
	e := configtest.Settings(strings.Join(env, "\n"), "=")
	if !strings.Contains(e["RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS"], "-proto_dist inet_tls ") || strings.Contains(e["RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS"], "inet6") {
		t.Fatal("IPv4 distribution requires IPv6")
	}
	configtest.CheckIPv4NativeConfig(t, configtest.NetworkConfig{Listeners: []string{v["management.tcp.ip"], v["management.ssl.ip"]}, AdvertisedHosts: []string{strings.TrimPrefix(e["RABBITMQ_NODENAME"], "rabbit@"), strings.TrimPrefix(v["cluster_formation.classic_config.nodes.1"], "rabbit@")}})
}
