// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/configtest"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
	"strings"
	"testing"
)

func TestIPv6NativeConfig(t *testing.T) {
	for _, component := range sbspec.Components {
		t.Run(component, func(t *testing.T) {
			s := testSpec(component)
			s.DatabaseIPv6 = true
			s.DataPeers = []string{configtest.IPv6}
			for key := range s.Upstreams {
				s.Upstreams[key] = "[" + configtest.PeerIPv6 + "]:8080"
			}
			p, files, err := Render(s, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			v := configtest.Settings(strings.Join(p.Env, "\n"), "=")
			c := configtest.NetworkConfig{AdvertisedEndpoints: []string{}}
			switch component {
			case "auth":
				c.Listeners = []string{v["GOTRUE_API_HOST"]}
			case "rest":
				c.Listeners = []string{v["PGRST_SERVER_HOST"]}
			case "storage":
				c.Listeners = []string{v["HOST"]}
			case "imgproxy":
				c.Listeners = []string{v["IMGPROXY_BIND"]}
			case "realtime":
				if v["REALTIME_IP_VERSION"] != "ipv6" {
					t.Fatal("Realtime HTTP family not explicit")
				}
				c.Listeners = []string{"::"}
			case "pooler":
				if v["ADDR_TYPE"] != "inet6" {
					t.Fatal("Supavisor HTTP family not explicit")
				}
				c.Listeners = []string{"::"}
			case "analytics":
				c.Listeners = []string{v["PHX_HTTP_IP"]}
			case "functions":
				for i, arg := range p.Args {
					if arg == "--ip" && i+1 < len(p.Args) {
						c.Listeners = []string{p.Args[i+1]}
					}
				}
			case "studio", "meta":
				_, address := privateHTTPListener(true, "3002")
				if component == "meta" {
					_, address = privateHTTPListener(true, "8080")
				}
				c.Listeners = []string{address}
			case "gateway":
				var cfg struct {
					Static struct {
						Listeners []struct {
							Address struct {
								Socket struct {
									Address string `json:"address"`
								} `json:"socket_address"`
							} `json:"address"`
						} `json:"listeners"`
					} `json:"static_resources"`
				}
				if err := json.Unmarshal(files["supabase/envoy.json"], &cfg); err != nil {
					t.Fatal(err)
				}
				for _, listener := range cfg.Static.Listeners {
					if listener.Address.Socket.Address != "127.0.0.1" {
						c.Listeners = append(c.Listeners, listener.Address.Socket.Address)
					}
				}
			case "vector":
				var cfg struct {
					Sources struct {
						Logs struct{ Address string } `json:"member_logs"`
					} `json:"sources"`
				}
				if err := json.Unmarshal(files["supabase/vector.json"], &cfg); err != nil {
					t.Fatal(err)
				}
				c.Listeners = []string{cfg.Sources.Logs.Address}
			}
			for _, peer := range s.Upstreams {
				c.AdvertisedEndpoints = append(c.AdvertisedEndpoints, peer)
			}
			configtest.CheckIPv6NativeConfig(t, c)
		})
	}
}

func TestIPv4NativeConfig(t *testing.T) {
	for _, component := range sbspec.Components {
		t.Run(component, func(t *testing.T) {
			s := testSpec(component)
			s.DatabaseIPv6, s.DataPeers = false, []string{configtest.IPv4}
			for key := range s.Upstreams {
				s.Upstreams[key] = configtest.PeerIPv4 + ":8080"
			}
			p, files, err := Render(s, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			v := configtest.Settings(strings.Join(p.Env, "\n"), "=")
			c := configtest.NetworkConfig{}
			switch component {
			case "auth":
				c.Listeners = []string{v["GOTRUE_API_HOST"]}
			case "rest":
				c.Listeners = []string{v["PGRST_SERVER_HOST"]}
			case "storage":
				c.Listeners = []string{v["HOST"]}
			case "imgproxy":
				c.Listeners = []string{v["IMGPROXY_BIND"]}
			case "realtime":
				if v["REALTIME_IP_VERSION"] != "ipv4" {
					t.Fatal("Realtime IPv4 HTTP requires IPv6")
				}
				c.Listeners = []string{"0.0.0.0"}
			case "pooler":
				if v["ADDR_TYPE"] != "inet" {
					t.Fatal("Supavisor IPv4 listener requires IPv6")
				}
				c.Listeners = []string{"0.0.0.0"}
			case "analytics":
				c.Listeners = []string{v["PHX_HTTP_IP"]}
			case "functions":
				for i, arg := range p.Args {
					if arg == "--ip" && i+1 < len(p.Args) {
						c.Listeners = []string{p.Args[i+1]}
					}
				}
			case "studio", "meta":
				port := "3002"
				if component == "meta" {
					port = "8080"
				}
				network, address := privateHTTPListener(false, port)
				if network != "tcp4" {
					t.Fatal("IPv4 private ingress must not require IPv6")
				}
				c.Listeners = []string{address}
			case "gateway":
				var cfg struct {
					Static struct {
						Listeners []struct {
							Address struct {
								Socket struct {
									Address string `json:"address"`
								} `json:"socket_address"`
							} `json:"address"`
						} `json:"listeners"`
					} `json:"static_resources"`
				}
				if err := json.Unmarshal(files["supabase/envoy.json"], &cfg); err != nil {
					t.Fatal(err)
				}
				for _, listener := range cfg.Static.Listeners {
					if listener.Address.Socket.Address != "127.0.0.1" {
						c.Listeners = append(c.Listeners, listener.Address.Socket.Address)
					}
				}
			case "vector":
				var cfg struct {
					Sources struct {
						Logs struct{ Address string } `json:"member_logs"`
					} `json:"sources"`
				}
				if err := json.Unmarshal(files["supabase/vector.json"], &cfg); err != nil {
					t.Fatal(err)
				}
				c.Listeners = []string{cfg.Sources.Logs.Address}
			}
			for _, peer := range s.Upstreams {
				c.AdvertisedEndpoints = append(c.AdvertisedEndpoints, peer)
			}
			configtest.CheckIPv4NativeConfig(t, c)
		})
	}
}
