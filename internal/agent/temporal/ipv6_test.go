// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"github.com/KarakoramDev/Karakoram/internal/configtest"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"gopkg.in/yaml.v3"
	"testing"
)

func TestIPv6NativeConfig(t *testing.T) {
	// Rendering is IPv6-safe; production admission intentionally requires private
	// IPv4 until Temporal route discovery/membership is qualified on IPv6.
	for _, role := range []string{tspec.Server, tspec.Frontend, tspec.History, tspec.Matching, tspec.Worker, tspec.UI} {
		t.Run(role, func(t *testing.T) {
			s := testSpec()
			s.Component = role
			s.Persistence.Host = configtest.PeerIPv6
			s.FrontendAddress = "[" + configtest.IPv6 + "]:7233"
			_, files, err := render(s, t.TempDir(), configtest.IPv6, "::", s.InstallDir)
			if err != nil {
				t.Fatal(err)
			}
			var v struct {
				Host   string
				Global struct {
					Membership struct {
						Broadcast string `yaml:"broadcastAddress"`
					}
				}
				Services map[string]struct {
					RPC struct {
						Bind string `yaml:"bindOnIP"`
					}
				}
				Persistence struct {
					Datastores map[string]struct {
						SQL struct {
							Address string `yaml:"connectAddr"`
						}
					}
				}
				Frontend string `yaml:"temporalGrpcAddress"`
			}
			if err := yaml.Unmarshal(files["temporal/development.yaml"], &v); err != nil {
				t.Fatal(err)
			}
			c := configtest.NetworkConfig{}
			if role == tspec.UI {
				c.Listeners = []string{v.Host}
				c.AdvertisedEndpoints = []string{v.Frontend}
			} else {
				c.AdvertisedHosts = []string{v.Global.Membership.Broadcast}
				for _, service := range v.Services {
					c.Listeners = append(c.Listeners, service.RPC.Bind)
				}
				for _, store := range v.Persistence.Datastores {
					c.AdvertisedEndpoints = append(c.AdvertisedEndpoints, store.SQL.Address)
				}
			}
			configtest.CheckIPv6NativeConfig(t, c)
		})
	}
}

func TestIPv4NativeConfig(t *testing.T) {
	for _, role := range []string{tspec.Server, tspec.Frontend, tspec.History, tspec.Matching, tspec.Worker, tspec.UI} {
		t.Run(role, func(t *testing.T) {
			s := testSpec()
			s.Component, s.Persistence.Host, s.FrontendAddress = role, configtest.PeerIPv4, configtest.IPv4+":7233"
			_, files, err := render(s, t.TempDir(), configtest.IPv4, "0.0.0.0", s.InstallDir)
			if err != nil {
				t.Fatal(err)
			}
			var v struct {
				Host     string
				Frontend string `yaml:"temporalGrpcAddress"`
				Global   struct {
					Membership struct {
						Broadcast string `yaml:"broadcastAddress"`
					}
				}
				Services map[string]struct {
					RPC struct {
						Bind string `yaml:"bindOnIP"`
					}
				}
				Persistence struct {
					Datastores map[string]struct {
						SQL struct {
							Address string `yaml:"connectAddr"`
						}
					}
				}
			}
			if err := yaml.Unmarshal(files["temporal/development.yaml"], &v); err != nil {
				t.Fatal(err)
			}
			c := configtest.NetworkConfig{}
			if role == tspec.UI {
				c.Listeners, c.AdvertisedEndpoints = []string{v.Host}, []string{v.Frontend}
			} else {
				c.AdvertisedHosts = []string{v.Global.Membership.Broadcast}
				for _, service := range v.Services {
					c.Listeners = append(c.Listeners, service.RPC.Bind)
				}
				for _, store := range v.Persistence.Datastores {
					c.AdvertisedEndpoints = append(c.AdvertisedEndpoints, store.SQL.Address)
				}
			}
			configtest.CheckIPv4NativeConfig(t, c)
		})
	}
}
