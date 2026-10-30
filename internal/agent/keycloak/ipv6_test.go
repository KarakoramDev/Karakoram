// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package keycloak

import (
	"github.com/KarakoramDev/Karakoram/internal/configtest"
	"strings"
	"testing"
)

func TestIPv6NativeConfig(t *testing.T) {
	s := validSpec()
	s.IPv6 = true
	s.Host = configtest.IPv6
	_, files, err := Render(s, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	values := configtest.Settings(string(files["keycloak/keycloak.conf"]), "=")
	configtest.CheckIPv6NativeConfig(t, configtest.NetworkConfig{Listeners: []string{strings.ReplaceAll(values["http-host"], `\:`, ":")}, AdvertisedEndpoints: []string{strings.TrimPrefix(strings.ReplaceAll(values["db-url"], `\:`, ":"), "jdbc:")}})
	if values["http-management-host"] != "127.0.0.1" {
		t.Fatal("local-only management bind widened")
	}
}

func TestIPv4NativeConfig(t *testing.T) {
	s := validSpec()
	s.Host = configtest.IPv4
	_, files, err := Render(s, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	v := configtest.Settings(string(files["keycloak/keycloak.conf"]), "=")
	configtest.CheckIPv4NativeConfig(t, configtest.NetworkConfig{
		Listeners:           []string{strings.ReplaceAll(v["http-host"], `\:`, ":")},
		LocalOnlyListeners:  []string{v["http-management-host"] + ":9000"},
		AdvertisedEndpoints: []string{strings.TrimPrefix(strings.ReplaceAll(v["db-url"], `\:`, ":"), "jdbc:")},
	})
}
