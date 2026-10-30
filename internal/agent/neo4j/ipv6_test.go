// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/configtest"
)

func localFamilyListener(t *testing.T, ipv6Available bool) string {
	t.Helper()
	var probe net.Listener
	selected := selectListenAddress(func(network, address string) (net.Listener, error) {
		if network != "tcp6" || address != "[::]:0" {
			t.Fatal("wrong local socket probe", network, address)
		}
		if !ipv6Available {
			return nil, syscall.EAFNOSUPPORT
		}
		var err error
		probe, err = net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = probe.Close() })
		return probe, nil
	})
	if probe != nil {
		if err := probe.(*net.TCPListener).SetDeadline(time.Now()); !errors.Is(err, net.ErrClosed) {
			t.Fatal("local socket probe was not closed", err)
		}
	}
	want := "0.0.0.0"
	if ipv6Available {
		want = "::"
	}
	if selected != want {
		t.Fatal("wrong agent-local listener", selected, want)
	}
	return selected
}

func TestIPv6NativeConfig(t *testing.T) {
	s := coreTestSpec(t)
	s.AdvertisedHost = configtest.IPv6
	files, err := render(s, "/volume/karakoram/neo4j", "", false, 7687, 7473, localFamilyListener(t, true))
	if err != nil {
		t.Fatal(err)
	}
	configtest.CheckIPv6Rendered(t, files, nil, nil, nil)
	v := configtest.Settings(string(files["neo4j/conf/neo4j.conf"]), "=")
	configtest.CheckIPv6NativeConfig(t, configtest.NetworkConfig{Listeners: []string{v["server.default_listen_address"], v["server.bolt.listen_address"], v["server.https.listen_address"]}, AdvertisedHosts: []string{v["server.default_advertised_address"]}, AdvertisedEndpoints: []string{v["server.bolt.advertised_address"], v["server.https.advertised_address"]}})
}

func TestIPv4NativeConfig(t *testing.T) {
	s := coreTestSpec(t)
	s.AdvertisedHost = configtest.IPv4
	files, err := render(s, "/volume/karakoram/neo4j", "", false, 7687, 7473, localFamilyListener(t, false))
	if err != nil {
		t.Fatal(err)
	}
	v := configtest.Settings(string(files["neo4j/conf/neo4j.conf"]), "=")
	configtest.CheckIPv4NativeConfig(t, configtest.NetworkConfig{Listeners: []string{v["server.default_listen_address"], v["server.bolt.listen_address"], v["server.https.listen_address"]}, AdvertisedHosts: []string{v["server.default_advertised_address"]}, AdvertisedEndpoints: []string{v["server.bolt.advertised_address"], v["server.https.advertised_address"]}})
	for _, available := range []bool{false, true} {
		files, err := render(s, "/volume/karakoram/neo4j", "", true, 17687, 17473, localFamilyListener(t, available))
		if err != nil {
			t.Fatal(err)
		}
		v := configtest.Settings(string(files["neo4j/recovery-conf/neo4j.conf"]), "=")
		configtest.CheckIPv4NativeConfig(t, configtest.NetworkConfig{LocalOnlyListeners: []string{v["server.bolt.listen_address"], v["server.https.listen_address"]}})
	}
}

func TestEngineSelectsListenerLocally(t *testing.T) {
	if got, want := New().listenerAddress(), selectListenAddress(net.Listen); got != want {
		t.Fatal("engine did not select local socket policy", got, want)
	}
	e := &Engine{listenAddress: "127.0.0.1"}
	if e.listenerAddress() != "127.0.0.1" {
		t.Fatal("trusted native-test override was replaced")
	}
}
