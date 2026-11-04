// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/configtest"
	"testing"
)

func TestIPv6NativeConfig(t *testing.T) {
	s, c := testConfig()
	s.ListenAddress = "::"
	s.APIAddr = "https://[" + configtest.IPv6 + "]:8200"
	s.ClusterAddr = "https://[" + configtest.IPv6 + "]:8201"
	s.Storage.RetryJoin[0].LeaderAPIAddr = "https://[" + configtest.PeerIPv6 + "]:8200"
	files, err := render(t.TempDir(), s, c)
	if err != nil {
		t.Fatal(err)
	}
	configtest.CheckIPv6Rendered(t, files, nil, nil, map[string]string{
		`"address": "127.0.0.1:8210"`:      "node-local metrics endpoint",
		`"cluster_address": "127.0.0.1:0"`: "disabled cluster transport for metrics/recovery",
	})
	var v struct {
		APIAddr     string `json:"api_addr"`
		ClusterAddr string `json:"cluster_addr"`
		Listener    []struct {
			TCP struct {
				Address        string `json:"address"`
				ClusterAddress string `json:"cluster_address"`
			} `json:"tcp"`
		} `json:"listener"`
		Storage struct {
			Raft struct {
				RetryJoin []struct {
					LeaderAPIAddr string `json:"leader_api_addr"`
				} `json:"retry_join"`
			} `json:"raft"`
		} `json:"storage"`
	}
	if err := json.Unmarshal(files["openbao/server.json"], &v); err != nil {
		t.Fatal(err)
	}
	endpoints := []string{v.APIAddr, v.ClusterAddr}
	for _, peer := range v.Storage.Raft.RetryJoin {
		endpoints = append(endpoints, peer.LeaderAPIAddr)
	}
	configtest.CheckIPv6NativeConfig(t, configtest.NetworkConfig{Listeners: []string{v.Listener[0].TCP.Address, v.Listener[0].TCP.ClusterAddress}, AdvertisedEndpoints: endpoints})
}

func TestIPv4NativeConfig(t *testing.T) {
	s, c := testConfig()
	s.ListenAddress = "0.0.0.0"
	s.APIAddr, s.ClusterAddr = "https://"+configtest.IPv4+":8200", "https://"+configtest.IPv4+":8201"
	s.Storage.RetryJoin[0].LeaderAPIAddr = "https://" + configtest.PeerIPv4 + ":8200"
	files, err := render(t.TempDir(), s, c)
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		API      string `json:"api_addr"`
		Cluster  string `json:"cluster_addr"`
		Listener []struct {
			TCP struct {
				Address string `json:"address"`
				Cluster string `json:"cluster_address"`
			} `json:"tcp"`
		} `json:"listener"`
	}
	if err := json.Unmarshal(files["openbao/server.json"], &v); err != nil {
		t.Fatal(err)
	}
	configtest.CheckIPv4NativeConfig(t, configtest.NetworkConfig{Listeners: []string{v.Listener[0].TCP.Address, v.Listener[0].TCP.Cluster}, AdvertisedEndpoints: []string{v.API, v.Cluster}})
}
