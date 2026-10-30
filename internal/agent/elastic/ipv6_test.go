// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package elastic

import (
	"github.com/KarakoramDev/Karakoram/internal/configtest"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
	"gopkg.in/yaml.v3"
	"strings"
	"testing"
)

func TestIPv6NativeConfig(t *testing.T) {
	s := esSpec()
	s.Elasticsearch.NodeName = configtest.Member
	s.Elasticsearch.SeedHosts = []string{"[" + configtest.IPv6 + "]:9300", "[" + configtest.PeerIPv6 + "]:9300"}
	files, process, err := render(s, t.TempDir(), "/opt/elasticsearch", "test", configtest.Member)
	if err != nil {
		t.Fatal(err)
	}
	configtest.CheckIPv6Rendered(t, files, process.Args, process.Env, nil)
	var v map[string]any
	if err := yaml.Unmarshal(files["config/elasticsearch.yml"], &v); err != nil {
		t.Fatal(err)
	}
	configtest.CheckIPv6NativeConfig(t, configtest.NetworkConfig{Listeners: []string{v["network.host"].(string)}, AdvertisedHosts: []string{v["network.publish_host"].(string)}, AdvertisedEndpoints: strings.Fields(string(files["config/unicast_hosts.txt"]))})
	for _, component := range []string{agentspec.ComponentKibana, agentspec.ComponentAPM, agentspec.ComponentLogstash} {
		t.Run(component, func(t *testing.T) {
			spec := agentspec.Spec{Component: component, Version: s.Version, CAPEM: "ca"}
			switch component {
			case agentspec.ComponentKibana:
				spec.Kibana = &agentspec.KibanaSpec{ESHosts: []string{"https://[" + configtest.IPv6 + "]:9200"}}
			case agentspec.ComponentAPM:
				spec.APM = &agentspec.APMSpec{ESHosts: []string{"https://[" + configtest.IPv6 + "]:9200"}}
			case agentspec.ComponentLogstash:
				spec.Logstash = &agentspec.LogstashSpec{ESHosts: []string{"https://[" + configtest.IPv6 + "]:9200"}}
			}
			files, process, err := render(spec, t.TempDir(), "/opt/elastic", "test", configtest.Member)
			if err != nil {
				t.Fatal(err)
			}
			configtest.CheckIPv6Rendered(t, files, process.Args, process.Env, map[string]string{
				"http.host: 127.0.0.1":     "APM node-local monitoring API",
				"api.http.host: 127.0.0.1": "Logstash node-local monitoring API",
			})
			if len(files) == 0 {
				t.Fatal("no native component config")
			}
			var values map[string]any
			if err := yaml.Unmarshal(files["config/"+component+".yml"], &values); err != nil {
				t.Fatal(err)
			}
			c := configtest.NetworkConfig{AdvertisedEndpoints: []string{"https://[" + configtest.IPv6 + "]:9200"}}
			switch component {
			case agentspec.ComponentKibana:
				c.Listeners = []string{values["server.host"].(string)}
			case agentspec.ComponentAPM:
				c.Listeners = []string{values["apm-server.host"].(string)}
			case agentspec.ComponentLogstash:
				pipeline := string(files["config/pipelines/main.conf"])
				for _, port := range []string{"5044", "8080"} {
					if !strings.Contains(pipeline, `host => "::" port => `+port) {
						t.Fatal("default pipeline lacks IPv6 listener")
					}
				}
				c.Listeners = []string{"[::]:5044", "[::]:8080"}
			}
			configtest.CheckIPv6NativeConfig(t, c)
			// Every Elastic component's peer URLs must retain bracketed IPv6.
			found := false
			for _, data := range files {
				if strings.Contains(string(data), "https://["+configtest.IPv6+"]:9200") {
					found = true
				}
			}
			if !found {
				t.Fatal("component lost IPv6 Elasticsearch upstream")
			}
		})
	}
}

func TestIPv4NativeConfig(t *testing.T) {
	for _, component := range []string{agentspec.ComponentElasticsearch, agentspec.ComponentKibana, agentspec.ComponentAPM, agentspec.ComponentLogstash} {
		t.Run(component, func(t *testing.T) {
			s := esSpec()
			s.Component, s.IPv4 = component, true
			s.Elasticsearch.NodeName = configtest.Member
			s.Elasticsearch.SeedHosts = []string{configtest.IPv4 + ":9300", configtest.PeerIPv4 + ":9300"}
			hosts := []string{"https://" + configtest.IPv4 + ":9200"}
			s.Kibana = &agentspec.KibanaSpec{ESHosts: hosts}
			s.APM = &agentspec.APMSpec{ESHosts: hosts}
			s.Logstash = &agentspec.LogstashSpec{ESHosts: hosts}
			files, _, err := render(s, t.TempDir(), "/opt/elastic", "test", configtest.Member)
			if err != nil {
				t.Fatal(err)
			}
			var v map[string]any
			if err := yaml.Unmarshal(files["config/"+component+".yml"], &v); err != nil {
				t.Fatal(err)
			}
			c := configtest.NetworkConfig{AdvertisedEndpoints: hosts}
			switch component {
			case agentspec.ComponentElasticsearch:
				c.Listeners = []string{v["network.host"].(string)}
				c.AdvertisedHosts = []string{v["network.publish_host"].(string)}
				c.AdvertisedEndpoints = strings.Fields(string(files["config/unicast_hosts.txt"]))
			case agentspec.ComponentKibana:
				c.Listeners = []string{v["server.host"].(string)}
			case agentspec.ComponentAPM:
				c.Listeners = []string{v["apm-server.host"].(string)}
			case agentspec.ComponentLogstash:
				pipeline := string(files["config/pipelines/main.conf"])
				for _, port := range []string{"5044", "8080"} {
					if !strings.Contains(pipeline, `host => "0.0.0.0" port => `+port) {
						t.Fatal("IPv4 pipeline requires an IPv6 socket")
					}
				}
				c.Listeners = []string{"0.0.0.0:5044", "0.0.0.0:8080"}
			}
			configtest.CheckIPv4NativeConfig(t, c)
		})
	}
}
