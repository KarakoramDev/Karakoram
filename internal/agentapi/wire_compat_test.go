// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package agentapi_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
	"github.com/KarakoramDev/Karakoram/internal/engine/keycloak/kcspec"
)

// These schemas intentionally omit the new family fields. They reproduce the
// ordinary json.Unmarshal decoding used by the pre-change Keycloak and Elastic
// agents; Neo4j's strict decoder is exercised against real CP output in its
// provider compatibility regression instead.
type legacyKeycloakSpec struct {
	Version              string `json:"version"`
	Host                 string `json:"host"`
	Port                 int    `json:"port"`
	Database             string `json:"database"`
	Username             string `json:"username"`
	Password             string `json:"password"`
	CAPEM                string `json:"caPEM"`
	Hostname             string `json:"hostname,omitempty"`
	ProxyHeaders         string `json:"proxyHeaders,omitempty"`
	AdminPassword        string `json:"adminPassword"`
	CredentialGeneration int64  `json:"credentialGeneration"`
}

func TestLegacyKeycloakDecoderAcceptsFamilyPolicy(t *testing.T) {
	want := legacyKeycloakSpec{Version: kcspec.Version, Host: "database", Port: 5432, Database: "keycloak", Username: "binding", Password: "secret", CAPEM: "ca", Hostname: "https://login.example.test", ProxyHeaders: "forwarded", AdminPassword: "admin-secret", CredentialGeneration: 7}
	current := kcspec.Spec{Version: want.Version, Host: want.Host, Port: want.Port, Database: want.Database, Username: want.Username, Password: want.Password, CAPEM: want.CAPEM, Hostname: want.Hostname, ProxyHeaders: want.ProxyHeaders, AdminPassword: want.AdminPassword, CredentialGeneration: want.CredentialGeneration, IPv6: true}
	if err := current.Validate(); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"ipv6":true`)) {
		t.Fatal("test did not exercise the new field")
	}
	var got legacyKeycloakSpec
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal("legacy Keycloak decoder rejected additive field", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("family extension changed legacy credential/database fields")
	}
}

type legacyElasticSpec struct {
	Component     string                       `json:"component"`
	Version       string                       `json:"version"`
	HeapMB        int                          `json:"heapMB,omitempty"`
	CAPEM         string                       `json:"caPEM"`
	RestartToken  string                       `json:"restartToken"`
	Elasticsearch *agentspec.ElasticsearchSpec `json:"elasticsearch,omitempty"`
	Kibana        *agentspec.KibanaSpec        `json:"kibana,omitempty"`
	Logstash      *agentspec.LogstashSpec      `json:"logstash,omitempty"`
	APM           *agentspec.APMSpec           `json:"apm,omitempty"`
}

func TestLegacyElasticDecoderAcceptsFamilyPolicy(t *testing.T) {
	for _, component := range []string{agentspec.ComponentElasticsearch, agentspec.ComponentKibana, agentspec.ComponentLogstash, agentspec.ComponentAPM} {
		t.Run(component, func(t *testing.T) {
			want := legacyElasticSpec{Component: component, Version: "9.5.5", HeapMB: 768, CAPEM: "ca", RestartToken: "token"}
			switch component {
			case agentspec.ComponentElasticsearch:
				want.Elasticsearch = &agentspec.ElasticsearchSpec{ClusterName: "test", NodeName: "member", Roles: []string{"master", "data"}}
			case agentspec.ComponentKibana:
				want.Kibana = &agentspec.KibanaSpec{ESHosts: []string{"https://peer:9200"}}
			case agentspec.ComponentLogstash:
				want.Logstash = &agentspec.LogstashSpec{ESHosts: []string{"https://peer:9200"}}
			case agentspec.ComponentAPM:
				want.APM = &agentspec.APMSpec{ESHosts: []string{"https://peer:9200"}}
			}
			current := agentspec.Spec{Component: want.Component, Version: want.Version, HeapMB: want.HeapMB, CAPEM: want.CAPEM, RestartToken: want.RestartToken, Elasticsearch: want.Elasticsearch, Kibana: want.Kibana, Logstash: want.Logstash, APM: want.APM, IPv4: true}
			raw, err := json.Marshal(current)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(raw, []byte(`"ipv4":true`)) {
				t.Fatal("test did not exercise the new field")
			}
			var got legacyElasticSpec
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal("legacy Elastic decoder rejected additive field", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatal("family extension changed legacy component fields")
			}
		})
	}
}
