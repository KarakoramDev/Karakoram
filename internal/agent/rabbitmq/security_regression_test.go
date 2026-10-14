// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestSpecRejectsImplicitManagementDependencies(t *testing.T) {
	for _, base := range []string{"rabbitmq_stream", "rabbitmq_shovel", "rabbitmq_federation"} {
		t.Run(base, func(t *testing.T) {
			s := testSpec()
			s.AMQPPort, s.ManagementPort = 0, 0
			s.AMQPSPort, s.ManagementTLSPort = 5671, 15671
			s.Plugins = append(s.Plugins, base+"_management")
			c := memberConfig(s)
			c.TLS = agentapi.TLSConfig{Mode: "requireTLS", CAPEM: "CA", CertPEM: "CERT", KeyPEM: "KEY"}
			if _, err := spec(c); err == nil || !strings.Contains(err.Error(), "requires") || !strings.Contains(err.Error(), base) {
				t.Fatalf("implicit dependency accepted: %v", err)
			}
			s.Plugins = append(s.Plugins, base)
			if base == "rabbitmq_stream" {
				s.StreamTLSPort = 5551
			}
			c.EngineSpec = memberConfig(s).EngineSpec
			if _, err := spec(c); err != nil {
				t.Fatal(err)
			}
			if base == "rabbitmq_stream" {
				files, _ := render("/data", s, c)
				config := files[0].content
				if !strings.Contains(config, "stream.listeners.tcp = none\n") || !strings.Contains(config, "stream.listeners.ssl.default = 5551\n") {
					t.Fatal("explicit stream dependency lacks TLS-only listeners", config)
				}
			}
		})
	}
}

func TestBrokerDataDirectoryPermissions(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := "new"
		if existing {
			name = "existing"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "rabbitmq")
			if existing {
				if err := os.Mkdir(path, 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(path, 0777); err != nil {
					t.Fatal(err)
				}
			}
			if err := prepareDataDir(dir); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if !info.IsDir() || info.Mode().Perm() != 0700 {
				t.Fatalf("broker data directory mode = %v, want 0700", info.Mode())
			}
		})
	}
	t.Run("file blocks startup", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "rabbitmq"), []byte("not a directory"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := prepareDataDir(dir); err == nil {
			t.Fatal("non-directory broker path accepted")
		}
	})
}

func TestRequireTLSRejectsPlaintextManagement(t *testing.T) {
	s := testSpec()
	s.AMQPPort = 0
	s.AMQPSPort, s.ManagementTLSPort = 5671, 15671
	c := memberConfig(s)
	c.TLS = agentapi.TLSConfig{Mode: "requireTLS", CAPEM: "CA", CertPEM: "CERT", KeyPEM: "KEY"}
	if _, err := spec(c); err == nil || !strings.Contains(err.Error(), "plaintext") {
		t.Fatalf("plaintext public-management bypass accepted: %v", err)
	}
	s.ManagementPort = 0
	c.EngineSpec = memberConfig(s).EngineSpec
	if _, err := spec(c); err != nil {
		t.Fatal(err)
	}
}

func TestGeneratedErlangTLSClientsUseTLS12(t *testing.T) {
	for _, stream := range []bool{false, true} {
		s := testSpec()
		s.AMQPPort, s.ManagementPort = 0, 0
		s.AMQPSPort, s.ManagementTLSPort = 5671, 15671
		s.InterNodeTLS = true
		if stream {
			s.Plugins = append(s.Plugins, "rabbitmq_stream")
			s.StreamTLSPort = 5551
		}
		c := memberConfig(s)
		c.TLS = agentapi.TLSConfig{Mode: "requireTLS", CAPEM: "CA", CertPEM: "CERT", KeyPEM: "KEY"}
		files, env := render("/data", s, c)
		contents := map[string]string{}
		for _, f := range files {
			contents[f.name] = f.content
		}
		const versions = "{versions, ['tlsv1.2']}"
		_, client, ok := strings.Cut(contents["inter_node_tls.config"], "{client,")
		if !ok || !strings.Contains(client, versions) || !strings.Contains(client, "{verify,verify_peer}") {
			t.Fatal("distribution TLS client lost restriction or peer verification")
		}
		advanced := contents["advanced.config"]
		for _, key := range []string{"replication_server_ssl_options", "replication_client_ssl_options"} {
			_, options, ok := strings.Cut(advanced, "{"+key+",")
			options, _, _ = strings.Cut(options, "]")
			if !ok || !strings.Contains(options, "'tlsv1.2'") || !strings.Contains(options, "{verify,verify_peer}") {
				t.Fatalf("Osiris %s lost TLS 1.2 or peer verification", key)
			}
		}
		if strings.Count(advanced, versions) != 2 {
			t.Fatal("Osiris replication must constrain both endpoints")
		}
		for _, key := range []string{"RABBITMQ_CTL_ERL_ARGS=", "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="} {
			found := false
			for _, entry := range env {
				if strings.HasPrefix(entry, key) && strings.Contains(entry, "-ssl_dist_optfile /data/karakoram/inter_node_tls.config") {
					found = true
				}
			}
			if !found {
				t.Fatalf("%s does not use the restricted distribution config", key)
			}
		}
		if strings.Contains(contents["rabbitmq.conf"], "versions") {
			t.Fatal("temporary client mitigation changed public TLS listener versions")
		}
	}
}
