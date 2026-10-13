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
