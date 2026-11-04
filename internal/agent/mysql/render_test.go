// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestMemorySizingOperatorOverrides(t *testing.T) {
	for _, topology := range []string{myspec.TopologySingle, myspec.TopologyGroup, myspec.TopologyCluster} {
		for _, pool := range []string{"134217728", "1073741824"} {
			s := coreTestSpec()
			s.Topology = topology
			s.BufferPoolBytes, s.MaxConnections = 768<<20, 200
			if topology == myspec.TopologyGroup {
				s.GroupReplication = testGroupSpec().GroupReplication
			}
			s.Parameters = map[string]string{"innodb_buffer_pool_size": pool, "max_connections": "30"}
			config := renderConfig(s, dataPaths(t.TempDir()))
			for key, value := range s.Parameters {
				if !strings.Contains(config, "\n"+key+"="+optionQuote(value)+"\n") {
					t.Fatalf("%s: override %s=%s lost", topology, key, value)
				}
			}
		}
	}
}

func TestGroupStartupWriteFence(t *testing.T) {
	s := coreTestSpec()
	s.Topology = myspec.TopologyGroup
	s.GroupReplication = testGroupSpec().GroupReplication
	p := dataPaths("/data")
	for _, tc := range []struct {
		name  string
		rekey bool
	}{
		{name: "fresh", rekey: true},
		{name: "initialized-restart", rekey: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conf := renderConfig(s, p)
			if !strings.Contains(conf, "\nsuper_read_only=\"ON\"\n") {
				t.Fatal("GR server can admit writes before joining the group")
			}
			if !strings.Contains(conf, "\ngroup_replication_start_on_boot=\"OFF\"\n") {
				t.Fatal("GR startup must remain under managed join control")
			}
			startup := renderStartup(s, p, tc.rekey)
			if tc.rekey {
				if !strings.Contains(startup, "SET GLOBAL super_read_only=OFF;\n") {
					t.Fatal("account re-key no longer temporarily lifts the startup fence")
				}
				if !strings.HasSuffix(startup, "SET GLOBAL super_read_only=ON;\n") {
					t.Fatal("account re-key does not restore the fence before admitting clients")
				}
			} else {
				if strings.Contains(startup, "ALTER USER") || strings.Contains(startup, "IDENTIFIED BY") {
					t.Fatal("unchanged credentials must not re-key accounts")
				}
				if !strings.Contains(startup, "REVOKE IF EXISTS FILE ON *.* FROM 'admin'@'%' IGNORE UNKNOWN USER;") || !strings.HasSuffix(startup, "SET GLOBAL super_read_only=ON;\n") {
					t.Fatal("startup privilege policy must restore the GR fence before admitting clients")
				}
			}
		})
	}
}

func TestStartupWriteFenceDoesNotFenceSingleOrGalera(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec myspec.Spec
	}{
		{name: "single", spec: coreTestSpec()},
		{name: "galera", spec: testGaleraSpec()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := dataPaths("/data")
			conf := renderConfig(tc.spec, p)
			for _, option := range []string{"\nread_only=", "\nsuper_read_only="} {
				if strings.Contains(conf, option) {
					t.Fatalf("non-GR startup unexpectedly overrides write availability: %s", option)
				}
			}
			for _, rekey := range []bool{true, false} {
				startup := renderStartup(tc.spec, p, rekey)
				if strings.Contains(startup, "SET GLOBAL super_read_only=ON;") || strings.Contains(startup, "SET GLOBAL read_only=ON;") {
					t.Fatalf("rekey=%t: non-GR startup installs a GR write fence", rekey)
				}
				if !rekey && (strings.Contains(startup, "ALTER USER") || !strings.Contains(startup, "REVOKE IF EXISTS FILE ON *.* FROM 'admin'@'%' IGNORE UNKNOWN USER;")) {
					t.Fatal("unchanged credentials must enforce FILE policy without re-keying")
				}
			}
		})
	}
}

func TestSQLCannotReadRuntimeFiles(t *testing.T) {
	s := coreTestSpec()
	s.Parameters = map[string]string{"secure_file_priv": ""}
	p := dataPaths(t.TempDir())
	if !strings.Contains(renderConfig(s, p), "\nsecure_file_priv=\"NULL\"\n") {
		t.Fatal("SQL file access is not disabled independently of parameters")
	}
	if !strings.Contains(renderStartup(s, p, true), "REVOKE FILE ON *.* FROM 'admin'@'%';") {
		t.Fatal("connection administrator retained FILE authority")
	}
}
