// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package weaviate

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestClusterRenderEnvironment(t *testing.T) {
	t.Setenv("KARAKORAM_WEAVIATE_LOCAL_CLUSTER", "")
	s := testSpec()
	s.BindHost = "::"
	s.BootstrapExpect = 3
	s.RaftJoin = []string{"node00:8301", "node01:8301", "node02:8301"}
	s.GossipJoin = []string{"node00:7946", "node01:7946", "node02:7946"}
	s.ReplicationMinimumFactor = 3
	p, _ := render("/data", s, config(s))
	expected := strings.Split(`API_BASED_MODULES_DISABLED=true
AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=false
AUTHENTICATION_APIKEY_ALLOWED_KEYS=root-secret
AUTHENTICATION_APIKEY_ENABLED=true
AUTHENTICATION_APIKEY_USERS=root
AUTHENTICATION_DB_USERS_ENABLED=true
AUTHORIZATION_RBAC_ENABLED=true
AUTHORIZATION_RBAC_ROOT_USERS=root
BACKUP_DEDUPE_ENABLED=false
CLUSTER_ADVERTISE_PORT=27946
CLUSTER_BASIC_AUTH_PASSWORD=cluster-secret
CLUSTER_BASIC_AUTH_USERNAME=cluster
CLUSTER_BIND_ADDR=::
CLUSTER_DATA_BIND_PORT=27947
CLUSTER_GOSSIP_BIND_PORT=27946
CLUSTER_HOSTNAME=node00
CLUSTER_JOIN=node00:7946,node01:7946,node02:7946
DISABLE_TELEMETRY=true
DISK_USE_READONLY_PERCENTAGE=90
DISK_USE_WARNING_PERCENTAGE=80
ENABLE_MODULES=
GOMEMLIMIT=256MiB
GOMAXPROCS=2
GO_PROFILING_DISABLE=true
GRPC_PORT=25051
LIMIT_RESOURCES=false
NAMESPACES_ENABLED=false
PERSISTENCE_DATA_PATH=/data/weaviate
PROMETHEUS_MONITORING_ENABLED=true
PROMETHEUS_MONITORING_PORT=22112
QUERY_DEFAULTS_LIMIT=25
RAFT_BOOTSTRAP_EXPECT=3
RAFT_INTERNAL_RPC_PORT=28301
RAFT_JOIN=node00:8301,node01:8301,node02:8301
RAFT_PORT=28300
REPLICA_MOVEMENT_ENABLED=true
REPLICATION_MINIMUM_FACTOR=3
SELF_RECOVERY_ENABLED=false`, "\n")
	sort.Strings(expected)
	if !reflect.DeepEqual(p.Env, expected) {
		t.Fatal("rendered environment differs", p.Env)
	}
	redacted := strings.NewReplacer("root-secret", "<redacted>", "cluster-secret", "<redacted>").Replace(strings.Join(p.Env, "\n"))
	t.Logf("cluster args: %q\ncluster environment:\n%s", p.Args, redacted)
}
func TestLocalClusterRenderHook(t *testing.T) {
	s := testSpec()
	s.RaftJoin = []string{"node00:28301", "node01:28401", "node02:28501"}
	t.Setenv("KARAKORAM_WEAVIATE_LOCAL_CLUSTER", "true")
	p, _ := render("/data", s, config(s))
	env := strings.Join(p.Env, "\n")
	if !strings.Contains(env, "CLUSTER_IN_LOCALHOST=true") || !strings.Contains(env, "RAFT_JOIN=node00:28300,node01:28400,node02:28500") {
		t.Fatal(env)
	}
	t.Setenv("KARAKORAM_WEAVIATE_LOCAL_CLUSTER", "")
	p, _ = render("/data", s, config(s))
	env = strings.Join(p.Env, "\n")
	if strings.Contains(env, "CLUSTER_IN_LOCALHOST") || !strings.Contains(env, "RAFT_JOIN=node00:28301,node01:28401,node02:28501") {
		t.Fatal(env)
	}
}

func TestRenderBindHostAddressFamilies(t *testing.T) {
	t.Setenv("KARAKORAM_WEAVIATE_LOCAL_CLUSTER", "")
	for _, bindHost := range []string{"::", "0.0.0.0"} {
		for _, tls := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/TLS=%t", bindHost, tls), func(t *testing.T) {
				s := testSpec()
				s.BindHost, s.TLS = bindHost, tls
				process, _ := render("/data", s, config(s))
				wantArgs := []string{"--host", bindHost, "--port", "28080", "--scheme", "http"}
				if tls {
					wantArgs = []string{"--scheme", "https", "--tls-host", bindHost, "--tls-port", "28080", "--tls-certificate", "/data/karakoram/tls/weaviate.crt", "--tls-key", "/data/karakoram/tls/weaviate.key"}
				}
				if !reflect.DeepEqual(process.Args, wantArgs) {
					t.Errorf("args=%q; want %q", process.Args, wantArgs)
				}
				bindEntries := 0
				for _, entry := range process.Env {
					if strings.HasPrefix(entry, "CLUSTER_BIND_ADDR=") {
						bindEntries++
						if entry != "CLUSTER_BIND_ADDR="+bindHost {
							t.Errorf("cluster bind environment=%q; want address %q", entry, bindHost)
						}
					}
				}
				if bindEntries != 1 {
					t.Errorf("got %d CLUSTER_BIND_ADDR entries, want exactly one", bindEntries)
				}
			})
		}
	}
}
