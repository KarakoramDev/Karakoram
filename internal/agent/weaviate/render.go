// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package weaviate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvspec"
)

// KARAKORAM_WEAVIATE_INSTALL_ROOT is a test/development hook containing
// weaviate-<version>/weaviate, analogous to KARAKORAM_RABBITMQ_BIN.
func binary(version string) string {
	if root := os.Getenv("KARAKORAM_WEAVIATE_INSTALL_ROOT"); root != "" {
		return filepath.Join(root, "weaviate-"+version, "weaviate")
	}
	return wvspec.BinaryPath(version)
}

func render(dir string, s wvspec.MemberSpec, c agentapi.MemberConfig) (agent.ProcessSpec, map[string][]byte) {
	tlsDir := filepath.Join(dir, "karakoram", "tls")
	files := map[string][]byte{}
	args := []string{"--host", s.BindHost, "--port", strconv.Itoa(s.Ports.HTTP), "--scheme", "http"}
	env := map[string]string{
		"PERSISTENCE_DATA_PATH": filepath.Join(dir, "weaviate"),
		"CLUSTER_HOSTNAME":      s.NodeName, "CLUSTER_BIND_ADDR": s.BindHost,
		"CLUSTER_GOSSIP_BIND_PORT": strconv.Itoa(s.Ports.Gossip), "CLUSTER_DATA_BIND_PORT": strconv.Itoa(s.Ports.Data),
		"CLUSTER_JOIN": strings.Join(s.GossipJoin, ","), "RAFT_JOIN": strings.Join(s.RaftJoin, ","),
		"RAFT_BOOTSTRAP_EXPECT": strconv.Itoa(s.BootstrapExpect), "RAFT_PORT": strconv.Itoa(s.Ports.Raft), "RAFT_INTERNAL_RPC_PORT": strconv.Itoa(s.Ports.RaftRPC),
		"GRPC_PORT": strconv.Itoa(s.Ports.GRPC),
		"AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED": "false", "AUTHENTICATION_APIKEY_ENABLED": "true",
		"AUTHENTICATION_APIKEY_ALLOWED_KEYS": strings.Join(s.AllowedKeys, ","), "AUTHENTICATION_APIKEY_USERS": s.RootUser,
		"AUTHORIZATION_RBAC_ENABLED": "true", "AUTHORIZATION_RBAC_ROOT_USERS": s.RootUser, "AUTHENTICATION_DB_USERS_ENABLED": "true",
		"CLUSTER_BASIC_AUTH_USERNAME": s.ClusterUsername, "CLUSTER_BASIC_AUTH_PASSWORD": s.ClusterPassword,
		"REPLICA_MOVEMENT_ENABLED": "true", "REPLICATION_MINIMUM_FACTOR": strconv.Itoa(s.ReplicationMinimumFactor),
		"API_BASED_MODULES_DISABLED": "true", "QUERY_DEFAULTS_LIMIT": strconv.Itoa(s.QueryDefaultsLimit),
		"DISK_USE_WARNING_PERCENTAGE": strconv.Itoa(s.DiskWarningPercent), "DISK_USE_READONLY_PERCENTAGE": strconv.Itoa(s.DiskReadonlyPercent),
		"LIMIT_RESOURCES": "false", "GOMEMLIMIT": strconv.FormatInt(s.MemoryLimitMiB, 10) + "MiB", "GOMAXPROCS": strconv.Itoa(s.GOMAXPROCS),
		"PROMETHEUS_MONITORING_ENABLED": "true", "PROMETHEUS_MONITORING_PORT": strconv.Itoa(s.Ports.Metrics),
		"GO_PROFILING_DISABLE": "true", "DISABLE_TELEMETRY": "true",
	}
	env["CLUSTER_ADVERTISE_PORT"] = strconv.Itoa(s.Ports.Gossip)
	// KARAKORAM_WEAVIATE_LOCAL_CLUSTER is a test/development hook for
	// multiple native members sharing one network namespace. Internal native
	// data/gRPC/metrics listeners cannot bind individual loopback addresses.
	if os.Getenv("KARAKORAM_WEAVIATE_LOCAL_CLUSTER") == "true" {
		// Upstream's native localhost mode resolves each remote Raft RPC
		// listener as its consensus port + 1. The wire carries RPC ports.
		env["CLUSTER_IN_LOCALHOST"] = "true"
		join := make([]string, 0, len(s.RaftJoin))
		for _, address := range s.RaftJoin {
			host, port, _ := net.SplitHostPort(address)
			rpc, _ := strconv.Atoi(port)
			join = append(join, net.JoinHostPort(host, strconv.Itoa(rpc-1)))
		}
		env["RAFT_JOIN"] = strings.Join(join, ",")
	}
	for _, name := range wvspec.CommunityDisabledEnv {
		env[name] = "false"
	}
	if !s.AsyncReplication {
		env["ASYNC_REPLICATION_DISABLED"] = "true"
	}
	modules := make([]string, 0, len(s.Modules))
	for _, m := range s.Modules {
		if m != "backup-s3" || s.Backup != nil {
			modules = append(modules, m)
		}
	}
	sort.Strings(modules)
	env["ENABLE_MODULES"] = strings.Join(modules, ",")
	for k, v := range s.InferenceEndpoints {
		env[k] = v
	}
	if b := s.Backup; b != nil {
		env["BACKUP_S3_ENDPOINT"] = b.Endpoint
		env["BACKUP_S3_BUCKET"] = b.Bucket
		env["BACKUP_S3_PATH"] = b.Path
		env["BACKUP_S3_USE_SSL"] = strconv.FormatBool(b.UseSSL)
		env["AWS_REGION"] = b.Region
		env["AWS_ACCESS_KEY_ID"] = b.AccessKey
		env["AWS_SECRET_ACCESS_KEY"] = b.SecretKey
	}
	if s.TLS {
		files["tls/weaviate.crt"] = []byte(c.TLS.CertPEM)
		files["tls/weaviate.key"] = []byte(c.TLS.KeyPEM)
		files["tls/weaviate-ca.crt"] = []byte(c.TLS.CAPEM)
		cert, key := filepath.Join(tlsDir, "weaviate.crt"), filepath.Join(tlsDir, "weaviate.key")
		args = []string{"--scheme", "https", "--tls-host", s.BindHost, "--tls-port", strconv.Itoa(s.Ports.HTTP), "--tls-certificate", cert, "--tls-key", key}
		env["GRPC_CERT_FILE"] = cert
		env["GRPC_KEY_FILE"] = key
	}
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	values := make([]string, 0, len(keys))
	for _, k := range keys {
		values = append(values, k+"="+env[k])
	}
	return agent.ProcessSpec{Binary: binary(s.Version), Args: args, Env: values, Dir: dir, StopSignal: syscall.SIGTERM}, files
}

func fingerprint(s wvspec.MemberSpec, p agent.ProcessSpec, files map[string][]byte) string {
	data, _ := json.Marshal(struct {
		Version, RestartToken string
		Process               agent.ProcessSpec
		Files                 map[string][]byte
	}{s.Version, s.RestartToken, p, files})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
