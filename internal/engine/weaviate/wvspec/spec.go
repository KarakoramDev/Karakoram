// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package wvspec defines the encrypted Weaviate member wire contract.
// Changes must be additive: never rename JSON fields or change their meaning.
// New optional fields must preserve omitted-field behavior for retained members.
// Configuration rendering belongs to the agent, not this package.
package wvspec

import (
	"fmt"
	"net"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
)

const (
	Engine                 = "weaviate"
	Process                = "weaviate"
	DefaultHTTPPort        = 8080
	DefaultGRPCPort        = 50051
	DefaultGossipPort      = 7946
	DefaultDataPort        = 7947
	DefaultRaftPort        = 8300
	DefaultRaftRPCPort     = 8301
	DefaultMetricsPort     = 2112
	NamespacesEnabledEnv   = "NAMESPACES_ENABLED"
	SelfRecoveryEnabledEnv = "SELF_RECOVERY_ENABLED"
	BackupDedupeEnabledEnv = "BACKUP_DEDUPE_ENABLED"
)

// CommunityDisabledEnv must always be rendered as false, never operator overridden.
var CommunityDisabledEnv = []string{NamespacesEnabledEnv, SelfRecoveryEnabledEnv, BackupDedupeEnabledEnv}

func InstallDir(version string) string { return "/opt/karakoram/weaviate-" + version }
func BinaryPath(version string) string { return path.Join(InstallDir(version), Process) }

type Ports struct {
	HTTP    int `json:"http"`
	GRPC    int `json:"grpc"`
	Gossip  int `json:"gossip"`
	Data    int `json:"data"`
	Raft    int `json:"raft"`
	RaftRPC int `json:"raftRpc"`
	Metrics int `json:"metrics"`
}

// MemberSpec contains secrets. Never log it or include it in status/errors.
// TLS PEM material lives only in the enclosing agentapi.MemberConfig.TLS.
// TLS selects HTTPS-only REST and client gRPC TLS; internal traffic is private.
// RaftJoin is ordered voter name:RPC-port addresses, including the local name
// only for voters. Every non-voter still needs the same voter discovery list.
// Nodes must not have substring-overlapping names: upstream detects voters with
// strings.Contains against the first BootstrapExpect entries.
type MemberSpec struct {
	Version                  string            `json:"version"`
	NodeName                 string            `json:"nodeName"`
	Ports                    Ports             `json:"ports"`
	BindHost                 string            `json:"bindHost"`
	RaftJoin                 []string          `json:"raftJoin"`
	BootstrapExpect          int               `json:"bootstrapExpect"`
	GossipJoin               []string          `json:"gossipJoin"`
	TLS                      bool              `json:"tls"`
	RootUser                 string            `json:"rootUser"`
	AllowedKeys              []string          `json:"allowedKeys"`
	ClusterUsername          string            `json:"clusterUsername"`
	ClusterPassword          string            `json:"clusterPassword"`
	Modules                  []string          `json:"modules"`
	InferenceEndpoints       map[string]string `json:"inferenceEndpoints,omitempty"`
	ReplicationMinimumFactor int               `json:"replicationMinimumFactor"`
	AsyncReplication         bool              `json:"asyncReplication"`
	QueryDefaultsLimit       int               `json:"queryDefaultsLimit"`
	DiskWarningPercent       int               `json:"diskWarningPercent"`
	DiskReadonlyPercent      int               `json:"diskReadonlyPercent"`
	MemoryLimitMiB           int64             `json:"memoryLimitMiB"`
	GOMAXPROCS               int               `json:"gomaxprocs"`
	Backup                   *BackupS3         `json:"backup,omitempty"`
	RestartToken             string            `json:"restartToken,omitempty"`
}

type BackupS3 struct {
	Bucket    string `json:"bucket"`
	Path      string `json:"path"`
	Endpoint  string `json:"endpoint"`
	UseSSL    bool   `json:"useSSL"`
	Region    string `json:"region"`
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
}

type ReadinessRequest struct{}
type ReadinessResponse struct {
	Ready         bool   `json:"ready"`
	ConfigVersion int64  `json:"configVersion"`
	Version       string `json:"version"`
	// Message is a bounded, redacted diagnosis, never native response text.
	Message string `json:"message,omitempty"`
}

var nodeName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$`)
var exactVersion = regexp.MustCompile(`^1\.(39|40)\.(0|[1-9][0-9]*)$`)

func clean(s string) bool {
	return s != "" && !strings.ContainsAny(s, ",\x00\r\n") && strings.TrimSpace(s) == s
}
func address(s string) bool {
	h, p, e := net.SplitHostPort(s)
	if e != nil || h == "" {
		return false
	}
	n, e := strconv.Atoi(p)
	return e == nil && n > 0 && n <= 65535 && (net.ParseIP(h) != nil || nodeName.MatchString(h) || validDNS(h))
}
func validDNS(s string) bool {
	if len(s) > 253 {
		return false
	}
	for _, label := range strings.Split(s, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
		}
	}
	return true
}

func (s MemberSpec) Validate() error {
	if !exactVersion.MatchString(s.Version) {
		return fmt.Errorf("version must be an exact supported 1.39 or 1.40 patch")
	}
	if !nodeName.MatchString(s.NodeName) {
		return fmt.Errorf("invalid node name")
	}
	if net.ParseIP(s.BindHost) == nil {
		return fmt.Errorf("bindHost must be an IP address")
	}
	seen := map[int]bool{}
	for _, p := range []int{s.Ports.HTTP, s.Ports.GRPC, s.Ports.Gossip, s.Ports.Data, s.Ports.Raft, s.Ports.RaftRPC, s.Ports.Metrics} {
		if p < 1024 || p > 65535 || seen[p] {
			return fmt.Errorf("ports must be distinct and between 1024 and 65535")
		}
		seen[p] = true
	}
	if s.BootstrapExpect != 1 && s.BootstrapExpect != 3 || len(s.RaftJoin) != s.BootstrapExpect {
		return fmt.Errorf("raftJoin must contain exactly one or three voters matching bootstrapExpect")
	}
	peers := map[string]bool{}
	for _, a := range s.RaftJoin {
		if !address(a) || peers[a] {
			return fmt.Errorf("invalid or duplicate Raft join address")
		}
		peers[a] = true
	}
	for _, a := range s.GossipJoin {
		if !address(a) {
			return fmt.Errorf("invalid gossip join address")
		}
	}
	if len(s.GossipJoin) > 9 {
		return fmt.Errorf("too many gossip seeds")
	}
	if !clean(s.RootUser) || len(s.RootUser) > 128 || len(s.AllowedKeys) < 1 || len(s.AllowedKeys) > 2 || !clean(s.ClusterUsername) || !clean(s.ClusterPassword) {
		return fmt.Errorf("invalid authentication configuration")
	}
	keys := map[string]bool{}
	for _, k := range s.AllowedKeys {
		if !clean(k) || len(k) > 4096 || keys[k] {
			return fmt.Errorf("invalid or duplicate static key")
		}
		keys[k] = true
	}
	if s.ReplicationMinimumFactor < 1 || s.ReplicationMinimumFactor > 9 || s.QueryDefaultsLimit < 1 || s.QueryDefaultsLimit > 10000 {
		return fmt.Errorf("replication factor or query limit outside bounds")
	}
	if s.DiskWarningPercent < 1 || s.DiskWarningPercent >= s.DiskReadonlyPercent || s.DiskReadonlyPercent > 95 {
		return fmt.Errorf("disk thresholds must satisfy 1 <= warning < readonly <= 95")
	}
	if s.MemoryLimitMiB < 128 || s.MemoryLimitMiB > 1048576 || s.GOMAXPROCS < 1 || s.GOMAXPROCS > 1024 {
		return fmt.Errorf("memory limit or GOMAXPROCS outside bounds")
	}
	enabled := map[string]bool{}
	for _, m := range s.Modules {
		if !AllowedModule(m) || enabled[m] {
			return fmt.Errorf("unknown or duplicate module")
		}
		enabled[m] = true
	}
	if !enabled["backup-s3"] {
		return fmt.Errorf("backup-s3 module is required")
	}
	for env, raw := range s.InferenceEndpoints {
		mod, ok := InferenceModule(env)
		if !ok || !enabled[mod] {
			return fmt.Errorf("inference endpoint requires an allowed enabled module")
		}
		u, e := url.Parse(raw)
		if e != nil || u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.RawQuery != "" || (u.Scheme != "http" && u.Scheme != "https") || strings.ContainsAny(raw, "\x00\r\n") {
			return fmt.Errorf("invalid inference endpoint URL")
		}
		if u.Port() != "" {
			p, e := strconv.Atoi(u.Port())
			if e != nil || p < 1 || p > 65535 {
				return fmt.Errorf("invalid inference endpoint port")
			}
		}
	}
	for env, mod := range inferenceModules {
		if enabled[mod] && s.InferenceEndpoints[env] == "" {
			return fmt.Errorf("local inference module requires its endpoint")
		}
	}
	if b := s.Backup; b != nil {
		if !clean(b.Bucket) || len(b.Bucket) > 63 || !clean(b.Endpoint) || strings.ContainsAny(b.Path, "\x00\r\n") || strings.HasPrefix(b.Path, "/") {
			return fmt.Errorf("invalid backup location")
		}
		for _, part := range strings.Split(b.Path, "/") {
			if part == ".." || part == "." {
				return fmt.Errorf("invalid backup path")
			}
		}
		u, e := url.Parse("http://" + b.Endpoint)
		if e != nil || u.Hostname() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("backup endpoint must be host[:port]")
		}
		if u.Port() != "" {
			n, e := strconv.Atoi(u.Port())
			if e != nil || n < 1 || n > 65535 {
				return fmt.Errorf("invalid backup endpoint port")
			}
		}
		if !clean(b.AccessKey) || !clean(b.SecretKey) || !clean(b.Region) {
			return fmt.Errorf("backup credentials and region required")
		}
	}
	return nil
}

// MetricNames is the fixed public exposition vocabulary, in catalog order.
var MetricNames = []string{
	"karakoram_weaviate_up",
	"karakoram_weaviate_objects", "karakoram_weaviate_shards", "karakoram_weaviate_collections",
	"karakoram_weaviate_queries_total", "karakoram_weaviate_requests_total", "karakoram_weaviate_batch_objects_total",
	"karakoram_weaviate_vector_index_capacity", "karakoram_weaviate_vector_index_tombstones",
	"karakoram_weaviate_lsm_bytes", "karakoram_weaviate_async_queue_length",
}
