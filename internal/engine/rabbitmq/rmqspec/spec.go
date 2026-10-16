// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package rmqspec defines the RabbitMQ member and bounded agent protocol.
package rmqspec

import "encoding/json"

type MemberSpec struct {
	NodeName string   `json:"nodeName"`
	Peers    []string `json:"peers"`
	Cookie   string   `json:"cookie"`
	Version  string   `json:"version"`
	Plugins  []string `json:"plugins"`
	// AMQP and AMQPS are individually optional (0 disables); at least one is required.
	AMQPPort  int `json:"amqpPort"`
	AMQPSPort int `json:"amqpsPort"`
	// Management HTTP and HTTPS are individually optional; at least one is required.
	ManagementPort    int `json:"managementPort"`
	ManagementTLSPort int `json:"managementTlsPort"`
	// Stream listeners are required when rabbitmq_stream is enabled and follow TLS mode.
	StreamPort    int `json:"streamPort,omitempty"`
	StreamTLSPort int `json:"streamTlsPort,omitempty"`
	// PrometheusPort is always required by the enabled Prometheus plugin.
	PrometheusPort           int    `json:"prometheusPort"`
	ConfigOverrides          string `json:"configOverrides,omitempty"`
	MemoryHighWatermarkBytes int64  `json:"memoryHighWatermarkBytes"`
	TotalMemoryBytes         int64  `json:"totalMemoryBytes"`
	DiskFreeLimitBytes       int64  `json:"diskFreeLimitBytes"`
	DefaultQueueType         string `json:"defaultQueueType"`
	InterNodeTLS             bool   `json:"interNodeTls"`
	// IPv4Distribution selects IPv4 Erlang distribution; omitted keeps the
	// legacy IPv6 transport and serialized member specifications unchanged.
	IPv4Distribution bool `json:"ipv4Distribution,omitempty"`
}

type EmptyRequest struct{}
type JoinRequest struct {
	Seed string `json:"seed"`
}
type NodeRequest struct {
	Node string `json:"node"`
}
type AwaitQuorumRequest struct {
	TimeoutSeconds int `json:"timeoutSeconds,omitempty"`
}
type SetPasswordRequest struct {
	User string `json:"user"`
}
type StreamReplicasRequest struct {
	Action string `json:"action"`
	VHost  string `json:"vhost"`
	Queue  string `json:"queue"`
	Node   string `json:"node,omitempty"`
}
type ActionResponse struct {
	OK     bool            `json:"ok"`
	Output json.RawMessage `json:"output,omitempty"`
}
type QuorumBlocker struct {
	VHost   string   `json:"vhost"`
	Queue   string   `json:"queue"`
	Type    string   `json:"type"`
	Members []string `json:"members"`
}
type QuorumCriticalResponse struct {
	Blockers []QuorumBlocker `json:"blockers"`
}
type FeatureFlag struct {
	Name      string `json:"name"`
	State     string `json:"state"`
	Stability string `json:"stability,omitempty"`
}
type StatusResponse struct {
	NodeName     string          `json:"nodeName"`
	RunningNodes []string        `json:"runningNodes"`
	DiskNodes    []string        `json:"diskNodes"`
	Alarms       json.RawMessage `json:"alarms"`
	Partitions   json.RawMessage `json:"partitions"`
	Maintenance  bool            `json:"maintenance"`
	FeatureFlags []FeatureFlag   `json:"featureFlags"`
	Version      string          `json:"version"`
}
type StreamReplicasResponse struct {
	Replicas json.RawMessage `json:"replicas"`
}

// MetricNames is the fixed public exposition vocabulary, in catalog order.
var MetricNames = []string{
	"karakoram_rabbitmq_up", "karakoram_rabbitmq_uptime_seconds",
	"karakoram_rabbitmq_connections", "karakoram_rabbitmq_channels",
	"karakoram_rabbitmq_consumers", "karakoram_rabbitmq_queues",
	"karakoram_rabbitmq_messages_ready", "karakoram_rabbitmq_messages_unacked",
	"karakoram_rabbitmq_published_total", "karakoram_rabbitmq_delivered_total",
	"karakoram_rabbitmq_memory_alarm", "karakoram_rabbitmq_disk_alarm",
	"karakoram_rabbitmq_resident_memory_bytes", "karakoram_rabbitmq_memory_limit_bytes",
	"karakoram_rabbitmq_disk_free_bytes", "karakoram_rabbitmq_disk_free_limit_bytes",
	"karakoram_rabbitmq_cluster_members_running",
}
