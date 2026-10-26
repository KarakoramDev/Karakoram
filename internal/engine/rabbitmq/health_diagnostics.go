// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

func joinedDiagnostics(messages []string) string {
	slices.Sort(messages)
	return strings.Join(slices.DeleteFunc(slices.Compact(messages), func(message string) bool { return message == "" }), "; ")
}

func alarmDetails(observer string, raw json.RawMessage) []string {
	if noProblems(raw) {
		return nil
	}
	// cluster_status --formatter json returns alarm_maps: one entry per
	// affected node/resource, including alarms learned from other members.
	var alarms []struct {
		Node     string `json:"node"`
		Resource string `json:"resource"`
		Type     string `json:"type"`
	}
	if err := json.Unmarshal(raw, &alarms); err != nil || len(alarms) == 0 {
		return []string{fmt.Sprintf("%s reports a RabbitMQ alarm: inspect rabbitmq-diagnostics alarms and restore resource headroom; do not delete messages or force a restart", observer)}
	}
	messages := make([]string, 0, len(alarms))
	for _, alarm := range alarms {
		node := alarm.Node
		if node == "" {
			node = observer
		}
		switch alarm.Resource {
		case "disk":
			messages = append(messages, fmt.Sprintf("%s: disk free-space alarm blocks publishers; free non-database files or add disk capacity above disk_free_limit; do not delete queue data", node))
		case "memory":
			messages = append(messages, fmt.Sprintf("%s: memory watermark alarm blocks publishers; reduce publisher pressure, consume queued messages, or add memory capacity; do not purge queues or force a restart", node))
		default:
			messages = append(messages, fmt.Sprintf("%s: RabbitMQ %s/%s alarm; inspect rabbitmq-diagnostics alarms and restore resource headroom", node, alarm.Type, alarm.Resource))
		}
	}
	return messages
}

func partitionDetails(observer string, raw json.RawMessage) []string {
	if noProblems(raw) {
		return nil
	}
	var partitions map[string][]string
	if err := json.Unmarshal(raw, &partitions); err != nil || len(partitions) == 0 {
		return []string{fmt.Sprintf("%s reports a network partition; restore peer connectivity and inspect cluster_status before resuming changes; do not reset or force-boot brokers", observer)}
	}
	messages := make([]string, 0, len(partitions))
	for node, peers := range partitions {
		slices.Sort(peers)
		messages = append(messages, fmt.Sprintf("%s is partitioned from %s; restore peer connectivity and inspect cluster_status before resuming changes; do not reset or force-boot brokers", node, strings.Join(peers, ", ")))
	}
	return messages
}
