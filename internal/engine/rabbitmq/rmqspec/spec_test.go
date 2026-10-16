// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rmqspec

import (
	"crypto/sha256"
	"encoding/json"
	"testing"
)

func TestLegacyIPv6MemberSpecHash(t *testing.T) {
	spec := MemberSpec{NodeName: "rabbit@member", Peers: []string{"rabbit@member"}, Cookie: "test-cookie", Version: "4.3.6", Plugins: []string{"rabbitmq_management"}, AMQPPort: 5672, ManagementPort: 15672, PrometheusPort: 15692, MemoryHighWatermarkBytes: 600000000, TotalMemoryBytes: 1000000000, DiskFreeLimitBytes: 1000000000, DefaultQueueType: "quorum"}
	// Wire representation before adding the optional IPv4 distribution field.
	legacy := `{"nodeName":"rabbit@member","peers":["rabbit@member"],"cookie":"test-cookie","version":"4.3.6","plugins":["rabbitmq_management"],"amqpPort":5672,"amqpsPort":0,"managementPort":15672,"managementTlsPort":0,"prometheusPort":15692,"memoryHighWatermarkBytes":600000000,"totalMemoryBytes":1000000000,"diskFreeLimitBytes":1000000000,"defaultQueueType":"quorum","interNodeTls":false}`
	data, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(data) != sha256.Sum256([]byte(legacy)) {
		t.Fatal("default IPv6 member serialization changed the legacy hash")
	}
	spec.IPv4Distribution = true
	data, err = json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(data) == sha256.Sum256([]byte(legacy)) {
		t.Fatal("IPv4 distribution must change the member hash")
	}
}
