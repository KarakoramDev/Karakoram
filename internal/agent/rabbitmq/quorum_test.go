// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
)

func TestQuorumBlockersExposeOnlyStructuredIdentities(t *testing.T) {
	data := []byte(`{"result":"error","message":"private CLI diagnostics","queues":[{"name":"orders","virtual_host":"/customers","type":"rabbit_quorum_queue","members":["rabbit@ordersm0"],"readable_name":"private CLI diagnostics"},{"name":"events","virtual_host":"/","type":"rabbit_stream_queue"},{"name":"rabbitmq_metadata","virtual_host":"(not applicable)","type":"process"}]}`)
	result, err := parseQuorumCritical(data, errors.New("health check failed"))
	if err != nil || len(result.Blockers) != 3 {
		t.Fatal(result, err)
	}
	first := result.Blockers[0]
	if first.VHost != "/customers" || first.Queue != "orders" || first.Type != "quorum" || len(first.Members) != 1 || first.Members[0] != "rabbit@ordersm0" {
		t.Fatal(first)
	}
	if result.Blockers[1].Type != "stream" || result.Blockers[1].Members == nil || result.Blockers[2].Type != "process" {
		t.Fatal(result)
	}
}

func TestQuorumBlockersRejectMalformedNativeData(t *testing.T) {
	for _, data := range []string{
		`{"result":"error","queues":[{"name":"q","virtual_host":"/","type":"unknown"}]}`,
		`{"result":"error","queues":[{"name":"q\nsecret","virtual_host":"/","type":"quorum"}]}`,
		`{"result":"error","queues":[{"name":"q","type":"quorum"}]}`,
		`{"result":"error","queues":[{"name":"q","virtual_host":"/","type":"quorum","members":["rabbit@bad;command"]}]}`,
		`private CLI diagnostics`,
	} {
		_, err := parseQuorumCritical([]byte(data), nil)
		if err == nil || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "private CLI diagnostics") {
			t.Fatal("invalid data or diagnostics escaped", err)
		}
	}
}

func TestReplicaNodeTargetsRemainBounded(t *testing.T) {
	for _, node := range []string{"", "-n", "rabbit@node;shutdown", "rabbit@node\n", "rabbit@node --all", "rabbit@$(id)"} {
		if err := validateRequest(&rmqspec.NodeRequest{Node: node}); err == nil {
			t.Fatalf("accepted malformed target %q", node)
		}
	}
	if err := validateRequest(&rmqspec.NodeRequest{Node: "rabbit@ordersm2"}); err != nil {
		t.Fatal(err)
	}
}
