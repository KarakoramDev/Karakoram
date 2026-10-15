// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"encoding/json"
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
)

// Only native resource identities cross the agent boundary, never CLI diagnostics.
func parseQuorumCritical(data []byte, commandErr error) (rmqspec.QuorumCriticalResponse, error) {
	out := rmqspec.QuorumCriticalResponse{Blockers: []rmqspec.QuorumBlocker{}}
	var native struct {
		Result string `json:"result"`
		Queues []struct {
			Name    string   `json:"name"`
			VHost   string   `json:"virtual_host"`
			Type    string   `json:"type"`
			Members []string `json:"members"`
		} `json:"queues"`
	}
	if json.Unmarshal(data, &native) != nil {
		return out, errors.New("invalid quorum-critical response")
	}
	if native.Result == "ok" && commandErr == nil && len(native.Queues) == 0 {
		return out, nil
	}
	if native.Result != "error" || len(native.Queues) == 0 {
		if commandErr != nil {
			return out, commandErr
		}
		return out, errors.New("unexpected quorum-critical result")
	}
	for _, queue := range native.Queues {
		if !validQueueIdentity(queue.Name) || !validQueueIdentity(queue.VHost) {
			return out, errors.New("invalid quorum blocker identity")
		}
		kind := queue.Type
		switch kind {
		case "rabbit_quorum_queue":
			kind = "quorum"
		case "rabbit_stream_queue":
			kind = "stream"
		case "quorum", "stream", "process":
		default:
			return out, errors.New("invalid quorum blocker type")
		}
		members := queue.Members
		if members == nil {
			members = []string{}
		}
		for _, member := range members {
			if !nodePattern.MatchString(member) {
				return out, errors.New("invalid quorum blocker member")
			}
		}
		out.Blockers = append(out.Blockers, rmqspec.QuorumBlocker{Queue: queue.Name, VHost: queue.VHost, Type: kind, Members: members})
	}
	return out, nil
}

func validQueueIdentity(value string) bool {
	return value != "" && len(value) <= 255 && utf8.ValidString(value) && strings.IndexFunc(value, unicode.IsControl) < 0
}
