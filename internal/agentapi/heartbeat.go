// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agentapi

import (
	"encoding/json"
	"time"
)

// HeartbeatPath is served by the control plane's internal (agent) listener.
// Agents authenticate with the same member bearer as configuration retrieval.
const HeartbeatPath = "/v1/agent/heartbeat"

// Heartbeat timing shared by both sides. The control plane sends the interval
// in every reply; agents clamp it to [HeartbeatMinInterval, HeartbeatMaxInterval].
const (
	HeartbeatInterval    = 3 * time.Second
	HeartbeatMinInterval = time.Second
	HeartbeatMaxInterval = time.Minute
	// HeartbeatLegacyBackoff is how long an agent waits after a control plane
	// answered 404 (it predates heartbeats) before trying again.
	HeartbeatLegacyBackoff = 10 * time.Minute
	// HeartbeatSampleMaxBytes bounds one encoded sample response.
	HeartbeatSampleMaxBytes = 256 << 10
	// HeartbeatMaxBytes bounds one encoded heartbeat request.
	HeartbeatMaxBytes = 1 << 20
	// HeartbeatMaxSamples bounds the number of samples in one heartbeat.
	HeartbeatMaxSamples = 16
)

// GenericStatusAction names the generic GET /v1/status document in samples
// and requests; its Engine is empty.
const GenericStatusAction = "status"

// HeartbeatRequest is pushed by an agent. Samples were taken after the agent
// received the reply that carried Token, so the control plane may timestamp
// them with that token's issue time (a lower bound of the real sample time).
type HeartbeatRequest struct {
	// Session is random per agent process and never reused.
	Session string `json:"session"`
	// Token echoes the last token the control plane issued to this session.
	// Empty on the first heartbeat of a session.
	Token                 string            `json:"token,omitempty"`
	Instance              InstanceIdentity  `json:"instance"`
	Samples               []HeartbeatSample `json:"samples,omitempty"`
	ConfigRevision        string            `json:"configRevision,omitempty"`
	ConfigContentRevision string            `json:"-"`
}

// Kept out of the strict heartbeat JSON envelope for older control planes.
const ConfigContentRevisionHeader = "X-Karakoram-Config-Content-Revision"

// InstanceIdentity describes the runtime the agent runs in. TaskID is the
// platform task/allocation ID where the platform exposes one to the container
// (KARAKORAM_TASK_ID on Swarm, NOMAD_ALLOC_ID on Nomad); Addresses are the
// non-loopback interface addresses without prefix length.
type InstanceIdentity struct {
	TaskID    string   `json:"taskId,omitempty"`
	Hostname  string   `json:"hostname,omitempty"`
	Addresses []string `json:"addresses,omitempty"`
}

// HeartbeatSample is the result of one read-only observation executed by the
// agent in-process exactly as the control plane would have requested it.
// Engine "" with Action GenericStatusAction is GET /v1/status; otherwise it is
// POST /v1/<Engine>/<Action> with Request as the body.
type HeartbeatSample struct {
	Engine   string          `json:"engine,omitempty"`
	Action   string          `json:"action"`
	Request  json.RawMessage `json:"request,omitempty"`
	Code     int             `json:"code"`
	Response json.RawMessage `json:"response,omitempty"`
}

// HeartbeatResponse tells the agent when to send the next heartbeat and which
// observations to include. Agents execute only PushableAction requests.
type HeartbeatResponse struct {
	Token          string            `json:"token"`
	IntervalMillis int64             `json:"intervalMillis"`
	Requests       []HeartbeatAction `json:"requests,omitempty"`
	ReloadConfig   bool              `json:"reloadConfig,omitempty"`
	ConfigRevision string            `json:"configRevision,omitempty"`
}

// HeartbeatAction is one requested observation.
type HeartbeatAction struct {
	Engine  string          `json:"engine,omitempty"`
	Action  string          `json:"action"`
	Request json.RawMessage `json:"request,omitempty"`
}

// pushableActions are the read-only engine observations agents may execute
// periodically. Both sides enforce this list: the control plane only
// subscribes to these, and agents refuse anything else.
var pushableActions = map[string]map[string]bool{
	"":              {GenericStatusAction: true},
	"mysql":         {"status": true, "proxy-status": true, "galera-status": true},
	"postgres":      {"status": true},
	"neo4j":         {"readiness": true},
	"weaviate":      {"readiness": true},
	"elasticsearch": {"readiness": true},
	"rabbitmq":      {"observe": true},
	"keycloak":      {"status": true},
	"supabase":      {"status": true},
	"temporal":      {"status": true},
	"openbao":       {"status": true},
	"datamcp":       {"readiness": true},
}

// PushableAction reports whether engine/action is an allowlisted read-only
// observation that may be executed periodically and served from heartbeats.
func PushableAction(engine, action string) bool {
	return pushableActions[engine][action]
}
