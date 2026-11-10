// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import "sync/atomic"

const (
	agentConfigFull = iota
	agentConfigUnchanged
	agentConfigError
)

// Installation-wide process counters; no identities or request values are labels.
var agentConfigRequests [3]atomic.Uint64
