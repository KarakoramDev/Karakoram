// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package contracttest provides the portable infrastructure backend conformance suite.
// Backends can run it against isolated test targets or an in-memory API server.
package contracttest

import (
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Harness supplies an isolated target for one conformance subtest. Artifact must
// return a fresh reader on every call. Hostname returns unique valid names.
type Harness struct {
	Backend               infra.Backend
	Hostname              func(suffix string) string
	Artifact              func() infra.Artifact
	PollInterval, Timeout time.Duration
}

// Run checks backend-neutral service, deployment, and lifecycle semantics.
// Every subtest obtains its own isolated harness.
func Run(t *testing.T, newHarness func(t *testing.T) Harness) {
	run(t, newHarness)
}
