// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
)

type restartProbeState string

const (
	restartLive    restartProbeState = "live"
	restartDown    restartProbeState = "definitively-down"
	restartUnknown restartProbeState = "unknown"
)

type restartMemberSample struct {
	Name         string
	State        restartProbeState
	UptimeMillis int64
	Started      time.Time
	Finished     time.Time
	Failure      string
}

type restartProbeSummary struct {
	ConclusiveRounds   int
	InconclusiveRounds int
}

func (s restartProbeSummary) requireConclusive() error {
	if s.ConclusiveRounds == 0 {
		return fmt.Errorf("rolling restart had no conclusive live-quorum samples: inconclusive rounds=%d", s.InconclusiveRounds)
	}
	return nil
}

// Public-routing timeouts cannot distinguish an unavailable route from a down
// voter. A refused connection or a reachable unhealthy server is definitive.
func restartErrorState(err error) restartProbeState {
	if errors.Is(err, syscall.ECONNREFUSED) || strings.Contains(strings.ToLower(err.Error()), "connection refused") {
		return restartDown
	}
	var command mongo.CommandError
	if errors.As(err, &command) && (command.Code != 0 || !mongo.IsNetworkError(err)) {
		return restartDown
	}
	var selection topology.ServerSelectionError
	var network net.Error
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.As(err, &selection) ||
		(errors.As(err, &network) && network.Timeout()) ||
		mongo.IsNetworkError(err) {
		return restartUnknown
	}
	return restartDown
}

// The captured configuration establishes every endpoint is a voter. Only
// direct probes count; unknown routing failures are recorded, not called down.
func restartQuorum(samples []restartMemberSample) (conclusive bool, err error) {
	live, down, unknown := 0, 0, 0
	for _, sample := range samples {
		switch sample.State {
		case restartLive:
			live++
		case restartDown:
			down++
		default:
			unknown++
		}
	}
	majority := len(samples)/2 + 1
	if down >= majority || (live < majority && unknown == 0) {
		return true, fmt.Errorf("rolling restart lost voting quorum: voters=%d live=%d down=%d unknown=%d samples=%+v", len(samples), live, down, unknown, samples)
	}
	return live >= majority, nil
}

// Infer process-start intervals from uptimeMillis and local monotonic command
// timing. A process that merely runs longer keeps the same start interval;
// unlike comparing raw uptimes, this also detects resets during slow restarts.
func restartUptimeReset(before, after restartMemberSample) bool {
	if before.State != restartLive || after.State != restartLive {
		return false
	}
	latestOldStart := before.Finished.Add(-time.Duration(before.UptimeMillis) * time.Millisecond)
	earliestNewStart := after.Started.Add(-time.Duration(after.UptimeMillis) * time.Millisecond)
	// Account for the server's millisecond uptime quantization.
	return earliestNewStart.After(latestOldStart.Add(time.Millisecond))
}
