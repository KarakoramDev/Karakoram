//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// Each round probes every captured public endpoint concurrently with a shared
// one-second bound. Clients are direct and verify TLS against the cluster CA,
// as constructed by mongoMember before any restart is submitted.
func sampleRestartMembers(ctx context.Context, members []observedMember, clients []*mongo.Client) []restartMemberSample {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	samples := make([]restartMemberSample, len(members))
	var wg sync.WaitGroup
	for i := range members {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sample := &samples[i]
			sample.Name = members[i].Name
			sample.State = restartUnknown
			var status struct {
				UptimeMillis *int64 `bson:"uptimeMillis"`
			}
			sample.Started = time.Now()
			err := clients[i].Database("admin").RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}},
				options.RunCmd().SetReadPreference(readpref.Nearest())).Decode(&status)
			sample.Finished = time.Now()
			if err != nil {
				sample.State = restartErrorState(err)
				sample.Failure = "serverStatus uptime probe failed"
				return
			}
			if status.UptimeMillis == nil || *status.UptimeMillis < 0 {
				sample.State = restartDown
				sample.Failure = "reachable server omitted valid uptimeMillis"
				return
			}
			sample.UptimeMillis = *status.UptimeMillis
			var hello struct {
				Primary   bool `bson:"isWritablePrimary"`
				Secondary bool `bson:"secondary"`
			}
			if err := clients[i].Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}},
				options.RunCmd().SetReadPreference(readpref.Nearest())).Decode(&hello); err != nil {
				sample.State = restartErrorState(err)
				sample.Failure = "hello probe failed"
				return
			}
			sample.State = restartLive
			if !hello.Primary && !hello.Secondary {
				sample.State = restartDown
				sample.Failure = "member is neither PRIMARY nor SECONDARY"
			}
		}(i)
	}
	wg.Wait()
	return samples
}

// Sampling runs independently of potentially slow control-plane API polling.
// Definitive quorum failures are sticky; routing timeouts are inconclusive.
// The summary records both kinds of rounds so an entirely unknown restart
// window cannot pass based only on post-operation recovery.
func watchRestartQuorum(ctx context.Context, members []observedMember, clients []*mongo.Client) (<-chan []restartMemberSample, <-chan error, func() restartProbeSummary) {
	ctx, cancel := context.WithCancel(ctx)
	latest := make(chan []restartMemberSample, 1)
	failed := make(chan error, 1)
	done := make(chan struct{})
	var summary restartProbeSummary
	go func() {
		defer close(done)
		for {
			samples := sampleRestartMembers(ctx, members, clients)
			if ctx.Err() != nil {
				return
			}
			conclusive, err := restartQuorum(samples)
			if conclusive {
				summary.ConclusiveRounds++
			} else {
				summary.InconclusiveRounds++
			}
			if err != nil {
				failed <- err
				return
			}
			select {
			case <-latest:
			default:
			}
			latest <- samples
			select {
			case <-ctx.Done():
				return
			case <-time.After(250 * time.Millisecond):
			}
		}
	}()
	return latest, failed, func() restartProbeSummary {
		cancel()
		<-done
		return summary
	}
}

// Driver reconnects can remain unavailable while project-wide public routing
// settles. Require all endpoints to return live uptime samples within the same
// publicRoutingGrace bound used by the public-connect helpers.
func (s *suite) waitRestartMembers(label string, members []observedMember, clients []*mongo.Client) []restartMemberSample {
	s.t.Helper()
	var samples []restartMemberSample
	s.poll(label, publicRoutingGrace, func() bool {
		samples = sampleRestartMembers(s.ctx, members, clients)
		s.snapshot(label, samples)
		for _, sample := range samples {
			if sample.State != restartLive {
				return false
			}
		}
		return true
	})
	return samples
}
