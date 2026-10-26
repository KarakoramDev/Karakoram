// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
	"time"
)

type limitedScalingInfra struct {
	*fakeInfra
	at time.Time
}

func (f *limitedScalingInfra) SetContainerCount(context.Context, string, int, int) (infra.Op, error) {
	return infra.Op{}, &infra.RetryAfterError{Cause: infra.ErrUnavailable, At: f.at}
}
func TestBackendRateLimitPersistsRetryDeadline(t *testing.T) {
	p, rc, f, clock := retrySetup(t)
	s := syncToPending(t, p, rc, "deploy")
	target := &pending{Action: "containers", Member: s.Members[0].Name, Containers: 2}
	s.Pending = nil
	s.Retry = target
	s.PlatformRetries = map[string]*platformRetry{platformRetryKey(target): {RetryAfter: clock.at}}
	at := clock.at.Add(time.Hour)
	rc.Backend = &limitedScalingInfra{fakeInfra: f.fakeInfra, at: at}
	_, err := p.retryPending(context.Background(), rc, s)
	if !infra.RetryAt(err).Equal(at) || !s.PlatformRetries[platformRetryKey(target)].RetryAfter.Equal(at) {
		t.Fatalf("rate limit lost: err=%v retry=%+v", err, s.PlatformRetries)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	s = retryState(t, rc)
	if !s.PlatformRetries[platformRetryKey(target)].RetryAfter.Equal(at) {
		t.Fatal("rate limit lost after state reload")
	}
}
