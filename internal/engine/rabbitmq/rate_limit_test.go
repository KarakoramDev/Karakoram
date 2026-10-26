// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
	"time"
)

func TestBackendRateLimitPersistsRetryDeadline(t *testing.T) {
	p, rc, f, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	target := &pending{Action: "resources", Member: s.Members[0].Name, Resources: s.Members[0].Resources}
	at := rc.Clock.Now().Add(time.Hour)
	f.failures["resources:"+target.Member] = &infra.RetryAfterError{Cause: infra.ErrUnavailable, At: at}
	err := p.submit(context.Background(), rc, s, target)
	if !infra.RetryAt(err).Equal(at) || !s.PlatformRetries[platformRetryKey(target)].RetryAfter.Equal(at) {
		t.Fatalf("rate limit lost: err=%v retry=%+v", err, s.PlatformRetries)
	}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	s = state(t, rc)
	if !s.PlatformRetries[platformRetryKey(target)].RetryAfter.Equal(at) {
		t.Fatal("rate limit lost after state reload")
	}
}
