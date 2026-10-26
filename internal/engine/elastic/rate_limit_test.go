// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
	"time"
)

type limitedResourcesInfra struct {
	*coreInfra
	at time.Time
}

func (f *limitedResourcesInfra) SetResources(context.Context, string, infra.Resources) (infra.Op, error) {
	return infra.Op{}, &infra.RetryAfterError{Cause: infra.ErrUnavailable, At: f.at}
}
func TestBackendRateLimitPersistsRetryDeadline(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	s := coreConverge(t, p, rc, f)
	target := &pending{Action: "resources", Member: s.Members[0].Name, Resources: s.Members[0].Resources}
	at := rc.Clock.Now().Add(time.Hour)
	rc.Backend = &limitedResourcesInfra{coreInfra: f, at: at}
	err := p.issueAction(context.Background(), rc, s, target)
	if !infra.RetryAt(err).Equal(at) || !s.PlatformRetries[retryKey(target)].RetryAfter.Equal(at) {
		t.Fatalf("rate limit lost: err=%v retry=%+v", err, s.PlatformRetries)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	s, err = load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if !s.PlatformRetries[retryKey(target)].RetryAfter.Equal(at) {
		t.Fatal("rate limit lost after state reload")
	}
}
