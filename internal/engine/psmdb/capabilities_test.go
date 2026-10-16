// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestRequiredCapabilities(t *testing.T) {
	p := New(Options{})
	for _, tt := range []struct {
		topology string
		routers  int
		want     []string
	}{
		{"replicaSet", 3, nil},
		{"sharded", 0, nil},
		{"sharded", 1, nil},
		{"sharded", 2, []string{"horizontalScaling"}},
	} {
		t.Run(fmt.Sprintf("%s/%d", tt.topology, tt.routers), func(t *testing.T) {
			spec := domain.ClusterSpec{Topology: domain.Topology{Type: tt.topology}, Components: domain.Components{Router: domain.ComponentSpec{Replicas: tt.routers}}}
			if got := p.RequiredCapabilities(spec); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			err := engine.CheckCapabilities(p, spec, "limited", infra.Capabilities{})
			if len(tt.want) > 0 && !errors.Is(err, engine.ErrNotSupported) || len(tt.want) == 0 && err != nil {
				t.Fatal(err)
			}
			if err := engine.CheckCapabilities(p, spec, "capable", infra.Capabilities{HorizontalScaling: true}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type unsupportedContainerInfra struct {
	*fakeInfra
	err error
}

func (f *unsupportedContainerInfra) SetContainerCount(context.Context, string, int, int) (infra.Op, error) {
	return infra.Op{}, f.err
}

func TestUnsupportedRetryResponseDoesNotBackoff(t *testing.T) {
	for _, sentinel := range []error{infra.ErrUnsupported, engine.ErrNotSupported} {
		t.Run(sentinel.Error(), func(t *testing.T) {
			p, rc, f, clock := retrySetup(t)
			s := syncToPending(t, p, rc, "deploy")
			m := s.Members[0]
			v := &pending{Action: "containers", Member: m.Name, Containers: 2}
			r := &platformRetry{RetryAfter: clock.at, Failures: 1}
			s.Retry, s.Pending = v, nil
			s.PlatformRetries = map[string]*platformRetry{platformRetryKey(v): r}
			rc.Backend = &unsupportedContainerInfra{fakeInfra: f.fakeInfra, err: fmt.Errorf("container count: %w", sentinel)}
			before := *r
			if handled, err := p.retryPending(t.Context(), rc, s); !handled || !errors.Is(err, sentinel) {
				t.Fatalf("handled=%v err=%v", handled, err)
			}
			if s.Pending != nil || s.Retry != v || !reflect.DeepEqual(before, *r) {
				t.Fatal("unsupported response scheduled a retry or changed backoff")
			}
		})
	}
}
