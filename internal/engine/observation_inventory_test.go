// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type inventoryBackend struct {
	infra.Backend
	instances              []infra.Container
	enter                  chan struct{}
	release                <-chan struct{}
	active, maximum, calls atomic.Int32
}

func (b *inventoryBackend) wait(ctx context.Context) error {
	b.calls.Add(1)
	active := b.active.Add(1)
	defer b.active.Add(-1)
	for previous := b.maximum.Load(); active > previous && !b.maximum.CompareAndSwap(previous, active); previous = b.maximum.Load() {
	}
	if b.enter != nil {
		b.enter <- struct{}{}
	}
	if b.release != nil {
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
func (b *inventoryBackend) ProjectInfo(ctx context.Context) (infra.Project, error) {
	return infra.Project{}, b.wait(ctx)
}
func (b *inventoryBackend) ListServices(ctx context.Context, _ string) ([]infra.Service, error) {
	return nil, b.wait(ctx)
}
func (b *inventoryBackend) Containers(ctx context.Context, _ string) ([]infra.Container, error) {
	return b.instances, b.wait(ctx)
}
func (*inventoryBackend) Layout() infra.Layout { return infra.Layout{} }

func TestObservationPlatformConcurrencyIsBounded(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	release := make(chan struct{})
	b := &inventoryBackend{enter: make(chan struct{}, 22), release: release}
	names := make([]string, 20)
	for i := range names {
		names[i] = fmt.Sprint("m", i)
	}
	done := make(chan PlatformObservation, 1)
	go func() { done <- ObservePlatform(ctx, b, "demo", names) }()
	for range observationConcurrency {
		select {
		case <-b.enter:
		case <-ctx.Done():
			t.Fatal("platform workers did not overlap")
		}
	}
	close(release)
	select {
	case got := <-done:
		if got.ProjectErr != nil || got.ServicesErr != nil || len(got.Members) != 20 || b.calls.Load() != 22 || b.maximum.Load() != observationConcurrency {
			t.Fatal("unexpected bounded inventory", got, b.calls.Load(), b.maximum.Load())
		}
	case <-ctx.Done():
		t.Fatal("platform inventory did not complete")
	}
}

type inventoryAgentDialer struct{ options agentapi.ClientOptions }

func (d inventoryAgentDialer) Dial(context.Context, domain.ClusterRef, string) (*agentapi.Client, error) {
	return agentapi.NewClient(d.options)
}

func TestObservationConcurrentInstanceVisitsAreBounded(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	b := &inventoryBackend{}
	for i := range 20 {
		b.instances = append(b.instances, infra.Container{ID: fmt.Sprint(i), Hostname: fmt.Sprintf("instance-%d.test", i), Status: infra.ContainerActive})
	}
	rc := &Context{Cluster: &domain.Cluster{Name: "demo"}, Backend: b, Agents: inventoryAgentDialer{agentapi.ClientOptions{BaseURL: server.URL, CAPEM: ca, Token: "test"}}}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	entered := make(chan struct{}, 20)
	release := make(chan struct{})
	var active, maximum, visits atomic.Int32
	visit := func(*agentapi.Client) (bool, error) {
		visits.Add(1)
		n := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); n > old && !maximum.CompareAndSwap(old, n); old = maximum.Load() {
		}
		if entered != nil {
			entered <- struct{}{}
		}
		select {
		case <-release:
			return true, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	done := make(chan error, 1)
	go func() {
		all, count, err := VisitLiveInstancesConcurrent(ctx, rc, "member", visit)
		if err == nil && (!all || count != 20) {
			err = fmt.Errorf("all=%v count=%d", all, count)
		}
		done <- err
	}()
	for range observationConcurrency {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("instance probes did not overlap")
		}
	}
	close(release)
	if err := <-done; err != nil || maximum.Load() != observationConcurrency || visits.Load() != 20 {
		t.Fatal("unexpected concurrent visits", err, maximum.Load(), visits.Load())
	}
	entered = nil
	maximum.Store(0)
	all, count, err := VisitLiveInstances(ctx, rc, "member", visit)
	if err != nil || !all || count != 20 || maximum.Load() != 1 {
		t.Fatal("serial visitor contract changed", all, count, err, maximum.Load())
	}
}

func TestObservationConcurrentInstanceErrorsPreserveInventoryOrder(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	for _, order := range [][]int{{2, 1, 0}, {1, 0, 2}} {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		delay := map[string]time.Duration{}
		inventory := InstanceInventory{}
		for i, index := range order {
			delay[fmt.Sprintf("instance-%d.test", index)] = time.Duration(i+1) * 40 * time.Millisecond
		}
		for i := range 3 {
			inventory.Containers = append(inventory.Containers, infra.Container{ID: fmt.Sprint(i), Hostname: fmt.Sprintf("instance-%d.test", i), Status: infra.ContainerActive})
		}
		rc := &Context{Cluster: &domain.Cluster{Name: "demo"}, Backend: &inventoryBackend{}, Agents: inventoryAgentDialer{agentapi.ClientOptions{
			BaseURL: server.URL, CAPEM: ca, Token: "test",
			DialInstanceContext: func(ctx context.Context, _, _, instance string) (net.Conn, error) {
				timer := time.NewTimer(delay[instance])
				defer timer.Stop()
				select {
				case <-timer.C:
					return nil, fmt.Errorf("failure at %s", instance)
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		}}}
		all, count, err := VisitObservedInstances(ctx, rc, "member", inventory, func(client *agentapi.Client) (bool, error) {
			_, err := client.DialContext(ctx, "tcp", "logical.test:27017")
			return false, err
		})
		cancel()
		if err == nil || all || count != 3 {
			t.Fatal(all, count, err)
		}
		message := err.Error()
		if strings.Index(message, "instance 0:") > strings.Index(message, "instance 1:") || strings.Index(message, "instance 1:") > strings.Index(message, "instance 2:") {
			t.Fatal("diagnostic order followed completion rather than inventory", message)
		}
	}
}
