// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	model "github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// inventoryProvider exercises the production shared sweep with real backend
// HTTP observations; native/agent health is held healthy by this fixture.
type inventoryProvider struct {
	engine.Provider
	names []string
}

func (p inventoryProvider) Status(ctx context.Context, rc *engine.Context) (model.ClusterStatus, error) {
	inventory := engine.ObservePlatform(ctx, rc.Backend, rc.Cluster.Name, p.names)
	if err := errors.Join(inventory.ProjectErr, inventory.ServicesErr); err != nil {
		return model.ClusterStatus{}, err
	}
	services := map[string]infra.Service{}
	for _, s := range inventory.Services {
		services[s.Hostname] = s
	}
	status := model.ClusterStatus{Phase: model.PhaseReady}
	for i, name := range p.names {
		healthy := services[name].Status == infra.StatusRunning && inventory.Members[i].Err == nil && len(inventory.Members[i].Containers) > 0
		for _, c := range inventory.Members[i].Containers {
			healthy = healthy && (c.Status == infra.ContainerRunning || c.Status == infra.ContainerActive)
		}
		status.Members = append(status.Members, model.MemberStatus{Name: name, Health: healthy, Agent: model.AgentStatus{Healthy: healthy, LastSeen: time.Now()}})
		if !healthy {
			status.Phase = model.PhaseDegraded
		}
	}
	return status, nil
}

func TestObservationRealBackendLatencyFitsSweep(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	var names []string
	for i := range 3 {
		name, volume := fmt.Sprint("runtime", i), fmt.Sprint("volume", i)
		importFake(t, b, volumeSpec(volume), runtimeSpec(name, volume))
		deployFake(t, b, name, volume)
		names = append(names, name)
	}
	f.mu.Lock()
	f.latency = 600 * time.Millisecond
	f.mu.Unlock()
	status, err := engine.ObserveStatus(t.Context(), inventoryProvider{names: names}, &engine.Context{Backend: b, Cluster: &model.Cluster{Name: "runtime"}})
	if err != nil || status.Phase != model.PhaseReady || status.ObservedAt.IsZero() {
		t.Fatal("healthy delayed platform could not complete a fresh sweep", status, err)
	}
}

func TestConcurrentObservationSerializesRestoration(t *testing.T) {
	for _, mode := range []string{"list", "member"} {
		t.Run(mode, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			imported := importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			removeFakeService(f, imported.Services["runtime"])
			before := fakeCalls(f, "application.reload")
			backends := []*Backend{b, fakeBackend(t, f, nil), fakeBackend(t, f, nil)}
			var calls sync.WaitGroup
			failures := make(chan error, len(backends))
			for _, backend := range backends {
				calls.Go(func() {
					ctx := infra.ObservationContext(t.Context())
					var err error
					if mode == "member" {
						_, err = backend.GetService(ctx, "runtime")
					} else {
						_, err = backend.ListServices(ctx, "runtime")
					}
					failures <- err
				})
			}
			calls.Wait()
			close(failures)
			for err := range failures {
				if err != nil {
					t.Fatal(err)
				}
			}
			if got := fakeCalls(f, "application.reload") - before; got != 1 {
				t.Fatal("concurrent observation submitted restoration more than once", got)
			}
		})
	}
}

func TestObservationReadsDoNotAcquireTargetMutationLock(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", "volume"), runtimeSpec("relay", ""), runtimeSpec("undeployed", ""))
	deployFake(t, b, "runtime", "volume")
	deployFake(t, b, "relay", "")
	for _, marked := range []bool{false, true} {
		t.Run(fmt.Sprintf("marked=%t", marked), func(t *testing.T) {
			reads := map[string]func(context.Context) error{
				"runtime":    func(ctx context.Context) error { _, err := b.GetService(ctx, "runtime"); return err },
				"relay":      func(ctx context.Context) error { _, err := b.GetService(ctx, "relay"); return err },
				"volume":     func(ctx context.Context) error { _, err := b.GetService(ctx, "volume"); return err },
				"undeployed": func(ctx context.Context) error { _, err := b.GetService(ctx, "undeployed"); return err },
				"absent":     func(ctx context.Context) error { _, err := b.GetService(ctx, "absent"); return err },
				"list":       func(ctx context.Context) error { _, err := b.ListServices(ctx, ""); return err },
				"containers": func(ctx context.Context) error { _, err := b.Containers(ctx, "runtime"); return err },
				"proxy-lookup": func(ctx context.Context) error {
					_, err := b.LookupProxyDeployment(ctx, "runtime", "request")
					return err
				},
				"proxy-policy": func(ctx context.Context) error { _, err := b.ProxyUpdatesPreserveServing(ctx, "runtime"); return err },
				"project":      func(ctx context.Context) error { _, err := b.ProjectInfo(ctx); return err },
				"public-ports": func(ctx context.Context) error { _, err := b.ListPublicPorts(ctx); return err },
				"subdomains":   func(ctx context.Context) error { _, err := b.SubdomainURLs(ctx, "runtime"); return err },
			}
			for name, read := range reads {
				t.Run(name, func(t *testing.T) {
					if err := b.mu.Lock(t.Context()); err != nil {
						t.Fatal(err)
					}
					defer b.mu.Unlock()
					ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
					defer cancel()
					if marked {
						ctx = infra.ObservationContext(ctx)
					}
					if err := read(ctx); err != nil {
						t.Fatalf("pure observation waited for target mutation lock: %v", err)
					}
				})
			}
		})
	}
}

func TestObservationSnapshotKeepsTaskAuthorizationFresh(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	imported := importFake(t, b, runtimeSpec("runtime", ""), runtimeSpec("other", ""))
	deployFake(t, b, "runtime", "")
	deployFake(t, b, "other", "")
	ctx := infra.WithFreshTaskInventory(infra.WithObservationCache(infra.ObservationContext(t.Context()), new(sync.Map)))
	beforeEnvironment, beforeApplication := fakeCalls(f, "environment.one"), fakeCalls(f, "application.one")
	beforeTasks := fakeCalls(f, "docker.getServiceContainersByAppName")
	var workers sync.WaitGroup
	for range 12 {
		workers.Go(func() {
			containers, err := b.Containers(ctx, "runtime")
			if err != nil || len(containers) != 1 {
				t.Errorf("current task inventory: count=%d err=%v", len(containers), err)
			}
		})
	}
	workers.Wait()
	if fakeCalls(f, "environment.one")-beforeEnvironment != 1 || fakeCalls(f, "application.one")-beforeApplication != 1 {
		t.Fatal("same-pass ownership facts were not single-flighted")
	}
	if got := fakeCalls(f, "docker.getServiceContainersByAppName") - beforeTasks; got != 12 {
		t.Fatalf("task authorization reused stale inventory: reads=%d", got)
	}
	removeFakeService(f, imported.Services["runtime"])
	if containers, err := b.Containers(ctx, "runtime"); err != nil || len(containers) != 0 {
		t.Fatal("removed task remained authorized within the same pass", containers, err)
	}
	f.mu.Lock()
	f.environments["foreign"] = map[string]any{"environmentId": "foreign", "projectId": "project", "name": "foreign"}
	f.apps[imported.Services["runtime"]].row["environmentId"] = "foreign"
	f.mu.Unlock()
	beforeUpdate, beforeReload := fakeCalls(f, "application.update"), fakeCalls(f, "application.reload")
	if _, err := b.GetService(ctx, "runtime"); !errors.Is(err, infra.ErrConflict) {
		t.Fatal("recovery trusted cached ownership instead of locked fresh reload", err)
	}
	if fakeCalls(f, "application.update") != beforeUpdate || fakeCalls(f, "application.reload") != beforeReload {
		t.Fatal("changed ownership reached a mutation")
	}
	if _, err := b.application(infra.WithoutObservation(ctx), "runtime"); !errors.Is(err, infra.ErrConflict) {
		t.Fatal("lifecycle lookup reused observation ownership", err)
	}
	if _, err := b.application(infra.ObservationContext(infra.WithoutObservation(ctx)), "runtime"); !errors.Is(err, infra.ErrConflict) {
		t.Fatal("re-entering observation resurrected lifecycle's old ownership snapshot", err)
	}
	fresh := infra.WithObservationCache(infra.ObservationContext(t.Context()), new(sync.Map))
	if _, err := b.Containers(fresh, "runtime"); !errors.Is(err, infra.ErrConflict) {
		t.Fatal("new pass inherited an old ownership snapshot", err)
	}
}

func TestLifecycleRejectsCachedDiscoveryAfterOwnershipChange(t *testing.T) {
	for _, operation := range []string{"stop", "deploy"} {
		for _, change := range []string{"name", "environment", "metadata-environment"} {
			t.Run(operation+"/"+change, func(t *testing.T) {
				f := newFakeAPI(t)
				b := fakeBackend(t, f, nil)
				id := importFake(t, b, runtimeSpec("runtime", "")).Services["runtime"]
				deployFake(t, b, "runtime", "")
				ctx := infra.WithObservationCache(infra.ObservationContext(t.Context()), new(sync.Map))
				if _, err := b.Containers(ctx, "runtime"); err != nil {
					t.Fatal(err)
				}
				f.mu.Lock()
				switch change {
				case "name":
					f.apps[id].row["name"] = "foreign"
				case "environment":
					f.environments["foreign"] = map[string]any{"environmentId": "foreign", "projectId": "project", "name": "foreign"}
					f.apps[id].row["environmentId"] = "foreign"
				default:
					m, _ := parseMetadata(fakeString(f.apps[id].row, "description"))
					m.Environment = "foreign"
					f.apps[id].row["description"] = m.description()
				}
				mutations := 0
				f.requestHook = func(_ http.ResponseWriter, r *http.Request) (bool, func()) {
					f.mu.Lock()
					defer f.mu.Unlock()
					if r.Method != http.MethodGet {
						mutations++
					}
					return false, nil
				}
				f.mu.Unlock()
				var err error
				if operation == "stop" {
					_, err = b.Stop(ctx, "runtime")
				} else {
					_, err = b.Deploy(ctx, "runtime", testDeployment(t, ""))
				}
				f.mu.Lock()
				writes := mutations
				f.mu.Unlock()
				if !errors.Is(err, infra.ErrConflict) || writes != 0 {
					t.Fatalf("cached discovery authorized lifecycle mutation: err=%v writes=%d", err, writes)
				}
			})
		}
	}
}

// Signal the precise point at which a follower starts waiting on a shared call.
type observationWaitingContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *observationWaitingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestObservationSnapshotLeaderCancellationDoesNotPoisonWaiter(t *testing.T) {
	ctx, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	ctx = infra.WithObservationCache(infra.ObservationContext(ctx), new(sync.Map))
	leader, cancelLeader := context.WithCancel(ctx)
	started := make(chan struct{})
	leaderResult := make(chan error, 1)
	key := observationKey{kind: "cancellation"}
	go func() {
		_, err := observationValue(leader, key, func() (int, error) {
			close(started)
			<-leader.Done()
			return 0, leader.Err()
		})
		leaderResult <- err
	}()
	<-started
	waiter := &observationWaitingContext{Context: ctx, waiting: make(chan struct{})}
	waiterResult := make(chan error, 1)
	go func() {
		value, err := observationValue(waiter, key, func() (int, error) { return 42, nil })
		if err == nil && value != 42 {
			err = fmt.Errorf("unexpected value: %d", value)
		}
		waiterResult <- err
	}()
	<-waiter.waiting
	cancelLeader()
	if err := <-leaderResult; !errors.Is(err, context.Canceled) {
		t.Fatal("leader cancellation lost", err)
	}
	if err := <-waiterResult; err != nil {
		t.Fatal("live waiter inherited another caller's cancellation", err)
	}
}

func TestObservationSnapshotWaiterCancellationDoesNotCancelLeader(t *testing.T) {
	ctx, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	ctx = infra.WithObservationCache(infra.ObservationContext(ctx), new(sync.Map))
	started, release := make(chan struct{}), make(chan struct{})
	leaderResult := make(chan error, 1)
	key := observationKey{kind: "cancellation"}
	go func() {
		_, err := observationValue(ctx, key, func() (int, error) {
			close(started)
			select {
			case <-release:
				return 42, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		})
		leaderResult <- err
	}()
	<-started
	waiterCtx, cancelWaiter := context.WithCancel(ctx)
	waiter := &observationWaitingContext{Context: waiterCtx, waiting: make(chan struct{})}
	waiterResult := make(chan error, 1)
	go func() {
		_, err := observationValue(waiter, key, func() (int, error) { return 0, errors.New("waiter unexpectedly became leader") })
		waiterResult <- err
	}()
	<-waiter.waiting
	cancelWaiter()
	if err := <-waiterResult; !errors.Is(err, context.Canceled) {
		t.Fatal("waiter ignored cancellation", err)
	}
	close(release)
	if err := <-leaderResult; err != nil {
		t.Fatal("waiter cancellation aborted leader", err)
	}
	value, err := observationValue(ctx, key, func() (int, error) { return 0, errors.New("successful read was not cached") })
	if err != nil || value != 42 {
		t.Fatal("successful shared result lost", value, err)
	}
}
