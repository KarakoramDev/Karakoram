// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestMutationLockRecoveryIsolation(t *testing.T) {
	p, a, backend := setup(t, "group")
	converge(t, p, a, backend)
	suspendGroup(t, p, a, backend)
	_, b, otherBackend := setup(t, "single")
	b.Cluster.Name = "other"
	if err := b.Store.(*store.Store).CreateCluster(t.Context(), b.Cluster); err != nil {
		t.Fatal(err)
	}
	converge(t, p, b, otherBackend)
	installAccessAgent(t, b, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/mysql/status":
			json.NewEncoder(w).Encode(myspec.StatusResponse{MemberState: "ONLINE", MemberRole: "PRIMARY"})
		default:
			json.NewEncoder(w).Encode(struct{}{})
		}
	})
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	backend.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path != "/v1/mysql/offline-gtid" {
			return false
		}
		once.Do(func() { close(entered) })
		select {
		case <-release:
		case <-r.Context().Done():
			return true
		}
		json.NewEncoder(w).Encode(backend.offline[name])
		return true
	}
	done := make(chan error, 1)
	go func() {
		for range 60 {
			if _, err := p.Sync(t.Context(), a); err != nil {
				done <- err
				return
			}
			backend.apply(t, p, a)
			select {
			case <-entered:
				done <- nil
				return
			default:
			}
		}
		done <- engine.ErrUnavailable
	}()
	var pending chan error
	defer func() {
		close(release)
		if err := <-done; err != nil {
			t.Error(err)
		}
		if pending != nil {
			<-pending
		}
	}()
	select {
	case <-entered:
	case err := <-done:
		done <- err
		t.Fatalf("recovery did not reach native probe: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("recovery did not reach native probe")
	}
	mutation := make(chan error, 1)
	go func() { mutation <- p.CreateDatabase(t.Context(), b, Database{Name: "app"}) }()
	pending = mutation
	select {
	case err := <-mutation:
		pending = nil
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cluster A recovery blocked cluster B access mutation")
	}
	operation := make(chan error, 1)
	go func() {
		_, err := p.RunOperation(t.Context(), b, &domain.Operation{Kind: "unsupported"})
		operation <- err
	}()
	pending = operation
	select {
	case err := <-operation:
		pending = nil
		if err != engine.ErrNotSupported {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cluster A recovery blocked cluster B operation")
	}
}

func TestMutationLockSameCluster(t *testing.T) {
	p, rc, backend := setup(t, "single")
	converge(t, p, rc, backend)
	entered, release := make(chan struct{}), make(chan struct{})
	installAccessAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/mysql/status" {
			json.NewEncoder(w).Encode(myspec.StatusResponse{MemberState: "ONLINE", MemberRole: "PRIMARY"})
			return
		}
		close(entered)
		<-release
		json.NewEncoder(w).Encode(struct{}{})
	})
	done := make(chan error, 1)
	go func() { done <- p.CreateDatabase(t.Context(), rc, Database{Name: "app"}) }()
	defer func() {
		close(release)
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first mutation did not enter agent")
	}
	calls := []func(context.Context) error{
		func(ctx context.Context) error { return p.CreateDatabase(ctx, rc, Database{Name: "second"}) },
		func(ctx context.Context) error { _, err := p.mutateUser(ctx, rc, myspec.AccessRequest{}); return err },
		func(ctx context.Context) error {
			_, err := p.RunOperation(ctx, rc, &domain.Operation{Kind: "unsupported"})
			return err
		},
		func(ctx context.Context) error { _, err := p.Sync(ctx, rc); return err },
		func(ctx context.Context) error { _, err := p.Cleanup(ctx, rc, domain.DeleteOptions{}); return err },
		func(ctx context.Context) error {
			return p.DeleteRetainedBackup(ctx, domain.BackupStorage{}, domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Provider: "mysql"})
		},
		func(ctx context.Context) error {
			_, err := p.CancelOperation(ctx, rc, &domain.Operation{Kind: domain.OperationUpgradeAgents})
			return err
		},
		func(ctx context.Context) error {
			_, err := p.CancelOperation(ctx, rc, &domain.Operation{Kind: domain.OperationReplaceMember})
			return err
		},
	}
	for i, call := range calls {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
		err := call(ctx)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("mutation %d bypassed cluster gate: %v", i, err)
		}
	}
}

func TestMutationLockEntriesReleased(t *testing.T) {
	var locks clusterMutationLocks
	ref := domain.ClusterRef{Name: "demo"}
	held, err := locks.acquire(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if _, err := locks.acquire(ctx, domain.ClusterRef{Namespace: "default", Name: "demo"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("default namespace aliases did not serialize", err)
	}
	locks.mu.Lock()
	if len(locks.entries) != 1 || held.refs != 1 {
		t.Error("canceled waiter leaked a reference")
	}
	locks.mu.Unlock()
	locks.release(held)
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Go(func() {
			for range 10 {
				lock, err := locks.acquire(t.Context(), domain.ClusterRef{Namespace: fmt.Sprint(i % 5), Name: "demo"})
				if err != nil {
					t.Error(err)
					return
				}
				locks.release(lock)
			}
		})
	}
	wg.Wait()
	locks.mu.Lock()
	defer locks.mu.Unlock()
	if len(locks.entries) != 0 {
		t.Fatalf("lock entries leaked: %d", len(locks.entries))
	}
}

type mutationLockStore struct {
	engine.ClusterStore
	enter func()
}

func (s mutationLockStore) ListBackups(context.Context, domain.ClusterRef) ([]domain.Backup, error) {
	s.enter()
	return nil, engine.ErrUnavailable
}
func (s mutationLockStore) GetBackup(context.Context, domain.ClusterRef, string) (*domain.Backup, error) {
	s.enter()
	return nil, engine.ErrUnavailable
}
func (s mutationLockStore) GetRestore(context.Context, domain.ClusterRef, string) (*domain.Restore, error) {
	s.enter()
	return nil, engine.ErrUnavailable
}

func TestMutationLockBackupCleanup(t *testing.T) {
	for _, mode := range []string{"retained", "cleanup", "delete", "backup", "restore"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, backend := setup(t, "single")
			converge(t, p, rc, backend)
			entered, release := make(chan struct{}), make(chan struct{})
			st := mutationLockStore{ClusterStore: rc.Store, enter: func() { close(entered); <-release }}
			rc.Store, p.options.Store = st, st
			done := make(chan error, 1)
			go func() {
				var err error
				switch mode {
				case "retained":
					err = p.DeleteRetainedBackup(t.Context(), domain.BackupStorage{}, domain.Backup{Namespace: "default", Cluster: "retired", Provider: "mysql"})
				case "cleanup":
					_, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
					// Cleanup can have runtime deletion checkpoints before repository cleanup.
					for err == nil {
						var finished bool
						finished, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
						if finished {
							break
						}
					}
				case "delete":
					_, err = p.RunOperation(t.Context(), rc, &domain.Operation{Kind: domain.OperationDeleteBackup, Payload: json.RawMessage(`{"backup":"base"}`)})
				case "backup":
					_, err = p.RunOperation(t.Context(), rc, &domain.Operation{Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"base"}`)})
				case "restore":
					_, err = p.RunOperation(t.Context(), rc, &domain.Operation{Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"restore"}`)})
				}
				done <- err
			}()
			defer func() {
				close(release)
				if err := <-done; !errors.Is(err, engine.ErrUnavailable) {
					t.Error(err)
				}
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("storage step did not enter")
			}
			if p.backupCleanupMu.TryLock() {
				p.backupCleanupMu.Unlock()
				t.Fatal("storage step did not exclude deletion")
			}
			read := p.backupCleanupMu.TryRLock()
			if read {
				p.backupCleanupMu.RUnlock()
			}
			if read != (mode == "backup" || mode == "restore") {
				t.Fatal("incorrect storage cleanup lock mode", mode)
			}
		})
	}
}
