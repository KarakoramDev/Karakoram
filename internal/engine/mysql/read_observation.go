// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type mysqlReadModeKey struct{}
type mysqlQuorumReadKey struct{}
type mysqlReadScopeKey struct{ namespace, cluster string }
type mysqlReadKey struct{ member, instance, action string }

type mysqlReadMember struct {
	name, service, volume, server, role, stage string
	config                                     int64
	offline, removing                          bool
}

type mysqlReadIdentity struct {
	generation, config, restoreGeneration                           int64
	epoch                                                           uint64
	uid, configHash, group, formation, fence, restoreID, checkpoint string
}

type mysqlReadCall struct {
	done  chan struct{}
	value any
	at    time.Time
	err   error
}

type mysqlReadScope struct {
	mu                  sync.Mutex
	deadline            time.Time
	quorumDeadline      time.Time
	identity            mysqlReadIdentity
	members             []mysqlReadMember
	initialized         bool
	calls               map[mysqlReadKey]*mysqlReadCall
	optionalAttempts    map[mysqlReadKey]int
	hasSample           bool
	identityInvalidated bool
}

// Fresh actor reads are deliberately outside the read-only pass's budget and
// memo. Mutation boundaries must additionally invalidateMySQLReadObservation.
func freshMySQLReads(ctx context.Context) context.Context {
	return context.WithValue(ctx, mysqlReadModeKey{}, true)
}

// Status is read-only even when its caller is an imperative actor.
func normalMySQLReads(ctx context.Context) context.Context {
	return context.WithValue(ctx, mysqlReadModeKey{}, false)
}

func mysqlReadsFresh(ctx context.Context) bool {
	fresh, _ := ctx.Value(mysqlReadModeKey{}).(bool)
	return fresh
}

func mysqlObservationScope(ctx context.Context, rc *engine.Context) *mysqlReadScope {
	cache := engine.ObservationPassCache(ctx)
	if cache == nil {
		return nil
	}
	key := mysqlReadScopeKey{rc.Cluster.Namespace, rc.Cluster.Name}
	if value, ok := cache.Load(key); ok {
		return value.(*mysqlReadScope)
	}
	value, _ := cache.LoadOrStore(key, new(mysqlReadScope))
	return value.(*mysqlReadScope)
}

// mysqlReadContext bounds network and lock waits by one absolute four-second
// deadline per cluster/pass, not four seconds per caller. Successful memo hits
// remain usable without issuing network calls after that budget is exhausted.
func mysqlReadContext(ctx context.Context, rc *engine.Context) (context.Context, context.CancelFunc) {
	if mysqlReadsFresh(ctx) {
		probe, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
		return infra.ObservationContext(probe), cancel
	}
	engine.ObservationPassStart(ctx, now(rc))
	scope := mysqlObservationScope(ctx, rc)
	if scope == nil {
		probe, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
		return infra.ObservationContext(probe), cancel
	}
	// Continuity gets its own bounded reservation, not the leftovers of native
	// readiness/platform reads. Repeated callers still share one absolute budget.
	quorum, _ := ctx.Value(mysqlQuorumReadKey{}).(bool)
	scope.mu.Lock()
	budget := &scope.deadline
	if quorum {
		budget = &scope.quorumDeadline
	}
	if budget.IsZero() {
		*budget = time.Now().Add(engine.ObservationTimeout)
	}
	deadline := *budget
	scope.mu.Unlock()
	probe, cancel := context.WithDeadline(ctx, deadline)
	return infra.ObservationContext(probe), cancel
}

// Invalidation discards in-flight and completed payloads, never the pass budget.
// Old in-flight results cannot repopulate the new map after a native mutation.
func invalidateMySQLReadObservation(ctx context.Context, rc *engine.Context) {
	cache := engine.ObservationPassCache(ctx)
	if cache == nil {
		return
	}
	cache.Delete(gtidObservationKey{rc.Cluster.Namespace, rc.Cluster.Name})
	if value, ok := cache.Load(mysqlReadScopeKey{rc.Cluster.Namespace, rc.Cluster.Name}); ok {
		scope := value.(*mysqlReadScope)
		scope.mu.Lock()
		scope.identityInvalidated = scope.identityInvalidated || scope.hasSample
		scope.calls = nil
		scope.initialized = false
		scope.mu.Unlock()
	}
}

func mysqlReadState(rc *engine.Context) (mysqlReadIdentity, *state, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return mysqlReadIdentity{}, nil, err
	}
	identity := mysqlReadIdentity{
		generation: rc.Cluster.Generation, config: s.ConfigVersion,
		epoch: s.Formation.FormationEpoch, uid: s.UID, configHash: s.ConfigHash,
		group: s.Formation.GroupName, formation: s.Formation.State, fence: s.RestoreGTIDFence,
	}
	backup, err := loadBackupState(rc.Cluster)
	if err != nil {
		return identity, nil, err
	}
	if op := backup.RestoreOperation; op != nil {
		identity.restoreID, identity.checkpoint = op.ID, string(op.Checkpoint)
		identity.restoreGeneration = op.TargetGeneration
	}
	return identity, s, nil
}

func mysqlReadMemberIdentity(m *member) mysqlReadMember {
	return mysqlReadMember{m.Name, m.ServiceID, m.VolumeID, m.ServerUUID, m.Role, m.Stage, m.ConfigVersion, m.Offline, m.Removing}
}

func (scope *mysqlReadScope) match(identity mysqlReadIdentity, s *state) bool {
	if !scope.initialized || scope.identity != identity || len(scope.members) != len(s.Members) {
		return false
	}
	for i, m := range s.Members {
		if scope.members[i] != mysqlReadMemberIdentity(m) {
			return false
		}
	}
	return true
}

// Network calls never hold scope.mu. Each entry single-flights one endpoint,
// allowing unrelated metadata/native/member reads to proceed concurrently.
// Payloads (including agent maps and proxy slices) are immutable after decode.
func mysqlReadCachedValue(ctx context.Context, rc *engine.Context, key mysqlReadKey, read func(context.Context) (any, error)) (any, time.Time, error) {
	scope := mysqlObservationScope(ctx, rc)
	if scope == nil || mysqlReadsFresh(ctx) {
		probe, cancel := mysqlReadContext(ctx, rc)
		defer cancel()
		at := now(rc)
		value, err := read(probe)
		return value, at, err
	}
	identity, s, err := mysqlReadState(rc)
	if err != nil {
		return nil, time.Time{}, err
	}
	scope.mu.Lock()
	if !scope.match(identity, s) {
		scope.identityInvalidated = scope.identityInvalidated || scope.initialized && scope.hasSample
		scope.identity, scope.initialized = identity, true
		scope.members = make([]mysqlReadMember, len(s.Members))
		for i, m := range s.Members {
			scope.members[i] = mysqlReadMemberIdentity(m)
		}
		scope.calls = make(map[mysqlReadKey]*mysqlReadCall)
		scope.optionalAttempts = nil
	}
	if call, ok := scope.calls[key]; ok {
		scope.mu.Unlock()
		// Prefer an already completed sample over an expired read budget. This
		// performs no work and never changes the original sample timestamp.
		select {
		case <-call.done:
			return call.value, call.at, call.err
		default:
		}
		probe, cancel := mysqlReadContext(ctx, rc)
		defer cancel()
		select {
		case <-call.done:
			return call.value, call.at, call.err
		case <-probe.Done():
			return nil, time.Time{}, scope.readError(probe.Err())
		}
	}
	// Optional continuity sampling may retry one fast incomplete sweep, but an
	// unsupported old-agent endpoint must not be queried at every callsite.
	if key.action == myspec.ActionGaleraStatus {
		if scope.optionalAttempts[key] >= 2 {
			scope.mu.Unlock()
			return nil, time.Time{}, engine.ErrUnavailable
		}
		if scope.optionalAttempts == nil {
			scope.optionalAttempts = make(map[mysqlReadKey]int)
		}
		scope.optionalAttempts[key]++
	}
	call := &mysqlReadCall{done: make(chan struct{})}
	scope.calls[key] = call
	scope.mu.Unlock()
	probe, cancel := mysqlReadContext(ctx, rc)
	defer cancel()
	at := now(rc)
	var value any
	if err = probe.Err(); err == nil {
		value, err = read(probe)
	}
	err = scope.readError(err)
	scope.mu.Lock()
	if err == nil {
		scope.hasSample = true
	}
	call.value, call.at, call.err = value, at, err
	if err != nil && scope.calls[key] == call {
		delete(scope.calls, key)
	}
	close(call.done)
	scope.mu.Unlock()
	return value, at, err
}

// An initial full-length failed probe is still a real observation failure.
// Only a shortened remainder after identity/proof invalidation is a deferral.
func (scope *mysqlReadScope) readError(err error) error {
	if !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	scope.mu.Lock()
	deferred := scope.identityInvalidated && !scope.deadline.IsZero() && !time.Now().Before(scope.deadline)
	scope.mu.Unlock()
	if deferred {
		translated, _ := deferReadDeadlines(err)
		return translated
	}
	return err
}

type deferredReadCause struct {
	original error
	cause    error
}

func (e *deferredReadCause) Error() string { return e.original.Error() }
func (e *deferredReadCause) Unwrap() error { return e.cause }

// Translate only deadline leaves. Aggregates can contain both an actual HTTP
// failure and an exhausted sibling; replacing the aggregate would erase failure
// evidence before the provider can classify its fresh negative observations.
func deferReadDeadlines(err error) (error, bool) {
	if err == context.DeadlineExceeded {
		return engine.ErrObservationBudgetExhausted, true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		var causes []error
		for i, child := range children {
			cause, changed := deferReadDeadlines(child)
			if changed {
				if causes == nil {
					causes = append([]error(nil), children...)
				}
				causes[i] = cause
			}
		}
		if causes != nil {
			return errors.Join(causes...), true
		}
		return err, false
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		cause, changed := deferReadDeadlines(wrapped.Unwrap())
		if changed {
			return &deferredReadCause{original: err, cause: cause}, true
		}
	}
	return err, false
}

func mysqlAgentStatus(ctx context.Context, rc *engine.Context, member string) (agentapi.Status, time.Time, error) {
	return mysqlReadStatus(ctx, rc, member, "", nil)
}

func mysqlReadService(ctx context.Context, rc *engine.Context, name string) (*infra.Service, error) {
	service, _, err := mysqlReadValue(ctx, rc, name, "platform-service", func(probe context.Context) (*infra.Service, error) {
		return rc.Backend.GetService(probe, name)
	})
	return service, err
}

func mysqlReadClientStatus(ctx context.Context, rc *engine.Context, member string, client *agentapi.Client) (agentapi.Status, time.Time, error) {
	return mysqlReadStatus(ctx, rc, member, client.ObservationInstance(), client)
}

func mysqlReadStatus(ctx context.Context, rc *engine.Context, member, instance string, client *agentapi.Client) (agentapi.Status, time.Time, error) {
	value, at, err := mysqlReadCachedValue(ctx, rc, mysqlReadKey{member, instance, "agent-status"}, func(probe context.Context) (any, error) {
		if client == nil {
			if rc.Agents == nil {
				return nil, engine.ErrUnavailable
			}
			var err error
			client, err = rc.Agents.Dial(probe, rc.Cluster.Ref(), member)
			if err != nil {
				return nil, err
			}
		}
		return client.Status(probe)
	})
	if err != nil {
		return agentapi.Status{}, at, err
	}
	return value.(agentapi.Status), at, nil
}

func mysqlReadAction(ctx context.Context, rc *engine.Context, member, action string, request, response any) error {
	_, err := mysqlReadActionAt(ctx, rc, member, action, request, response)
	return err
}

func mysqlReadActionAt(ctx context.Context, rc *engine.Context, member, action string, request, response any) (time.Time, error) {
	return mysqlReadClientActionAt(ctx, rc, member, nil, action, request, response)
}

// Only no-argument native read actions may enter this cache. The integration
// dispatches its explicit read whitelist here; mutation actions never do.
func mysqlReadClientAction(ctx context.Context, rc *engine.Context, member string, client *agentapi.Client, action string, request, response any) error {
	_, err := mysqlReadClientActionAt(ctx, rc, member, client, action, request, response)
	return err
}

func mysqlReadClientActionAt(ctx context.Context, rc *engine.Context, member string, client *agentapi.Client, action string, request, response any) (time.Time, error) {
	instance := ""
	if client != nil {
		instance = client.ObservationInstance()
	}
	value, at, err := mysqlReadCachedValue(ctx, rc, mysqlReadKey{member, instance, action}, func(probe context.Context) (any, error) {
		if client == nil {
			if rc.Agents == nil {
				return nil, engine.ErrUnavailable
			}
			var err error
			client, err = rc.Agents.Dial(probe, rc.Cluster.Ref(), member)
			if err != nil {
				return nil, err
			}
		}
		if err := client.Engine(probe, "mysql", action, request, response); err != nil {
			return nil, err
		}
		switch out := response.(type) {
		case *myspec.StatusResponse:
			return *out, nil
		case *myspec.GaleraStatusResponse:
			return *out, nil
		case *myspec.ProxyStatusResponse:
			return *out, nil
		case *myspec.BinlogArchiveStatus:
			return *out, nil
		default:
			return nil, engine.ErrNotSupported
		}
	})
	if err != nil {
		return at, err
	}
	switch out := response.(type) {
	case *myspec.StatusResponse:
		*out = value.(myspec.StatusResponse)
	case *myspec.GaleraStatusResponse:
		*out = value.(myspec.GaleraStatusResponse)
	case *myspec.ProxyStatusResponse:
		*out = value.(myspec.ProxyStatusResponse)
	case *myspec.BinlogArchiveStatus:
		*out = value.(myspec.BinlogArchiveStatus)
	default:
		return at, engine.ErrNotSupported
	}
	return at, nil
}

// mysqlReadValue memoizes an aggregate read under the same identity, deadline
// and single-flight rules as individual RPCs. Fetch must not mutate native state.
func mysqlReadValue[T any](ctx context.Context, rc *engine.Context, member, action string, fetch func(context.Context) (T, error)) (T, time.Time, error) {
	value, at, err := mysqlReadCachedValue(ctx, rc, mysqlReadKey{member: member, action: action}, func(probe context.Context) (any, error) {
		return fetch(probe)
	})
	if err != nil {
		var zero T
		return zero, at, err
	}
	return value.(T), at, nil
}
