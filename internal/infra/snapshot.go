// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra

import (
	"context"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"weak"
)

// SnapshotListingInterval bounds out-of-band drift visible in the target listing.
const SnapshotListingInterval = time.Minute

// SnapshotMaxAge outlives one steady deep revalidation (engine.SteadyResyncInterval,
// 30 min) so steady observations do not refetch between reconciles; the listing,
// mutation epochs and agent identity changes invalidate entries earlier.
const SnapshotMaxAge = 40 * time.Minute
const snapshotRetryInterval = 5 * time.Second
const snapshotMaxEntries = 4096

// SnapshotMaxBody bounds retained bytes for one successful platform response.
const SnapshotMaxBody = 4 << 20

// SnapshotMaxBytes bounds all retained response bodies for one shared target.
const SnapshotMaxBytes = 64 << 20

const snapshotPruneInterval = time.Minute

type requestCallerKey struct{}

const (
	CallerOther             = "other"
	CallerListing           = "listing"
	CallerReconcile         = "reconcile"
	CallerOperation         = "operation"
	CallerAvailability      = "availability"
	CallerBackoffObserve    = "backoff-observe"
	CallerDialAuthorization = "dial-authorization"
	CallerNamespace         = "namespace"
	CallerOrphanJanitor     = "orphan-janitor"
	CallerAPI               = "api"
)

var requestCallers = [...]string{
	CallerOther, CallerListing, CallerReconcile, CallerOperation,
	CallerAvailability, CallerBackoffObserve, CallerDialAuthorization,
	CallerNamespace, CallerOrphanJanitor, CallerAPI,
}

// WithRequestCaller tags platform traffic with a bounded caller class. Unknown
// classes become "other"; the innermost tag always wins.
func WithRequestCaller(ctx context.Context, class string) context.Context {
	index := 0
	for i, name := range requestCallers {
		if name == class {
			index = i
			break
		}
	}
	return context.WithValue(ctx, requestCallerKey{}, index)
}

func requestCallerIndex(ctx context.Context) int {
	index, _ := ctx.Value(requestCallerKey{}).(int)
	return index
}

func RequestCaller(ctx context.Context) string {
	return requestCallers[requestCallerIndex(ctx)]
}

type steadyObservationKey struct{}
type evidenceKey struct{}

func WithSteadyObservation(ctx context.Context) context.Context {
	return context.WithValue(ctx, steadyObservationKey{}, true)
}
func WithoutSteadyObservation(ctx context.Context) context.Context {
	marked, _ := ctx.Value(steadyObservationKey{}).(bool)
	if !marked {
		return ctx
	}
	return context.WithValue(ctx, steadyObservationKey{}, false)
}
func SteadyObservation(ctx context.Context) bool {
	marked, _ := ctx.Value(steadyObservationKey{}).(bool)
	return marked && IsObservation(ctx) && !FreshTaskInventory(ctx)
}

type snapshotProof struct{ epoch, drift uint64 }

// Evidence belongs to one observation attempt, including its parallel probes.
type Evidence struct {
	mu      sync.Mutex
	members map[string]time.Time
	targets map[*TargetSnapshot]snapshotProof
	invalid bool
}

func WithEvidence(ctx context.Context) (context.Context, *Evidence) {
	e := &Evidence{}
	return context.WithValue(ctx, evidenceKey{}, e), e
}
func NoteAgentSample(ctx context.Context, member string, sampledAt time.Time) {
	e, _ := ctx.Value(evidenceKey{}).(*Evidence)
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.members == nil {
		e.members = make(map[string]time.Time)
	}
	if old, ok := e.members[member]; !ok || sampledAt.Before(old) {
		e.members[member] = sampledAt
	}
}
func (e *Evidence) Oldest() (time.Time, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	var oldest time.Time
	found := false
	for _, at := range e.members {
		if !found || at.Before(oldest) {
			oldest, found = at, true
		}
	}
	return oldest, found
}
func (e *Evidence) MemberOldest(member string) (time.Time, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	at, ok := e.members[member]
	return at, ok
}
func (e *Evidence) SnapshotValid() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.invalid {
		return false
	}
	for s, proof := range e.targets {
		s.mu.Lock()
		valid := s.epoch == proof.epoch && s.inFlight == 0 && s.drift == proof.drift
		s.mu.Unlock()
		if !valid {
			return false
		}
	}
	return true
}
func (e *Evidence) DriftGeneration() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	var drift uint64
	for _, proof := range e.targets {
		drift = max(drift, proof.drift)
	}
	return drift
}
func (e *Evidence) note(s *TargetSnapshot, proof snapshotProof) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.targets == nil {
		e.targets = make(map[*TargetSnapshot]snapshotProof)
	}
	if old, ok := e.targets[s]; ok {
		if old != proof {
			e.invalid = true
		}
		proof.epoch = old.epoch
		proof.drift = max(old.drift, proof.drift)
	}
	e.targets[s] = proof
}

type snapshotEntry struct {
	body  []byte
	at    time.Time
	epoch uint64
}

type TargetSnapshot struct {
	metrics                              *platformCounters
	mu                                   sync.Mutex
	entries                              map[string]snapshotEntry
	entryBytes                           int
	nextPrune                            time.Time
	epoch, drift                         uint64
	inFlight                             int
	listing                              func(context.Context) (string, error)
	listingVersion                       uint64
	checking                             chan struct{}
	checkedAt, attemptedAt               time.Time
	listingOK, attempted, fingerprintSet bool
	fingerprint                          string
}

var platformSnapshots = struct {
	sync.Mutex
	values map[string]weak.Pointer[TargetSnapshot]
}{values: make(map[string]weak.Pointer[TargetSnapshot])}

type platformCounters struct {
	requests                 [len(requestCallers)]platformRequestCounters
	unchanged, drift, errors atomic.Uint64
}

type platformRequestCounters struct{ reads, hits, mutations atomic.Uint64 }

type PlatformRequestCounterValues struct {
	Caller                 string
	Reads, Hits, Mutations uint64
}

// PlatformCounterValues aggregates process-lifetime traffic by provider and caller.
type PlatformCounterValues struct {
	Provider                 string
	Requests                 [len(requestCallers)]PlatformRequestCounterValues
	Unchanged, Drift, Errors uint64
}

var platformCounterRegistry = make(map[string]*platformCounters) // guarded by platformSnapshots

func PlatformCounters() []PlatformCounterValues {
	platformSnapshots.Lock()
	defer platformSnapshots.Unlock()
	values := make([]PlatformCounterValues, 0, len(platformCounterRegistry))
	for provider, c := range platformCounterRegistry {
		value := PlatformCounterValues{Provider: provider, Unchanged: c.unchanged.Load(), Drift: c.drift.Load(), Errors: c.errors.Load()}
		for i, caller := range requestCallers {
			r := &c.requests[i]
			value.Requests[i] = PlatformRequestCounterValues{caller, r.reads.Load(), r.hits.Load(), r.mutations.Load()}
		}
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Provider < values[j].Provider })
	return values
}

func PlatformSnapshot(key string) *TargetSnapshot {
	platformSnapshots.Lock()
	defer platformSnapshots.Unlock()
	if s := platformSnapshots.values[key].Value(); s != nil {
		return s
	}
	provider, _, _ := strings.Cut(key, "|")
	counters := platformCounterRegistry[provider]
	if counters == nil {
		counters = &platformCounters{}
		platformCounterRegistry[provider] = counters
	}
	s := &TargetSnapshot{metrics: counters}
	pointer := weak.Make(s)
	platformSnapshots.values[key] = pointer
	// The cleanup argument must not hold the target or its listing client alive.
	runtime.AddCleanup(s, func(entry snapshotRegistration) {
		platformSnapshots.Lock()
		defer platformSnapshots.Unlock()
		if platformSnapshots.values[entry.key] == entry.pointer {
			delete(platformSnapshots.values, entry.key)
		}
	}, snapshotRegistration{key, pointer})
	return s
}

type snapshotRegistration struct {
	key     string
	pointer weak.Pointer[TargetSnapshot]
}

func (s *TargetSnapshot) SetListing(check func(context.Context) (string, error)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listing = check
	s.listingVersion++
}

var snapshotClock = struct {
	sync.RWMutex
	now func() time.Time
}{now: time.Now}

// SnapshotNow is the shared clock for request-start timestamps and snapshot age.
func SnapshotNow() time.Time {
	snapshotClock.RLock()
	now := snapshotClock.now
	snapshotClock.RUnlock()
	return now()
}
func SetSnapshotClockForTest(now func() time.Time) (restore func()) {
	snapshotClock.Lock()
	previous := snapshotClock.now
	snapshotClock.now = now
	snapshotClock.Unlock()
	return func() { snapshotClock.Lock(); snapshotClock.now = previous; snapshotClock.Unlock() }
}

// ensureListing does not launch workers: the first caller checks and waiters can
// abandon the shared flight independently when their own sweep expires.
func (s *TargetSnapshot) ensureListing(ctx context.Context) bool {
	for {
		now := SnapshotNow()
		s.mu.Lock()
		if s.inFlight != 0 {
			s.mu.Unlock()
			return false
		}
		if s.checking != nil {
			flight := s.checking
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return false
			case <-flight:
				continue
			}
		}
		if s.listingOK && !now.Before(s.checkedAt) && now.Sub(s.checkedAt) < SnapshotListingInterval {
			s.mu.Unlock()
			return true
		}
		if s.listing == nil || s.attempted && now.Sub(s.attemptedAt) < snapshotRetryInterval {
			s.mu.Unlock()
			return false
		}
		check, epoch, version := s.listing, s.epoch, s.listingVersion
		flight := make(chan struct{})
		s.checking, s.attemptedAt, s.attempted = flight, now, true
		s.mu.Unlock()
		fingerprint, err := check(WithRequestCaller(WithoutSteadyObservation(ctx), CallerListing))
		s.mu.Lock()
		s.listingOK = err == nil && ctx.Err() == nil && s.epoch == epoch && s.inFlight == 0 && version == s.listingVersion
		if s.metrics != nil {
			switch {
			case !s.listingOK:
				s.metrics.errors.Add(1)
			case s.fingerprintSet && s.fingerprint != fingerprint:
				s.metrics.drift.Add(1)
			default:
				s.metrics.unchanged.Add(1)
			}
		}
		if s.listingOK {
			if !s.fingerprintSet || s.fingerprint != fingerprint {
				s.entries = nil
				s.entryBytes = 0
				if s.fingerprintSet {
					s.drift++
				}
				// A first baseline cannot certify earlier reads; a changed
				// listing cannot certify facts from the preceding baseline.
				s.epoch++
			}
			s.fingerprint, s.fingerprintSet, s.checkedAt = fingerprint, true, SnapshotNow()
		}
		ok := s.listingOK
		s.checking = nil
		close(flight)
		s.mu.Unlock()
		return ok
	}
}
func (s *TargetSnapshot) Lookup(ctx context.Context, key string) ([]byte, bool) {
	now := SnapshotNow()
	s.mu.Lock()
	s.pruneExpired(now)
	s.mu.Unlock()
	if !SteadyObservation(ctx) || ctx.Err() != nil {
		return nil, false
	}
	listingOK := s.ensureListing(ctx)
	now = SnapshotNow()
	s.mu.Lock()
	entry, ok := s.entries[key]
	valid := listingOK && ok && s.inFlight == 0 && entry.epoch == s.epoch && !now.Before(entry.at) && now.Sub(entry.at) <= SnapshotMaxAge && s.listingOK
	var body []byte
	if valid {
		body = append([]byte(nil), entry.body...)
	}
	proof := snapshotProof{s.epoch, s.drift}
	s.mu.Unlock()
	if e, _ := ctx.Value(evidenceKey{}).(*Evidence); e != nil {
		e.note(s, proof)
	}
	if !valid {
		return nil, false
	}
	if s.metrics != nil {
		s.metrics.requests[requestCallerIndex(ctx)].hits.Add(1)
	}
	return body, true
}
func (s *TargetSnapshot) ReadStart(ctx context.Context) uint64 {
	if s.metrics != nil {
		s.metrics.requests[requestCallerIndex(ctx)].reads.Add(1)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneExpired(SnapshotNow())
	return s.epoch
}
func (s *TargetSnapshot) Populate(key string, token uint64, fetchedAt time.Time, body []byte) {
	now := SnapshotNow()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneExpired(now)
	if len(body) > SnapshotMaxBody {
		return
	}
	if token != s.epoch || s.inFlight != 0 || fetchedAt.After(now) || now.Sub(fetchedAt) > SnapshotMaxAge {
		return
	}
	if old, ok := s.entries[key]; ok {
		s.entryBytes -= len(old.body)
		delete(s.entries, key)
	}
	if s.entries == nil {
		s.entries = make(map[string]snapshotEntry)
	}
	for len(s.entries) >= snapshotMaxEntries || s.entryBytes+len(body) > SnapshotMaxBytes {
		s.evictOldest()
	}
	s.entries[key] = snapshotEntry{append([]byte(nil), body...), fetchedAt, token}
	s.entryBytes += len(body)
}

// pruneExpired is amortized across reads as well as writes. Call with mu held.
func (s *TargetSnapshot) pruneExpired(now time.Time) {
	if now.Before(s.nextPrune) {
		return
	}
	for key, entry := range s.entries {
		if entry.epoch != s.epoch || now.Sub(entry.at) > SnapshotMaxAge {
			s.entryBytes -= len(entry.body)
			delete(s.entries, key)
		}
	}
	s.nextPrune = now.Add(snapshotPruneInterval)
}

func (s *TargetSnapshot) evictOldest() {
	var oldestKey string
	var oldest time.Time
	found := false
	for key, entry := range s.entries {
		if !found || entry.at.Before(oldest) || entry.at.Equal(oldest) && key < oldestKey {
			oldestKey, oldest, found = key, entry.at, true
		}
	}
	s.entryBytes -= len(s.entries[oldestKey].body)
	delete(s.entries, oldestKey)
}
func (s *TargetSnapshot) BeginMutation(ctx context.Context) (done func()) {
	if s.metrics != nil {
		s.metrics.requests[requestCallerIndex(ctx)].mutations.Add(1)
	}
	s.mu.Lock()
	s.epoch++
	s.inFlight++
	s.entries = nil
	s.entryBytes = 0
	s.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { s.mu.Lock(); s.epoch++; s.inFlight--; s.mu.Unlock() }) }
}
func (s *TargetSnapshot) Invalidate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epoch++
	s.entries = nil
	s.entryBytes = 0
}
func (s *TargetSnapshot) DriftGeneration() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.drift
}

type PlatformSnapshotter interface{ PlatformSnapshot() *TargetSnapshot }
