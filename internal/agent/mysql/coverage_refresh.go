// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"errors"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/minio/minio-go/v7"
)

const (
	coverageLease           = 60 * time.Second
	coverageRefreshInterval = 30 * time.Second
	coverageActiveLifetime  = 2 * time.Minute
	coverageJobTimeout      = 20 * time.Minute
	coverageResidentRoots   = 32
	coverageReadConcurrency = 8
	coverageBatchSize       = 16
	coverageQuantum         = 128
)

var errCoverageQueueBusy = errors.New("coverage_refresh_queue_busy")

type coverageEntry struct {
	key                                string
	request                            myspec.PITRCoverageRequest
	verified                           *coverageVerifiedDisk
	progress                           myspec.PITRCoverageProgress
	err                                error
	failureDurable                     bool
	requested, lastAttempt, freshAfter time.Time
	queued, working                    bool
	changed                            chan struct{}
}
type coverageRefresh struct {
	mu           sync.Mutex
	runner       *Runner
	root         string
	startedAt    time.Time
	ctx          context.Context
	cancel       context.CancelFunc
	wake         chan struct{}
	done         chan struct{}
	entries      map[string]*coverageEntry
	recentRoots  map[string]time.Time
	lastPrune    time.Time
	retentionErr error
	queue        []*coverageEntry
	currentKey   string
	work         *coverageWork
	client       *minio.Client
	closeClient  func()
}

func coverageRequestKey(request myspec.PITRCoverageRequest) string {
	request.RequireFresh = false
	request.AcceptPending = false
	return requestDigest(request)
}
func (e *Runner) coverageService() (*coverageRefresh, error) {
	e.coverageServiceMu.Lock()
	defer e.coverageServiceMu.Unlock()
	if e.coverageStopped {
		return nil, errors.New("coverage_refresh_stopped")
	}
	if e.coverageRefresh != nil {
		return e.coverageRefresh, nil
	}
	_, p := e.state()
	if err := privateDir(p.root); err != nil {
		return nil, err
	}
	if err := pruneCoverageStore(p.root, nil, true); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	service := &coverageRefresh{runner: e, root: p.root, startedAt: time.Now().UTC(), ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1), done: make(chan struct{}), entries: map[string]*coverageEntry{}}
	e.coverageRefresh = service
	go service.run()
	return service, nil
}
func (e *Runner) stopCoverageRefresh(ctx context.Context) {
	e.coverageServiceMu.Lock()
	e.coverageStopped = true
	service := e.coverageRefresh
	e.coverageServiceMu.Unlock()
	if service == nil {
		return
	}
	service.cancel()
	select {
	case <-service.done:
	case <-ctx.Done():
	}
}
func (s *coverageRefresh) notify(entry *coverageEntry) {
	close(entry.changed)
	entry.changed = make(chan struct{})
}
func (s *coverageRefresh) enqueue(entry *coverageEntry) {
	if entry.queued || entry.working {
		return
	}
	entry.queued = true
	entry.progress.Stage = "queued"
	s.queue = append(s.queue, entry)
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
func (s *coverageRefresh) entry(request myspec.PITRCoverageRequest) (*coverageEntry, error) {
	key := coverageRequestKey(request)
	if s.recentRoots == nil {
		s.recentRoots = map[string]time.Time{}
	}
	s.recentRoots[key] = time.Now()
	if entry := s.entries[key]; entry != nil {
		return entry, nil
	}
	if len(s.entries) >= coverageResidentRoots {
		var oldest *coverageEntry
		for _, entry := range s.entries {
			if entry.working || entry.queued {
				continue
			}
			if oldest == nil || entry.requested.Before(oldest.requested) {
				oldest = entry
			}
		}
		if oldest == nil {
			return nil, errCoverageQueueBusy
		}
		if oldest.err != nil && !oldest.failureDurable {
			if err := invalidateCoverageProof(coverageStatePrefix(s.root, oldest.key)); err != nil {
				return nil, err
			}
		}
		delete(s.entries, oldest.key)
	}
	entry := &coverageEntry{key: key, request: request, changed: make(chan struct{}), progress: myspec.PITRCoverageProgress{Stage: "queued"}}
	entry.request.ObjectPaths = slices.Clone(request.ObjectPaths)
	entry.request.ObjectManifests = slices.Clone(request.ObjectManifests)
	verified, err := loadCoverageVerified(coverageStatePrefix(s.root, key))
	if err != nil {
		entry.err = err
	} else {
		entry.verified = verified
		if verified != nil {
			if verified.Error != "" {
				entry.err = errors.New(verified.Error)
				entry.failureDurable = true
			}
			if !verified.VerifiedAt.After(s.startedAt) {
				verified.VerifiedAt = time.Time{}
			} else {
				entry.lastAttempt = verified.StartedAt
			}
		}
	}
	s.entries[key] = entry
	return entry, nil
}
func (s *coverageRefresh) poll(ctx context.Context, request myspec.PITRCoverageRequest) (myspec.PITRCoverageResponse, error) {
	if err := ctx.Err(); err != nil {
		return myspec.PITRCoverageResponse{}, err
	}
	admission := time.Now().UTC()
	for {
		s.mu.Lock()
		if err := s.ctx.Err(); err != nil {
			s.mu.Unlock()
			return myspec.PITRCoverageResponse{}, err
		}
		if s.retentionErr != nil {
			err := s.retentionErr
			s.mu.Unlock()
			return myspec.PITRCoverageResponse{}, err
		}
		entry, err := s.entry(request)
		if err != nil {
			s.mu.Unlock()
			if !request.AcceptPending || !errors.Is(err, errCoverageQueueBusy) {
				return myspec.PITRCoverageResponse{}, err
			}
			return myspec.PITRCoverageResponse{Pending: true, Progress: &myspec.PITRCoverageProgress{Stage: "queue-busy"}}, nil
		}
		now := time.Now().UTC()
		entry.requested = now
		if request.RequireFresh && admission.After(entry.freshAfter) {
			entry.freshAfter = admission
		}
		valid := entry.verified != nil && entry.verified.Cache != nil && entry.err == nil && !entry.verified.VerifiedAt.IsZero() && !now.Before(entry.verified.VerifiedAt) && now.Sub(entry.verified.VerifiedAt) <= coverageLease
		fresh := valid && entry.verified.StartedAt.After(admission)
		if valid && (!request.RequireFresh || fresh) {
			response := entry.verified.Cache.Response
			response.VerificationStartedAt = entry.verified.StartedAt
			response.VerifiedAt = entry.verified.VerifiedAt
			response.Fresh = fresh
			if now.Sub(entry.lastAttempt) >= coverageRefreshInterval {
				s.enqueue(entry)
			}
			s.mu.Unlock()
			return response, nil
		}
		if request.RequireFresh && entry.err != nil && entry.lastAttempt.After(admission) {
			err := entry.err
			s.mu.Unlock()
			return myspec.PITRCoverageResponse{}, err
		}
		if request.RequireFresh || entry.lastAttempt.IsZero() || now.Sub(entry.lastAttempt) >= coverageRefreshInterval || !valid && entry.err == nil && !entry.working && !entry.queued {
			s.enqueue(entry)
		}
		progress := entry.progress
		failure := entry.err
		changed := entry.changed
		s.mu.Unlock()
		if !request.AcceptPending {
			if failure != nil {
				return myspec.PITRCoverageResponse{}, failure
			}
			return myspec.PITRCoverageResponse{}, errors.New("pitr_coverage_verification_pending")
		}
		if !request.RequireFresh {
			if failure != nil {
				return myspec.PITRCoverageResponse{}, failure
			}
			return myspec.PITRCoverageResponse{Pending: true, Progress: &progress}, nil
		}
		select {
		case <-ctx.Done():
			return myspec.PITRCoverageResponse{}, ctx.Err()
		case <-s.ctx.Done():
			return myspec.PITRCoverageResponse{}, s.ctx.Err()
		case <-changed:
		}
	}
}
func (s *coverageRefresh) closeCurrent() {
	if s.closeClient != nil {
		s.closeClient()
	}
	s.client = nil
	s.closeClient = nil
	s.work = nil
	s.currentKey = ""
}
func (s *coverageRefresh) run() {
	defer close(s.done)
	defer s.closeCurrent()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		now := time.Now()
		if now.Sub(s.lastPrune) >= time.Minute {
			retained := map[string]bool{}
			for key, stamp := range s.recentRoots {
				if now.Sub(stamp) > coverageDurableLifetime {
					delete(s.recentRoots, key)
				} else if now.Sub(stamp) < coverageActiveLifetime {
					retained[key] = true
				}
			}
			for key, entry := range s.entries {
				if entry.working || entry.queued {
					retained[key] = true
				}
			}
			s.retentionErr = pruneCoverageStore(s.root, retained, false)
			s.lastPrune = now
		}
		for _, entry := range s.entries {
			if now.Sub(entry.requested) < coverageActiveLifetime && !entry.working && !entry.queued && now.Sub(entry.lastAttempt) >= coverageRefreshInterval {
				s.enqueue(entry)
			}
		}
		var entry *coverageEntry
		if len(s.queue) > 0 {
			entry = s.queue[0]
			s.queue = s.queue[1:]
			entry.queued = false
			entry.working = true
		}
		s.mu.Unlock()
		if entry == nil {
			select {
			case <-s.ctx.Done():
				return
			case <-s.wake:
			case <-ticker.C:
			}
			continue
		}
		if s.ctx.Err() != nil {
			return
		}
		finished, err := s.advance(entry)
		err = errors.Join(err, pruneCoverageInventories(coverageStatePrefix(s.root, entry.key)))
		if err != nil && s.ctx.Err() == nil {
			s.mu.Lock()
			entry.err = err
			entry.failureDurable = false
			entry.progress.Stage = "failed"
			s.notify(entry)
			s.mu.Unlock()
			persistErr := s.persistFailure(entry)
			s.mu.Lock()
			entry.failureDurable = persistErr == nil
			if persistErr != nil {
				entry.err = errors.Join(err, persistErr)
				s.notify(entry)
			}
			s.mu.Unlock()
		}
		s.mu.Lock()
		entry.working = false
		if err != nil {
			s.closeCurrent()
		} else if finished {
			record := coverageCacheRecord(s.work.cache)
			entry.verified = &coverageVerifiedDisk{Version: 1, Baseline: s.work.Baseline, Cache: &record, StartedAt: s.work.StartedAt, VerifiedAt: time.Now().UTC()}
			entry.err = nil
			entry.progress = myspec.PITRCoverageProgress{Stage: "complete", Processed: s.work.Total, Total: s.work.Total}
			s.notify(entry)
			needFresh := entry.freshAfter.After(s.work.StartedAt)
			s.closeCurrent()
			if needFresh {
				s.enqueue(entry)
			}
		} else if time.Since(entry.requested) > coverageActiveLifetime {
			entry.progress.Stage = "paused"
			s.notify(entry)
			s.closeCurrent()
		} else {
			s.enqueue(entry)
		}
		s.mu.Unlock()
	}
}
func (s *coverageRefresh) persistFailure(entry *coverageEntry) error {
	record := coverageVerifiedDisk{Version: 1, Error: "pitr_coverage_refresh_failed"}
	if entry.verified != nil {
		record = *entry.verified
		record.Error = "pitr_coverage_refresh_failed"
	}
	return durableJSON(coverageStatePrefix(s.root, entry.key)+".verified.json", record)
}
func (s *coverageRefresh) progress(entry *coverageEntry, stage string, position, total int) {
	s.mu.Lock()
	entry.progress = myspec.PITRCoverageProgress{Stage: stage, Processed: position, Total: total}
	s.notify(entry)
	s.mu.Unlock()
}
func (s *coverageRefresh) advance(entry *coverageEntry) (bool, error) {
	if s.currentKey != entry.key {
		s.closeCurrent()
		s.currentKey = entry.key
		client, closeClient, err := s3Client(entry.request.Source)
		if err != nil {
			return false, err
		}
		s.client = client
		s.closeClient = closeClient
		if err = s.begin(entry); err != nil {
			return false, err
		}
	}
	ctx, cancel := context.WithDeadline(s.ctx, s.work.StartedAt.Add(coverageJobTimeout))
	defer cancel()
	end := min(s.work.Position+coverageQuantum, s.work.Total)
	for s.work.Position < end {
		batchEnd := min(s.work.Position+coverageBatchSize, end)
		logs, err := readCoverageBatch(ctx, s.client, entry.request.Source, s.work.refs[s.work.Position:batchEnd])
		if err != nil {
			return false, err
		}
		candidate := s.work.cache.state.clone()
		for _, log := range logs {
			if err = candidate.consume(log, s.work.Baseline); err != nil {
				return false, err
			}
		}
		s.work.cache.state = candidate
		s.work.Position = batchEnd
		if err = s.work.checkpoint(); err != nil {
			return false, err
		}
		s.progress(entry, "sidecars", s.work.Position, s.work.Total)
	}
	if s.work.Position == s.work.Total {
		if err := s.work.complete(); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}
func (s *coverageRefresh) begin(entry *coverageEntry) error {
	start := time.Now().UTC()
	s.mu.Lock()
	entry.lastAttempt = start
	s.mu.Unlock()
	s.progress(entry, "baseline", 0, 0)
	ctx, cancel := context.WithTimeout(s.ctx, coverageJobTimeout)
	defer cancel()
	spec, p := s.runner.state()
	baseline, err := loadPITRBaseline(ctx, spec, p, entry.request)
	if err != nil {
		return err
	}
	prefix := coverageStatePrefix(s.root, entry.key)
	verified, err := loadCoverageVerified(prefix)
	if err != nil {
		return err
	}
	work, err := loadCoverageWork(prefix)
	if err != nil {
		return err
	}
	if verified != nil && work != nil && verified.Cache != nil && !verified.VerifiedAt.IsZero() && !verified.StartedAt.Before(work.StartedAt) {
		work = nil
	}
	var previous *replayCoverageCache
	if work != nil {
		previous = work.cache
	} else if verified != nil && verified.Cache != nil {
		previous, err = verified.Cache.cache()
		if err != nil {
			return err
		}
	}
	s.progress(entry, "inventory", 0, 0)
	plan, err := planReplayCoverage(ctx, s.client, entry.request.Source, baseline, previous)
	if err != nil {
		return err
	}
	refs := plan.refs
	position := 0
	if work != nil && plan.continued {
		refs = append(work.refs, refs...)
		position = work.Position
		plan.cache.state = work.cache.state.clone()
	}
	if err = saveCoverageRefs(prefix, plan.cache.inventory, refs); err != nil {
		return err
	}
	s.work = &coverageWork{coverageWorkDisk: coverageWorkDisk{Version: 1, Baseline: baseline, StartedAt: start, Position: position, Total: len(refs)}, cache: plan.cache, refs: refs, prefix: prefix}
	if err = s.work.checkpoint(); err != nil {
		return err
	}
	if work != nil && work.cache.inventory != s.work.cache.inventory {
		if err = os.Remove(prefix + ".refs-" + work.cache.inventory); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	s.progress(entry, "sidecars", position, len(refs))
	return nil
}
func readCoverageBatch(ctx context.Context, client *minio.Client, source myspec.Repository, refs []replayObjectRef) ([]archivedBinlog, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	logs := make([]archivedBinlog, len(refs))
	jobs := make(chan int)
	failures := make(chan error, 1)
	var workers sync.WaitGroup
	for range min(coverageReadConcurrency, len(refs)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				log, err := readReplaySidecar(ctx, client, source, refs[index].key)
				if err != nil {
					select {
					case failures <- err:
					default:
					}
					cancel()
					return
				}
				logs[index] = log
			}
		}()
	}
	for index := range refs {
		select {
		case jobs <- index:
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			select {
			case err := <-failures:
				return nil, err
			default:
				return nil, ctx.Err()
			}
		}
	}
	close(jobs)
	workers.Wait()
	select {
	case err := <-failures:
		return nil, err
	default:
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return logs, nil
}
