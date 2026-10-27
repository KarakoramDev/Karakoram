// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func platformRetryKey(v *pending) string { return v.Member + "/" + v.Action }
func (s *internalState) resetPlatformRetry(v *pending) {
	delete(s.PlatformRetries, platformRetryKey(v))
	if s.Retry != nil && platformRetryKey(s.Retry) == platformRetryKey(v) {
		s.Retry = nil
	}
}
func (s *internalState) platformFailureMessage() string {
	if s.Retry != nil {
		if r := s.PlatformRetries[platformRetryKey(s.Retry)]; r != nil {
			return fmt.Sprintf("%s: %s; retry after %s", s.Retry.Action, r.Message, r.RetryAfter.Format(time.RFC3339))
		}
	}
	return ""
}
func (s *internalState) failPendingCause(rc *engine.Context, cause error) error {
	v := s.Pending
	var resourceRejected *infra.ResourceUpdateRejectedError
	if errors.As(cause, &resourceRejected) {
		// Admission failed before changing applied bounds. Discard the
		// captured resource change as well as its receipt, so a corrective
		// spec is not pinned behind this rejected target.
		if m := s.find(v.Member); m != nil && m.Change != nil && m.Change.Kind == "resources" {
			m.Change = nil
		}
	}
	var wait *engine.PlatformRetryWait
	if errors.As(cause, &wait) || engine.PlatformFailureDefinitive(cause) || errors.Is(cause, engine.ErrNotSupported) || errors.Is(cause, infra.ErrUnsupported) {
		s.Pending = nil
		s.resetPlatformRetry(v)
		return cause
	}
	failure := s.failPending(rc, cause.Error())
	if at := infra.RetryAt(cause); !at.IsZero() {
		r := s.PlatformRetries[platformRetryKey(v)]
		if at.After(r.RetryAfter) {
			r.RetryAfter = r.Schedule(now(rc), at)
		}
		return errors.Join(failure, cause)
	}
	return failure
}
func (s *internalState) failPending(rc *engine.Context, message string) error {
	v := s.Pending
	if s.PlatformRetries == nil {
		s.PlatformRetries = map[string]*platformRetry{}
	}
	key := platformRetryKey(v)
	r := s.PlatformRetries[key]
	if r == nil {
		r = &platformRetry{}
		s.PlatformRetries[key] = r
	}
	r.Failures++
	r.Message = message
	r.RetryAfter = r.Schedule(now(rc), now(rc).Add(engine.PlatformRetryDelay(r.Failures)))
	r.LastAttemptAt = now(rc)
	r.LastProcessID = v.Op.ID
	if volumeDelete(v.Action) && strings.Contains(strings.ToLower(message), "still mounted") {
		if r.FirstFailedAt.IsZero() {
			r.FirstFailedAt = now(rc)
			r.FirstError = message
		}
		r.StillMountedFailures++
	} else {
		r.FirstFailedAt = time.Time{}
		r.FirstError = ""
		r.StillMountedFailures = 0
	}
	v.Op = infra.Op{}
	s.Retry = v
	s.Pending = nil
	return fmt.Errorf("platform %s %s failed: %s; retry after %s", v.Action, v.Member, message, r.RetryAfter.Format(time.RFC3339))
}
func (p *Provider) submit(ctx context.Context, rc *engine.Context, s *internalState, v *pending) error {
	m := s.find(v.Member)
	if m == nil {
		return engine.ErrNotFound
	}
	if err := verifyMemberServices(ctx, rc, m); err != nil {
		return err
	}
	var op infra.Op
	var err error
	switch v.Action {
	case "import":
		return p.importMember(ctx, rc, s, m)
	case "deploy":
		return p.deploy(ctx, rc, s, m, v.Version)
	case "resources":
		op, err = rc.Backend.SetResources(ctx, m.Name, v.Resources)
	case "disk":
		if err = engine.RequireDiskGrowth(rc.Backend); err != nil {
			return err
		}
		op, err = rc.Backend.SetResources(ctx, m.Volume, domain.Resources{MinDisk: v.Resources.MinDisk, MaxDisk: v.Resources.MaxDisk})
	case "cleanup-runtime", "cleanup-volume", "delete-runtime", "delete-volume":
		name, id := m.Name, m.ServiceID
		if volumeDelete(v.Action) {
			name, id = m.Volume, m.VolumeID
		}
		service, e := engine.ObserveRetiringService(ctx, rc.Backend, name, id, m.Volume, m.VolumeID, m.RuntimeRecovery.VolumeLost)
		if e != nil {
			return e
		}
		if service != nil {
			op, err = rc.Backend.DeleteService(ctx, name)
		}
	default:
		return fmt.Errorf("unsupported OpenBao platform action %q", v.Action)
	}
	v.Op = op
	s.Pending = v
	if err != nil {
		return s.failPendingCause(rc, err)
	}
	s.Retry = nil
	return nil
}
func (p *Provider) retryPending(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	v := s.Retry
	if v == nil {
		return false, nil
	}
	r := s.PlatformRetries[platformRetryKey(v)]
	if r == nil {
		return true, fmt.Errorf("missing platform retry checkpoint")
	}
	if volumeDelete(v.Action) {
		if done, err := p.orphanVolume(ctx, rc, s, v, r); done || err != nil {
			return true, err
		}
	}
	if r.Waiting(now(rc), r.RetryAfter) {
		return true, &engine.PlatformRetryWait{RetryAt: r.RetryAfter, Message: s.platformFailureMessage()}
	}
	// A retry is a new disruptive boundary, not a continuation of the old
	// admission decision. Native peers may have sealed or elected this target
	// while the platform backoff was pending.
	if m := s.find(v.Member); m != nil && m.Change != nil && (v.Action == "deploy" || v.Action == "resources" || v.Action == "disk") {
		p.observeMembers(ctx, rc, s)
		if busy, err := p.recoverEstablishedPeers(ctx, rc, s, m); busy || err != nil {
			return true, err
		}
		allowed, err := p.allowDisruption(ctx, rc, s, m)
		if err != nil || !allowed {
			return true, err
		}
	}
	err := p.submit(ctx, rc, s, v)
	if err == nil && v.Action == "deploy" {
		if m := s.find(v.Member); m != nil && m.Change != nil {
			m.Change.Step = "wait"
		}
	}
	return true, err
}
func (p *Provider) observeReceipt(ctx context.Context, rc *engine.Context, s *internalState, v *pending) (bool, error) {
	var st infra.OpState
	if v.Op.ID != "" {
		var err error
		st, err = rc.Backend.OpStatus(ctx, v.Op)
		if st.Status == "DeploymentRetriesExhausted" && err == nil {
			err = &infra.DeploymentExhaustedError{Service: v.Member, LastError: st.Message}
		}
		if errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
			if m := s.find(v.Member); m != nil {
				if spec, e := p.runtimeSpec(rc, m, v.Version); e == nil {
					err = engine.RecordDeploymentExhausted(ctx, rc, m.Name, spec, err)
				}
			}
			s.resetPlatformRetry(v)
			return true, err
		}
		var rejected *infra.ResourceUpdateRejectedError
		if errors.As(err, &rejected) {
			s.Pending = v
			return true, s.failPendingCause(rc, err)
		}
		if err != nil {
			return false, err
		}
		if st.Failed {
			s.Pending = v
			return true, s.failPending(rc, st.Message)
		}
		if !st.Done {
			return false, nil
		}
	}
	m := s.find(v.Member)
	if m == nil {
		return false, engine.ErrNotFound
	}
	switch v.Action {
	case "import":
		complete, err := adoptImported(ctx, rc, m, v.Op, st)
		if err != nil {
			return false, err
		}
		if err = verifyMemberServices(ctx, rc, m); err != nil {
			return false, err
		}
		if complete {
			m.Stage = "imported"
			m.ImportPlan = nil
		}
	case "deploy":
		m.Stage = "deployed"
		if m.Resume {
			m.Stage = "resume-deployed"
		}
	case "resources":
		m.Resources = v.Resources
		if m.Change != nil {
			m.Change.Step = "disk"
		}
	case "disk":
		m.Disk = v.Resources
		if m.Change != nil {
			m.Change.Step = "resource-config"
		}
	case "cleanup-runtime":
		m.ServiceID = ""
		m.Stage = "imported"
	case "cleanup-volume":
		m.VolumeID = ""
	case "delete-runtime":
		m.RemoveStep = "delete-volume"
	case "delete-volume":
		if err := deleteMemberSecrets(ctx, rc, m.Name); err != nil {
			return false, err
		}
		removeRecord(s, m.Name)
	}
	s.resetPlatformRetry(v)
	return true, nil
}
func (p *Provider) observePending(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	if s.Pending == nil {
		return false, nil
	}
	v := s.Pending
	done, err := p.observeReceipt(ctx, rc, s, v)
	if done && s.Pending == v {
		s.Pending = nil
	}
	return true, err
}
func (p *Provider) observeDeployments(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	busy := false
	for _, m := range ordered(s) {
		if m.ResumeDeploy == nil {
			continue
		}
		busy = true
		v := m.ResumeDeploy
		done, err := p.observeReceipt(ctx, rc, s, v)
		if done {
			m.ResumeDeploy = nil
		}
		if err != nil {
			return busy, err
		}
	}
	return busy, nil
}
