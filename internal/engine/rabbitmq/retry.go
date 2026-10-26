// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"strings"
	"time"
)

func platformRetryKey(v *pending) string { return v.Member + "/" + v.Action }
func (s *internalState) resetPlatformRetry(v *pending) {
	delete(s.PlatformRetries, platformRetryKey(v))
	if s.Retry != nil && platformRetryKey(s.Retry) == platformRetryKey(v) {
		s.Retry = nil
	}
}

// failPendingCause preserves rate-limit deadlines in both the returned error
// tree and the retry state that survives control-plane restarts.
func (s *internalState) failPendingCause(rc *engine.Context, cause error) error {
	v := s.Pending
	var wait *engine.PlatformRetryWait
	if errors.As(cause, &wait) {
		s.Pending = nil
		s.resetPlatformRetry(v)
		return cause
	}
	if errors.Is(cause, infra.ErrDeploymentRetriesExhausted) {
		s.Pending = nil
		s.resetPlatformRetry(v)
		return cause
	}
	failure := s.failPending(rc, cause.Error())
	if at := infra.RetryAt(cause); !at.IsZero() {
		if r := s.PlatformRetries[platformRetryKey(v)]; r != nil && at.After(r.RetryAfter) {
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
	recorded := now(rc)
	r.RetryAfter = r.Schedule(recorded, recorded.Add(engine.PlatformRetryDelay(r.Failures)))
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
func (s *internalState) platformFailureMessage() string {
	if s.Retry != nil {
		if r := s.PlatformRetries[platformRetryKey(s.Retry)]; r != nil {
			return fmt.Sprintf("%s: %s; retry after %s", s.Retry.Action, r.Message, r.RetryAfter.Format(time.RFC3339))
		}
	}
	return ""
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
	case "deploy", "upgrade":
		return p.deploy(ctx, rc, s, m, v.Action, v.Version)
	case "resources":
		op, err = rc.Backend.SetResources(ctx, m.Name, v.Resources)
	case "disk":
		if err := engine.RequireDiskGrowth(rc.Backend); err != nil {
			s.resetPlatformRetry(v)
			return err
		}
		op, err = rc.Backend.SetResources(ctx, m.Volume, domain.Resources{MinDisk: v.Resources.MinDisk, MaxDisk: v.Resources.MaxDisk})
	case "restart":
		op, err = rc.Backend.Restart(ctx, m.Name)
	case "management", "cleanup-management":
		o, e := parseOptions(m.EngineOptions)
		if e != nil {
			return e
		}
		if v.Action == "cleanup-management" {
			o.ManagementUI.Public = false
		}
		backend, e := engine.PublicHTTPS(rc.Backend)
		if e != nil {
			if o.ManagementUI.Public {
				s.resetPlatformRetry(v)
				return e
			}
			// Disabling routing is already complete on a backend without it.
			break
		}
		op, err = backend.SetSubdomainAccess(ctx, m.Name, o.ManagementUI.Public)
	case "delete-runtime", "cleanup-runtime", "delete-volume", "cleanup-volume":
		name, recordedID := m.Name, m.ServiceID
		if volumeDelete(v.Action) {
			name, recordedID = m.Volume, m.VolumeID
		}
		service, e := engine.ObserveRetiringService(ctx, rc.Backend, name, recordedID, m.Volume, m.VolumeID, m.RuntimeRecovery.VolumeLost)
		if e != nil {
			return e
		}
		if service != nil {
			op, err = rc.Backend.DeleteService(ctx, name)
		}
	default:
		return fmt.Errorf("unsupported platform action %q", v.Action)
	}
	v.Op = op
	s.Pending = v
	if err != nil {
		if errors.Is(err, engine.ErrNotSupported) || errors.Is(err, infra.ErrUnsupported) {
			s.Pending = nil
			s.resetPlatformRetry(v)
			return err
		}
		if v.Action == "resources" && errors.Is(err, infra.ErrResourceChangeCooldown) {
			at := now(rc).Add(time.Hour)
			if !m.CPUModeChangedAt.IsZero() && m.CPUModeChangedAt.Add(time.Hour).After(now(rc)) {
				at = m.CPUModeChangedAt.Add(time.Hour)
			}
			if s.PlatformRetries == nil {
				s.PlatformRetries = map[string]*platformRetry{}
			}
			s.PlatformRetries[platformRetryKey(v)] = &platformRetry{RetryAfter: at, Message: err.Error(), CPUModeCooldown: true}
			r := s.PlatformRetries[platformRetryKey(v)]
			r.RetryAfter = r.Schedule(now(rc), at)
			s.Retry = v
			s.Pending = nil
			return &engine.PlatformRetryWait{RetryAt: r.RetryAfter, Message: err.Error()}
		}
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
		return true, fmt.Errorf("missing retry checkpoint")
	}
	if volumeDelete(v.Action) {
		if done, err := p.orphanVolume(ctx, rc, s, v, r); done || err != nil {
			return true, err
		}
	}
	if r.Waiting(now(rc), r.RetryAfter) {
		return true, &engine.PlatformRetryWait{RetryAt: r.RetryAfter, Message: s.platformFailureMessage()}
	}
	return true, p.submit(ctx, rc, s, v)
}
func (p *Provider) observePending(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	v := s.Pending
	if v == nil {
		return false, nil
	}
	var resizeErr error
	if m := s.find(v.Member); v.Action == "deploy" && m != nil && !m.RuntimeRecovery.Initialized && m.Stage != "ready" && !m.Resume {
		var changed bool
		changed, resizeErr = engine.ResizeUnplacedDeployment(ctx, rc, m.Name, runtimeResources(rc.Cluster.Spec.Components.Engine.Resources), &m.Resources, &v.Op)
		if changed {
			return true, nil
		}
	}
	var st infra.OpState
	if v.Op.ID != "" || resizeErr != nil {
		err := resizeErr
		if err == nil {
			st, err = rc.Backend.OpStatus(ctx, v.Op)
		}
		if st.Status == "DeploymentRetriesExhausted" && err == nil {
			err = &infra.DeploymentExhaustedError{Service: v.Member, LastError: st.Message}
		}
		if errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
			if m := s.find(v.Member); m != nil {
				engine.ApplyDeploymentResourceRollback(err, &m.Resources)
				if spec, specErr := p.runtimeSpec(rc, m, v.Version); specErr == nil {
					err = engine.RecordDeploymentExhausted(ctx, rc, m.Name, spec, err)
				}
			}
			s.Pending = nil
			s.resetPlatformRetry(v)
			return false, err
		}
		if err != nil {
			return true, err
		}
		if st.Failed {
			return true, s.failPending(rc, st.Message)
		}
		if !st.Done {
			return true, nil
		}
	}
	m := s.find(v.Member)
	if m == nil {
		return true, engine.ErrNotFound
	}
	switch v.Action {
	case "import":
		complete, err := adoptImported(ctx, rc, m, v.Op, st)
		if err != nil {
			return true, err
		}
		if err := verifyMemberServices(ctx, rc, m); err != nil {
			return true, err
		}
		// An expired or partial import may have created only some services;
		// leave the member planned so importMember creates the rest.
		if complete {
			m.Stage = "imported"
			m.ImportPlan = nil
		}
	case "deploy":
		m.Stage = "deployed"
		if m.Resume {
			m.Stage = "resume-deployed"
		}
		m.Version = v.Version
	case "upgrade", "restart":
		m.Version = v.Version
		m.ChangeStep = "reload"
	case "resources":
		if m.Resources.CPUMode != v.Resources.CPUMode {
			m.CPUModeChangedAt = now(rc)
		}
		m.Resources = v.Resources
		if m.ChangeAction == "resources" {
			m.ChangeStep = "redeploy"
			if resourceUpdatesDeployRuntime(rc.Backend) {
				// The backend receipt already reapplied the stored runtime.
				// Reload the published native configuration, then retain the
				// same start/config/membership/revive gate before another broker.
				m.ChangeStep = "reload"
			}
		}
	case "disk":
		m.Disk = v.Resources
	case "management":
		o, err := parseOptions(m.EngineOptions)
		if err != nil {
			return true, err
		}
		m.ManagementPublic = o.ManagementUI.Public
	case "cleanup-management":
		m.ManagementPublic = false
	case "delete-runtime":
		m.RemoveStep = "delete-volume"
	case "delete-volume":
		if err := deleteMemberSecrets(ctx, rc, m.Name); err != nil {
			return true, err
		}
		removeRecord(s, m.Name)
		delete(rc.Cluster.Status.DeploymentFailures, m.Name)
	}
	s.resetPlatformRetry(v)
	s.Pending = nil
	return true, nil
}
