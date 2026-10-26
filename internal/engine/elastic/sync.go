// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

func retryKey(v *pending) string { return v.Member + "/" + v.Action }
func (p *Provider) failPending(ctx context.Context, rc *engine.Context, s *state, v *pending, message string) error {
	if s.PlatformRetries == nil {
		s.PlatformRetries = map[string]*platformRetry{}
	}
	key := retryKey(v)
	r := s.PlatformRetries[key]
	if r == nil {
		r = &platformRetry{FirstFailedAt: now(rc)}
		s.PlatformRetries[key] = r
	}
	r.Failures++
	r.Message = message
	r.LastProcessID = v.Op.ID
	recorded := now(rc)
	r.RetryAfter = r.Schedule(recorded, recorded.Add(engine.PlatformRetryDelay(r.Failures)))
	if strings.Contains(strings.ToLower(message), "still mounted") {
		r.StillMountedFailures++
	}
	s.Pending = nil
	s.Retry = v
	if (v.Action == "delete-volume" || v.Action == "cleanup-volume") && r.StillMountedFailures >= 5 && !now(rc).Before(r.FirstFailedAt.Add(30*time.Minute)) {
		m := s.find(v.Member)
		if m != nil {
			runtime, err := rc.Backend.GetService(ctx, m.Name)
			if err != nil {
				return err
			}
			if runtime == nil {
				if err = verifyMemberServices(ctx, rc, m); err != nil {
					return err
				}
				volume, err := rc.Backend.GetService(ctx, m.Volume)
				if err != nil {
					return err
				}
				if volume != nil {
					if rc.Store == nil {
						return errors.New("orphaned volume tracking requires store")
					}
					record, err := rc.Store.GetOrphanedVolume(ctx, rc.Cluster.Ref().Namespace, m.Volume)
					if missing(err) {
						record = &domain.OrphanedVolume{Namespace: rc.Cluster.Ref().Namespace, Hostname: m.Volume, ServiceID: volume.ID, Cluster: rc.Cluster.Name, Member: m.Name, FirstError: message, LastError: message, FirstFailedAt: r.FirstFailedAt, OrphanedAt: now(rc), LastAttemptAt: now(rc), NextAttemptAt: now(rc).Add(30 * time.Second), Attempts: r.Failures, LastProcessID: r.LastProcessID}
						if err = rc.Store.PutOrphanedVolume(ctx, record); err != nil {
							return err
						}
					} else if err != nil {
						return err
					} else if record.ServiceID != volume.ID {
						return engine.ErrConflict
					}
					if rc.Events != nil {
						if err = rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Type: "Warning", Reason: "VolumeOrphaned", Message: "A verified detached volume remains mounted; the volume janitor will retry deletion", CreatedAt: now(rc)}); err != nil {
							return err
						}
					}
					m.VolumeOrphaned = true
					m.Stage = "removed"
					s.Retry = nil
					delete(s.PlatformRetries, key)
					return nil
				}
			}
		}
	}
	return fmt.Errorf("platform %s failed; retry scheduled", v.Action)
}

// failPendingCause retains backend reset constraints when platform operations
// become durable retry records instead of discarding them with Error().
func (p *Provider) failPendingCause(ctx context.Context, rc *engine.Context, s *state, v *pending, cause error) error {
	var wait *engine.PlatformRetryWait
	if errors.As(cause, &wait) || errors.Is(cause, infra.ErrDeploymentRetriesExhausted) {
		s.Pending, s.Retry = nil, nil
		delete(s.PlatformRetries, retryKey(v))
		if s.Rolling != nil && s.Rolling.Current == v.Member && (v.Action == "upgrade" || v.Action == "deploy") {
			// Keep the same disruption owner and its recovery action, without
			// charging a new failure or requiring the absent target to rejoin.
			s.Retry = v
			if wait != nil {
				if s.PlatformRetries == nil {
					s.PlatformRetries = make(map[string]*platformRetry)
				}
				r := &platformRetry{}
				at := wait.RetryAt
				if backendAt := infra.RetryAt(cause); backendAt.After(at) {
					at = backendAt
				}
				r.RetryAfter = r.Schedule(now(rc), at)
				s.PlatformRetries[retryKey(v)] = r
			}
		}
		return cause
	}
	failure := p.failPending(ctx, rc, s, v, cause.Error())
	if at := infra.RetryAt(cause); !at.IsZero() {
		if r := s.PlatformRetries[retryKey(v)]; r != nil && at.After(r.RetryAfter) {
			r.RetryAfter = r.Schedule(now(rc), at)
		}
		return errors.Join(failure, cause)
	}
	return failure
}
func (p *Provider) issueAction(ctx context.Context, rc *engine.Context, s *state, v *pending) error {
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
			return err
		}
		op, err = rc.Backend.SetResources(ctx, m.Volume, v.Resources)
	case "containers":
		if v.Containers > 1 && !rc.Backend.Capabilities().HorizontalScaling {
			return fmt.Errorf("%w: component %s requires horizontal scaling", engine.ErrNotSupported, m.Kind)
		}
		op, err = rc.Backend.SetContainerCount(ctx, m.Name, v.Containers, v.Containers)
	case "delete-runtime", "cleanup-runtime":
		op, err = rc.Backend.DeleteService(ctx, m.Name)
	case "delete-volume", "cleanup-volume":
		op, err = rc.Backend.DeleteService(ctx, m.Volume)
	case "stop", "stop-removal":
		op, err = rc.Backend.Stop(ctx, m.Name)
	case "start":
		op, err = rc.Backend.Start(ctx, m.Name)
	case "subdomain-on", "subdomain-off":
		sub, e := engine.PublicHTTPS(rc.Backend)
		if e != nil {
			return e
		}
		op, err = sub.SetSubdomainAccess(ctx, m.Name, v.Action == "subdomain-on")
	default:
		return invalid("unknown platform action")
	}
	v.Op = op
	if err != nil {
		if errors.Is(err, infra.ErrUnsupported) || errors.Is(err, engine.ErrNotSupported) {
			return err
		}
		return p.failPendingCause(ctx, rc, s, v, err)
	}
	s.Pending = v
	return nil
}
func (p *Provider) observePending(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	v := s.Pending
	if v == nil {
		return false, nil
	}
	var resizeErr error
	if m := s.find(v.Member); v.Action == "deploy" && m != nil && !m.RuntimeRecovery.Initialized && m.Stage != stageJoined {
		var changed bool
		changed, resizeErr = engine.ResizeUnplacedDeployment(ctx, rc, m.Name, runtimeResources(m, component(rc.Cluster.Spec, m).Resources), &m.Resources, &v.Op)
		if changed {
			return true, nil
		}
	}
	var status infra.OpState
	if v.Op.ID != "" || resizeErr != nil {
		err := resizeErr
		if err == nil {
			status, err = rc.Backend.OpStatus(ctx, v.Op)
		}
		if status.Status == "DeploymentRetriesExhausted" && err == nil {
			err = &infra.DeploymentExhaustedError{Service: v.Member, LastError: status.Message}
		}
		if errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
			if m := s.find(v.Member); m != nil {
				engine.ApplyDeploymentResourceRollback(err, &m.Resources)
				if spec, specErr := p.deploymentSpec(rc, m, v.Version); specErr == nil {
					err = engine.RecordDeploymentExhausted(ctx, rc, m.Name, spec, err)
				}
			}
			return false, p.failPendingCause(ctx, rc, s, v, err)
		}
		if err != nil {
			return true, err
		}
		if status.Failed {
			return true, p.failPending(ctx, rc, s, v, status.Message)
		}
		if !status.Done {
			return true, nil
		}
	}
	m := s.find(v.Member)
	if m != nil {
		switch v.Action {
		case "import":
			complete, err := adoptImported(ctx, rc, m, v.Op, status)
			if err != nil {
				return true, err
			}
			if err := verifyMemberServices(ctx, rc, m); err != nil {
				return true, err
			}
			// Expired or partial imports can leave services absent; keep the
			// pre-import stage so the normal import step creates the rest.
			if complete {
				m.Stage = stageImported
				m.Resources = v.Resources
				m.Containers = v.Containers
				m.Disk = component(rc.Cluster.Spec, m).Resources
				if m.ImportPlan != nil {
					for _, spec := range m.ImportPlan.Specs {
						if spec.Hostname == m.Volume {
							m.Disk.MinDisk, m.Disk.MaxDisk = spec.Resources.MinDisk, spec.Resources.MaxDisk
						}
					}
				}
				m.ImportPlan = nil
				if rc.Store != nil && m.Volume != "" {
					if err := rc.Store.DeleteRetainedVolume(ctx, rc.Cluster.Ref(), m.Name); err != nil && !missing(err) {
						return true, err
					}
				}
			}
		case "deploy", "upgrade":
			m.Stage = stageDeployed
			m.Version = v.Version
		case "resources":
			m.Resources = v.Resources
		case "disk":
			m.Disk = v.Resources
		case "containers":
			m.Containers = v.Containers
		case "stop-removal":
			m.Stage = stageDeleting
		case "delete-runtime":
			m.Stage = "delete-volume"
		case "delete-volume":
			m.Stage = "removed"
		case "subdomain-on":
			m.Subdomain = true
			p.bumpConfig(s)
		case "subdomain-off":
			m.Subdomain = false
			p.bumpConfig(s)
		}
	}
	delete(s.PlatformRetries, retryKey(v))
	s.Pending = nil
	return true, nil
}
func (p *Provider) retryPending(ctx context.Context, rc *engine.Context, s *state) (bool, time.Duration, error) {
	v := s.Retry
	if v == nil {
		return false, 0, nil
	}
	if m := s.find(v.Member); m != nil && (v.Action == "upgrade" || v.Action == "deploy") {
		if spec, err := p.deploymentSpec(rc, m, v.Version); err == nil && engine.DeploymentBlocked(rc, m.Name, spec) {
			// Retain the action for a changed artifact or operator re-arm, but
			// let independent observations and convergence continue.
			return false, 0, nil
		}
	}
	if r := s.PlatformRetries[retryKey(v)]; r != nil {
		recorded := now(rc)
		if delay := r.RetryAfter.Sub(recorded); r.Waiting(recorded, r.RetryAfter) {
			return true, delay, &engine.PlatformRetryWait{RetryAt: r.RetryAfter, Message: "waiting for scheduled platform retry"}
		}
	}
	// A delayed platform retry rechecks the other masters and committed quorum.
	// The current failed action may already have taken its own target offline.
	if m := s.find(v.Member); s.Rolling != nil && m != nil && masterEligible(rc.Cluster.Spec, m) && (v.Action == "resources" || v.Action == "upgrade" || v.Action == "deploy") {
		c, err := p.es(ctx, rc, s)
		if err != nil {
			return true, 5 * time.Second, err
		}
		defer c.Close()
		if err = rollingMasterReady(ctx, rc, s, m, c, s.Rolling.Current == m.Name); err != nil {
			return true, 5 * time.Second, err
		}
	}
	if m := s.find(v.Member); m != nil && m.Removing && isES(m) && (v.Action == "stop-removal" || v.Action == "delete-runtime" || v.Action == "delete-volume") {
		c, err := p.es(ctx, rc, s)
		if err != nil {
			return true, 5 * time.Second, err
		}
		defer c.Close()
		h, err := c.Health(ctx, esapi.HealthOptions{})
		if err != nil {
			return true, 5 * time.Second, err
		}
		if h.Status != "green" && h.Status != "yellow" {
			if err = blockDrain(ctx, rc, s, m, "unassigned primary shards prevent safe removal"); err != nil {
				return true, 5 * time.Second, err
			}
			return true, 5 * time.Second, errWaiting
		}
		s.DrainBlocked = ""
	}
	s.Retry = nil
	err := p.issueAction(ctx, rc, s, v)
	return true, 5 * time.Second, err
}

func (p *Provider) importMember(ctx context.Context, rc *engine.Context, s *state, m *member) error {
	if err := verifyMemberServices(ctx, rc, m); err != nil {
		return err
	}
	tok, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil {
		return err
	}
	comp := component(rc.Cluster.Spec, m)
	specs := []infra.ServiceSpec{}
	incomplete := false
	if m.Volume != "" {
		volume, err := rc.Backend.GetService(ctx, m.Volume)
		if err != nil {
			return err
		}
		incomplete = incomplete || volume != nil && volume.Importing
		if volume == nil {
			spec := engine.VolumeServiceSpec(rc.Cluster.Name, m.Name, m.Volume, tok.Token, comp.Resources)
			if old := s.find(m.Replaces); old != nil {
				spec.SpreadReplaces = old.Volume
			}
			if rc.Cluster.Spec.Topology.Type == "dedicatedMasters" {
				// Spread the master quorum and data replicas independently.
				spec.SpreadGroup = rc.Cluster.Name + "/" + m.Kind
			}
			specs = append(specs, spec)
		}
	}
	service, err := rc.Backend.GetService(ctx, m.Name)
	if err != nil {
		return err
	}
	incomplete = incomplete || service != nil && service.Importing
	containers := 1
	if !isES(m) {
		containers = comp.Replicas
	}
	r := runtimeResources(m, comp.Resources)
	if service == nil {
		specs = append(specs, engine.RuntimeServiceSpec(rc.Backend.Layout(), engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: tok.Token, Volume: m.Volume, Resources: r, Containers: containers}))
	}
	if incomplete && m.ImportPlan == nil {
		return fmt.Errorf("%w: member %s has an incomplete import without its original durable batch; recover the original import plan before deployment", engine.ErrConflict, m.Name)
	}
	if m.ImportPlan != nil && (len(specs) > 0 || incomplete) {
		specs = m.ImportPlan.Services(rc.Backend.Layout(), rc.Cluster.Name, m.Name, tok.Token)
		for _, spec := range specs {
			if spec.Hostname == m.Name {
				r, containers = spec.Resources, spec.MinContainers
			}
		}
	}
	if len(specs) == 0 {
		m.Stage = stageImported
		m.ImportPlan = nil
		m.Resources = r
		m.Containers = containers
		m.Disk = comp.Resources
		return nil
	}
	if m.ImportPlan == nil {
		m.ImportPlan = engine.NewImportPlan(specs)
		return nil // Commit the exact batch before a backend can reserve it.
	}
	op, err := m.ImportPlan.Submit(ctx, rc, m.Name, tok.Token)
	v := &pending{Op: op, Action: "import", Member: m.Name, Resources: r, Containers: containers}
	if err != nil {
		return p.failPendingCause(ctx, rc, s, v, err)
	}
	s.Pending = v
	return nil
}
func (p *Provider) deploy(ctx context.Context, rc *engine.Context, s *state, m *member, action, version string) error {
	if err := verifyMemberServices(ctx, rc, m); err != nil {
		return err
	}
	if err := p.publishConfig(ctx, rc, s, m, version, ""); err != nil {
		return err
	}
	spec, err := p.deploymentSpec(rc, m, version)
	if err != nil {
		return err
	}
	if action == "deploy" && m.Stage == stageImported {
		desired := runtimeResources(m, component(rc.Cluster.Spec, m).Resources)
		if op, changed, err := engine.PrepareInitialDeploymentResources(ctx, rc, m.Name, spec, desired, m.Resources); changed || err != nil {
			if err == nil {
				s.Pending = &pending{Op: op, Action: "resources", Member: m.Name, Resources: desired, Version: version}
			}
			return err
		}
	}
	op, err := engine.Deploy(ctx, rc, m.Name, spec)
	v := &pending{Op: op, Action: action, Member: m.Name, Version: version}
	if err != nil {
		return p.failPendingCause(ctx, rc, s, v, err)
	}
	s.Pending = v
	return nil
}

func (p *Provider) deploymentSpec(rc *engine.Context, m *member, version string) (infra.RuntimeSpec, error) {
	url := rc.AgentURL
	if url == "" {
		url = p.options.ControlPlaneURL
	}
	spec, err := RuntimeSpec(rc.Backend.Layout(), RuntimeInput{Hostname: m.Name, Cluster: rc.Cluster.Name, Component: product(m), Version: version, Volume: m.Volume, AgentURL: url, InternalCAPEM: p.options.InternalCAPEM})
	if err != nil {
		return spec, err
	}
	if !isES(m) {
		if args := engine.StatelessReadiness(rc, m.Name, &m.NativeReadiness, "--engine=elasticsearch", "--data-dir="+rc.Backend.Layout().DataDir(m.Volume), product(m)); len(args) > 0 {
			spec.Readiness.Args = append([]string{"agent", "readiness"}, args...)
		}
	}
	return spec, nil
}

// componentConfigurationError is terminal for an applied generation, not for
// independent database operations or future configuration repair.
type componentConfigurationError struct{ message string }

func (e *componentConfigurationError) Error() string { return e.message }
func (*componentConfigurationError) Unwrap() error   { return engine.ErrInvalid }

// componentMemoryPressure is diagnostic, never a terminal configuration error.
type componentMemoryPressure struct{ message string }

func (e *componentMemoryPressure) Error() string { return e.message }

// componentMemoryEvidenceUnavailable is an observation gap, not measured pressure.
type componentMemoryEvidenceUnavailable struct{ message string }

func (e *componentMemoryEvidenceUnavailable) Error() string { return e.message }

// componentHeapExhausted is a retryable child failure, never invalid intent.
type componentHeapExhausted struct{ message string }

func (e *componentHeapExhausted) Error() string { return e.message }

func componentReadiness(ctx context.Context, a *agentapi.Client, m *member) (bool, string, error) {
	ctx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	var observation agentspec.Readiness
	if err := a.Engine(ctx, "elasticsearch", "readiness", nil, &observation); err != nil {
		return false, "Component application readiness unavailable", nil
	}
	if observation.ConfigVersion < m.ConfigVersion {
		return false, "Component is waiting for its current configuration", nil
	}
	if observation.ConfigurationError {
		return false, observation.Message, &componentConfigurationError{message: observation.Message}
	}
	if !observation.Ready && observation.Component == agentspec.ComponentKibana && observation.HeapExhaustion != nil {
		return false, observation.Message, &componentHeapExhausted{message: observation.Message}
	}
	if !observation.Ready && observation.MemoryPressure {
		return false, observation.Message, &componentMemoryPressure{message: observation.Message}
	}
	if !observation.Ready && observation.MemoryPressureUnavailable {
		return false, observation.Message, &componentMemoryEvidenceUnavailable{message: observation.Message}
	}
	return observation.Ready, observation.Message, nil
}

func (p *Provider) componentReady(ctx context.Context, rc *engine.Context, m *member) bool {
	ready, _ := p.componentReadyResult(ctx, rc, m)
	return ready
}

func componentInstancesReady(ctx context.Context, rc *engine.Context, m *member, observed ...engine.InstanceInventory) (bool, string, error) {
	return componentInstancesObservation(ctx, rc, m, observed)
}

func componentInstancesObservation(ctx context.Context, rc *engine.Context, m *member, observed []engine.InstanceInventory, observe ...func(string, agentapi.Status)) (bool, string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var configurationErr *componentConfigurationError
	replicas := max(m.Containers, component(rc.Cluster.Spec, m).Replicas)
	probe := func(client *agentapi.Client) (bool, error) {
		ready, message, nativeErr := componentReadiness(ctx, client, m)
		if nativeErr != nil {
			return false, nativeErr
		}
		if !ready && message != "" {
			return false, errors.New(message)
		}
		return ready, nil
	}
	componentProbe := engine.ComponentProbe{Ready: probe}
	if m.Kind == kindKibana {
		componentProbe.Failed = func(client *agentapi.Client) error {
			_, _, diagnostic := componentReadiness(ctx, client, m)
			var heap *componentHeapExhausted
			if errors.As(diagnostic, &heap) {
				return heap
			}
			return nil
		}
	}
	var ready bool
	var err error
	if len(observed) == 0 {
		ready, err = engine.ComponentInstancesHealthy(ctx, rc, m.Name, replicas, m.ConfigVersion, componentProbe, observe...)
	} else {
		ready, err = engine.ComponentObservedInstancesHealthy(ctx, rc, m.Name, replicas, m.ConfigVersion, observed[0], componentProbe, observe...)
	}
	if err != nil {
		errors.As(err, &configurationErr)
		if configurationErr != nil {
			// Preserve the typed refusal consumed by rolling reconciliation.
			return false, err.Error(), &componentConfigurationError{message: err.Error()}
		}
		var pressure *componentMemoryPressure
		var unavailable *componentMemoryEvidenceUnavailable
		var diagnostic error
		var heap *componentHeapExhausted
		if errors.As(err, &heap) {
			diagnostic = &componentHeapExhausted{message: err.Error()}
		}
		if errors.As(err, &pressure) {
			diagnostic = errors.Join(diagnostic, &componentMemoryPressure{message: err.Error()})
		}
		if errors.As(err, &unavailable) {
			diagnostic = errors.Join(diagnostic, &componentMemoryEvidenceUnavailable{message: err.Error()})
		}
		if diagnostic != nil {
			return false, err.Error(), diagnostic
		}
		return false, err.Error(), nil
	}
	return ready, "", nil
}

func (p *Provider) componentReadyResult(ctx context.Context, rc *engine.Context, m *member) (bool, error) {
	if isES(m) {
		a, err := p.agentClient(ctx, rc, m.Name)
		if err != nil {
			return false, nil
		}
		defer a.Close()
		status, err := engine.ObserveAgentStatus(ctx, a, "Elasticsearch", m.Name)
		if err != nil || status.Error != "" || status.ConfigVersion < m.ConfigVersion {
			return false, nil
		}
		for _, process := range status.Processes {
			if process.State == "paused" || process.Error != "" {
				return false, nil
			}
		}
		return strings.EqualFold(status.Processes[product(m)].State, "running"), nil
	}
	ready, _, readinessErr := componentInstancesReady(ctx, rc, m)
	var pressure *componentMemoryPressure
	var unavailable *componentMemoryEvidenceUnavailable
	var heap *componentHeapExhausted
	if errors.As(readinessErr, &pressure) || errors.As(readinessErr, &unavailable) || errors.As(readinessErr, &heap) {
		// Pressure is a retryable native-readiness wait, not invalid intent.
		return false, nil
	}
	if readinessErr != nil {
		warning := m.ConfigHash + "\n" + readinessErr.Error()
		if m.ConfigurationWarning != warning {
			if rc.Events != nil {
				if err := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Type: "Warning", Reason: "ComponentConfigurationError", Message: m.Name + ": " + readinessErr.Error(), CreatedAt: now(rc)}); err != nil {
					return false, err
				}
			}
			m.ConfigurationWarning = warning
		}
	}
	return ready, readinessErr
}

// Sync makes at most one disruptive platform or database transition per call.
func (p *Provider) Sync(ctx context.Context, rc *engine.Context) (result engine.Result, err error) {
	result.RequeueAfter = 5 * time.Second
	rc.SyncWait = ""
	s, err := load(rc.Cluster)
	if err != nil {
		return result, err
	}
	defer func() {
		var configurationErr *componentConfigurationError
		if errors.As(err, &configurationErr) {
			result.ConvergenceError, err = err, nil
		}
		if e := save(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	if e := observeInitialRestore(ctx, rc, s); e != nil {
		return result, e
	}
	if s.Phase == "ready" {
		s.Phase = "updating"
	}
	if busy, e := p.retireUnplacedScaleDown(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if busy, e := drainRuntimeRecovery(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if busy, e := p.observePending(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if busy, delay, e := p.retryPending(ctx, rc, s); busy || e != nil {
		if delay > 0 {
			result.RequeueAfter = delay
		}
		return result, e
	}
	if changed, e := p.revokeComponentExposure(ctx, rc, s); changed || e != nil {
		return result, e
	}
	if s.UID == "" {
		s.UID, err = secrets.RandomString(16)
		if err != nil {
			return result, err
		}
		s.Phase = "provisioning"
		for _, name := range configuredStorages(rc.Cluster.Spec) {
			s.Storages = append(s.Storages, rc.Cluster.Ref().Namespace+"/"+name)
		}
		for _, kind := range []string{kindMaster, kindNode} {
			count := rc.Cluster.Spec.Components.Extra[kind].Replicas
			if kind == kindNode {
				count = rc.Cluster.Spec.Components.Engine.Replicas
			}
			for range count {
				m, e := p.allocate(rc, s, kind)
				if e != nil {
					return result, e
				}
				if masterEligible(rc.Cluster.Spec, m) {
					s.InitialMasterNodes = append(s.InitialMasterNodes, m.Name)
				}
			}
		}
		return result, nil
	}
	if s.Suspended {
		if s.RestoreActive || s.InitialRestore == "initial-restore" {
			if rc.Events != nil {
				if e := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Type: "Warning", Reason: "RestoreAbandoned", Message: "Deletion interrupted snapshot restore; retained data may be partially restored. The restore will not resume automatically", CreatedAt: now(rc)}); e != nil {
					return result, e
				}
			}
			s.RestoreActive = false
			s.ResumeRestoreReset = true
			if s.InitialRestore == "initial-restore" {
				s.InitialRestore = "abandoned"
				s.InitialRestoreError = "deletion interrupted restore initial-restore; retained data may be incomplete"
			}
			p.bumpConfig(s)
		}
		s.Suspended = false
		s.CleanupBackups = false
		s.CleanupSecrets = false
		s.BackupCleanupStarted = nil
		s.CleanedRepositories = nil
		s.RetainedRepositories = nil
		for _, m := range s.Members {
			m.Stage = stagePlanned
		}
		s.Phase = "provisioning"
		return result, nil
	}
	// Retire unused clients before resolving settings, including storages whose
	// inventory entry was already deleted while the cluster was suspended.
	if changed, e := p.reconcileStorages(ctx, rc, s); changed || e != nil {
		return result, e
	}
	if err = p.skipOccupiedPlans(ctx, rc, s); err != nil {
		return result, err
	}
	if err = p.ensureSecrets(ctx, rc, s); err != nil {
		return result, err
	}
	if busy, err := p.recoverRuntimes(ctx, rc, s); busy || err != nil {
		return result, err
	}
	for _, m := range s.esNodes() {
		if m.Removing {
			if cancelled, e := p.cancelRemoval(ctx, rc, s, m); cancelled || e != nil {
				return result, e
			}
			return result, p.removeStep(ctx, rc, s, m)
		}
		switch m.Stage {
		case stagePlanned:
			return result, p.importMember(ctx, rc, s, m)
		case stageImported:
			if spec, err := p.deploymentSpec(rc, m, rc.Cluster.Spec.Version); err == nil && engine.DeploymentBlocked(rc, m.Name, spec) {
				continue
			}
			return result, p.deploy(ctx, rc, s, m, "deploy", rc.Cluster.Spec.Version)
		}
	}
	if s.Rolling != nil {
		for _, m := range s.componentMembers() {
			switch m.Stage {
			case stagePlanned:
				return result, p.importMember(ctx, rc, s, m)
			case stageImported:
				if spec, err := p.deploymentSpec(rc, m, rc.Cluster.Spec.Version); err == nil && engine.DeploymentBlocked(rc, m.Name, spec) {
					continue
				}
				return result, p.deploy(ctx, rc, s, m, "deploy", rc.Cluster.Spec.Version)
			}
		}
		_, _, err = p.rollingStep(ctx, rc, s)
		return result, err
	}
	c, e := p.es(ctx, rc, s)
	if errors.Is(e, errWaiting) {
		return result, nil
	}
	if e != nil {
		return result, e
	}
	defer c.Close()
	// Membership fans out to every JVM, including paused peers. Keep this
	// read inside the observation cadence: Sync shares the status worker.
	membership, cancelMembership := context.WithTimeout(ctx, 2*time.Second)
	nodes, e := c.Nodes(membership)
	cancelMembership()
	if e != nil {
		return result, fmt.Errorf("elasticsearch convergence-membership read failed (2s budget): inspect member responsiveness/connectivity: %w", e)
	}
	missingMember := false
	for _, m := range s.esNodes() {
		found := false
		for _, node := range nodes {
			if node.Name == m.Name {
				found = true
				m.Stage = stageJoined
			}
		}
		if !found {
			missingMember = true
		}
	}
	if missingMember {
		s.Phase = "forming"
		return result, nil
	}
	if s.ClusterUUID == "" {
		info, e := c.Info(ctx)
		if e != nil {
			return result, e
		}
		if info.ClusterUUID == "" || info.ClusterUUID == "_na_" {
			return result, nil
		}
		s.ClusterUUID = info.ClusterUUID
		s.InitialMasterNodes = nil
		p.bumpConfig(s)
		if e = save(rc.Cluster, s); e != nil {
			return result, e
		}
		for _, m := range s.esNodes() {
			p.reloadMembers(ctx, rc, m.Name)
		}
		return result, nil
	}
	if s.ResumeAllocationReset {
		if e = c.SetAllocationEnable(ctx, nil); e != nil {
			return result, e
		}
		s.ResumeAllocationReset = false
		return result, nil
	}
	if len(s.ResumeExcludeNames) > 0 {
		names, e := c.AllocationExcludeNames(ctx)
		if e != nil {
			return result, e
		}
		names = slices.DeleteFunc(names, func(name string) bool { return slices.Contains(s.ResumeExcludeNames, name) })
		if e = c.SetAllocationExcludeNames(ctx, names); e != nil {
			return result, e
		}
		s.ResumeExcludeNames = nil
		return result, nil
	}
	if s.ResumeVotingReset {
		if e = c.ClearVotingExclusions(ctx, false); e != nil {
			return result, e
		}
		s.ResumeVotingReset = false
		return result, nil
	}
	if s.ResumeRestoreReset {
		if e = c.PutClusterSettings(ctx, nil, map[string]any{"action.destructive_requires_name": nil}); e != nil {
			return result, e
		}
		s.ResumeRestoreReset = false
		return result, nil
	}
	if rc.Cluster.Spec.DataSource != nil && !engine.InitialDataSourceUnproven(rc.Cluster) && s.InitialRestore != "done" && s.InitialRestore != "abandoned" {
		if s.InitialRestore == "" {
			d := rc.Cluster.Spec.DataSource
			ns := d.Namespace
			if ns == "" {
				ns = rc.Cluster.Ref().Namespace
			}
			ref := rc.Cluster.Ref()
			existing, e := rc.Store.GetRestore(ctx, ref, "initial-restore")
			if e == nil {
				if existing.Source.Backup != d.Backup || existing.Source.SourceCluster != d.Cluster || domain.NormalizedNamespace(existing.SourceNamespace) != domain.NormalizedNamespace(ns) {
					return result, engine.ErrConflict
				}
				s.InitialRestore = "initial-restore"
				if e = observeInitialRestore(ctx, rc, s); e != nil {
					return result, e
				}
				return result, nil
			}
			if !missing(e) {
				return result, e
			}
			at := now(rc)
			restore := &domain.Restore{Namespace: ref.Namespace, SourceNamespace: ns, Cluster: ref.Name, Name: "initial-restore", Phase: domain.PhasePending, Source: domain.RestoreSource{SourceCluster: d.Cluster, Backup: d.Backup}, CreatedAt: at}
			payload, _ := json.Marshal(map[string]string{"restore": "initial-restore"})
			op := &domain.Operation{ID: "initial-restore-" + s.UID, Namespace: ref.Namespace, Cluster: ref.Name, Kind: domain.OperationRestore, Phase: domain.PhasePending, Payload: payload, CreatedAt: at}
			if e = rc.Store.EnqueueRestore(ctx, restore, op); e != nil {
				return result, e
			}
			s.InitialRestore = "initial-restore"
		}
		return result, nil
	}
	if s.RestoreActive {
		return result, nil
	}
	if e = p.ensureSecurity(ctx, rc, s, false); e != nil {
		return result, e
	}
	if changed, e := p.ensureSecureSettings(ctx, rc, s); changed || e != nil {
		return result, e
	}
	// Newly enabled components must not run the target Stack version before
	// Elasticsearch has completed its upgrade.
	for _, m := range s.esNodes() {
		if m.Version != rc.Cluster.Spec.Version {
			p.rollingStart(rc, s, "upgrade", rc.Cluster.Spec.Version, "")
			return result, nil
		}
	}
	for _, def := range stackComponents {
		if changed, e := p.reconcileComponent(ctx, rc, s, def); changed || e != nil {
			return result, e
		}
	}
	if changed, e := p.scale(ctx, rc, s); changed || e != nil {
		return result, e
	}
	for _, m := range s.Members {
		if m.Version != rc.Cluster.Spec.Version {
			p.rollingStart(rc, s, "upgrade", rc.Cluster.Spec.Version, "")
			return result, nil
		}
		desired, e := p.desiredSpec(ctx, rc, s, m, rc.Cluster.Spec.Version)
		if e != nil {
			return result, e
		}
		if desired.RestartToken != m.ConfigHash {
			p.rollingStart(rc, s, "config", rc.Cluster.Spec.Version, "")
			return result, nil
		}
		r := runtimeResources(m, component(rc.Cluster.Spec, m).Resources)
		if !reflect.DeepEqual(m.Resources, r) {
			return result, p.issueAction(ctx, rc, s, &pending{Action: "resources", Member: m.Name, Resources: r})
		}
		disk := component(rc.Cluster.Spec, m).Resources
		if m.Volume != "" && (m.Disk.MinDisk != disk.MinDisk || m.Disk.MaxDisk != disk.MaxDisk) {
			return result, p.issueAction(ctx, rc, s, &pending{Action: "disk", Member: m.Name, Resources: domain.Resources{MinDisk: disk.MinDisk, MaxDisk: disk.MaxDisk}})
		}
	}
	for _, name := range configuredStorages(rc.Cluster.Spec) {
		storage, e := rc.Store.GetBackupStorage(ctx, rc.Cluster.Ref().Namespace, name)
		if e != nil {
			return result, e
		}
		if _, e = p.ensureRepository(ctx, rc, s, storage, false, ""); errors.Is(e, errWaiting) {
			return result, nil
		} else if e != nil {
			return result, e
		}
	}
	if changed, e := p.expose(ctx, rc, s); changed || e != nil {
		return result, e
	}
	h, e := c.Health(ctx, esapi.HealthOptions{})
	if e != nil {
		rc.SyncWait = "Elasticsearch health probe failed; shard health is unavailable"
		return result, &healthProbeError{cause: e, message: rc.SyncWait}
	}
	ok, e := healthAcceptable(ctx, c, h, rc.Cluster.Spec.Components.Engine.Replicas)
	if e != nil {
		rc.SyncWait = "Elasticsearch health probe failed while checking unassigned replica placement; " + healthShardCounts(h)
		return result, &healthProbeError{cause: e, message: rc.SyncWait}
	}
	if !ok {
		rc.SyncWait = healthConvergenceMessage(h)
		s.Phase = "recovering"
		return result, nil
	}
	for _, m := range s.componentMembers() {
		ready, readinessErr := p.componentReadyResult(ctx, rc, m)
		if readinessErr != nil || !ready {
			s.Phase = "components"
			return result, readinessErr
		}
	}
	for _, m := range s.componentMembers() {
		m.NativeReadiness = nil
	}
	if rc.Cluster.Spec.DataSource != nil && s.InitialRestore == "abandoned" {
		s.Phase = "restore-failed"
		result.ConvergenceError = operationInvalid("initial snapshot restore was abandoned during deletion; retained data may be incomplete")
		return result, nil
	}
	if engine.InitialDataSourceUnproven(rc.Cluster) {
		s.Phase = "restore"
		result.RequeueAfter = 30 * time.Second
		return result, nil
	}
	s.Phase = "ready"
	rc.Cluster.Status.Phase = domain.PhaseReady
	rc.Cluster.Status.ObservedGeneration = rc.Cluster.Generation
	result.RequeueAfter = 30 * time.Second
	return result, nil
}
