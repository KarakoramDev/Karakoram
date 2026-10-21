// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

func hash(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func resourceUpdatesDeployRuntime(backend infra.Backend) bool {
	resourceDeployment, ok := backend.(infra.RuntimeResourceDeployment)
	return ok && resourceDeployment.ResourceUpdatesDeployRuntime()
}
func (p *Provider) agent(ctx context.Context, rc *engine.Context, name string) (*agentapi.Client, error) {
	return rc.Agents.Dial(ctx, rc.Cluster.Ref(), name)
}
func (p *Provider) call(ctx context.Context, rc *engine.Context, name, action string, req, resp any) error {
	a, err := p.agent(ctx, rc, name)
	if err != nil {
		return err
	}
	defer a.Close()
	return a.Engine(ctx, "rabbitmq", action, req, resp)
}
func (p *Provider) action(ctx context.Context, rc *engine.Context, name, action string, req any) error {
	if req == nil {
		req = rmqspec.EmptyRequest{}
	}
	var out rmqspec.ActionResponse
	if err := p.call(ctx, rc, name, action, req, &out); err != nil {
		return err
	}
	if !out.OK {
		return fmt.Errorf("RabbitMQ %s %s was not acknowledged", name, action)
	}
	return nil
}
func (p *Provider) brokerStatus(ctx context.Context, rc *engine.Context, name string) (rmqspec.StatusResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	var st rmqspec.StatusResponse
	a, err := p.agent(ctx, rc, name)
	if err != nil {
		return st, err
	}
	defer a.Close()
	err = a.Engine(ctx, "rabbitmq", "observe", rmqspec.EmptyRequest{}, &st)
	var response *agentapi.HTTPError
	if errors.As(err, &response) && response.StatusCode == http.StatusNotFound {
		// Older agents predate the HTTP observation route. Do not disguise a
		// failed fresh native read as success or retry it through the slow CLI.
		err = a.Engine(ctx, "rabbitmq", "status", rmqspec.EmptyRequest{}, &st)
	}
	return st, err
}

// Formation needs cookie-authenticated CLI access before managed HTTP credentials
// exist. It is still an observation: bound it separately from bootstrap/join.
func (p *Provider) formationStatus(ctx context.Context, rc *engine.Context, name string) (rmqspec.StatusResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	var status rmqspec.StatusResponse
	err := p.call(ctx, rc, name, "status", rmqspec.EmptyRequest{}, &status)
	if err != nil {
		return status, fmt.Errorf("RabbitMQ formation cluster_status read exceeded %s budget or failed: inspect member %s responsiveness/connectivity: %w", engine.ObservationTimeout, name, err)
	}
	return status, err
}
func (p *Provider) agentStatus(ctx context.Context, rc *engine.Context, name string) (agentapi.Status, error) {
	ctx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	a, err := p.agent(ctx, rc, name)
	if err != nil {
		return agentapi.Status{}, err
	}
	defer a.Close()
	return a.Status(ctx)
}
func running(st agentapi.Status) bool {
	return strings.EqualFold(st.Processes["rabbitmq-server"].State, "running")
}
func noProblems(raw json.RawMessage) bool {
	s := strings.TrimSpace(string(raw))
	return s == "" || s == "null" || s == "[]" || s == "{}"
}
func (p *Provider) healthy(ctx context.Context, rc *engine.Context, m *member) (bool, error) {
	_, healthy, err := p.memberHealth(ctx, rc, m)
	return healthy, err
}

func (p *Provider) memberHealth(ctx context.Context, rc *engine.Context, m *member) (rmqspec.StatusResponse, bool, error) {
	var st agentapi.Status
	var b rmqspec.StatusResponse
	var agentErr, brokerErr error
	if m.Stage == "deployed" {
		// Formation still gates the expensive cookie-authenticated CLI on an
		// applied, running process; managed HTTP credentials do not exist yet.
		st, agentErr = p.agentStatus(ctx, rc, m.Name)
		if agentErr == nil && running(st) && st.ConfigVersion >= m.ConfigVersion {
			b, brokerErr = p.formationStatus(ctx, rc, m.Name)
		}
	} else {
		// These independent live reads share a deadline, not serial budgets.
		var probes sync.WaitGroup
		probes.Go(func() { st, agentErr = p.agentStatus(ctx, rc, m.Name) })
		probes.Go(func() { b, brokerErr = p.brokerStatus(ctx, rc, m.Name) })
		probes.Wait()
	}
	if agentErr != nil && brokerErr != nil && agentErr.Error() == brokerErr.Error() {
		return b, false, agentErr
	}
	if err := errors.Join(agentErr, brokerErr); err != nil {
		return b, false, err
	}
	if !running(st) || st.Processes["rabbitmq-server"].Error != "" || st.ConfigVersion < m.ConfigVersion {
		return b, false, nil
	}
	return b, b.MembershipError == "" && b.Version != "" && slices.Contains(b.RunningNodes, "rabbit@"+m.Name) && noProblems(b.Alarms) && noProblems(b.Partitions) && !b.Maintenance, nil
}
func (p *Provider) importMember(ctx context.Context, rc *engine.Context, s *internalState, m *member) error {
	if err := verifyMemberServices(ctx, rc, m); err != nil {
		return err
	}
	tok, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil {
		return err
	}
	specs := []infra.ServiceSpec{}
	incomplete := false
	for _, volume := range []bool{true, false} {
		name := m.Name
		if volume {
			name = m.Volume
		}
		service, err := rc.Backend.GetService(ctx, name)
		if err != nil {
			return err
		}
		incomplete = incomplete || service != nil && service.Importing
		if service != nil {
			continue
		}
		if volume {
			spec := engine.VolumeServiceSpec(rc.Cluster.Name, m.Name, m.Volume, tok.Token, rc.Cluster.Spec.Components.Engine.Resources)
			spec.SpreadReplaces = m.ReplacesVolume
			specs = append(specs, spec)
		} else {
			specs = append(specs, engine.RuntimeServiceSpec(rc.Backend.Layout(), engine.MemberService{
				Cluster: rc.Cluster.Name, Member: m.Name, Token: tok.Token, Volume: m.Volume,
				Resources: runtimeResources(rc.Cluster.Spec.Components.Engine.Resources), Containers: 1,
			}))
		}
	}
	m.Resources = runtimeResources(rc.Cluster.Spec.Components.Engine.Resources)
	m.Disk = rc.Cluster.Spec.Components.Engine.Resources
	if incomplete && m.ImportPlan == nil {
		return fmt.Errorf("%w: member %s has an incomplete import without its original durable batch; recover the original import plan before deployment", engine.ErrConflict, m.Name)
	}
	if m.ImportPlan != nil && (len(specs) > 0 || incomplete) {
		specs = m.ImportPlan.Services(rc.Backend.Layout(), rc.Cluster.Name, m.Name, tok.Token)
		for _, spec := range specs {
			if spec.Hostname == m.Name {
				m.Resources = spec.Resources
			}
			if spec.Hostname == m.Volume {
				m.Disk.MinDisk, m.Disk.MaxDisk = spec.Resources.MinDisk, spec.Resources.MaxDisk
			}
		}
	}
	if len(specs) == 0 {
		m.Stage = "imported"
		m.ImportPlan = nil
		s.resetPlatformRetry(&pending{Action: "import", Member: m.Name})
		return nil
	}
	if m.ImportPlan == nil {
		m.ImportPlan = engine.NewImportPlan(specs)
		return nil // Commit the exact batch before a backend can reserve it.
	}
	op, err := m.ImportPlan.Submit(ctx, rc, m.Name, tok.Token)
	s.Pending = &pending{Op: op, Action: "import", Member: m.Name}
	if err != nil {
		return s.failPendingCause(rc, err)
	}
	return nil
}
func (p *Provider) deploy(ctx context.Context, rc *engine.Context, s *internalState, m *member, action, version string) error {
	if err := verifyMemberServices(ctx, rc, m); err != nil {
		return err
	}
	m.DesiredVersion = version
	if err := s.save(rc.Cluster); err != nil {
		return err
	}
	spec, err := p.runtimeSpec(rc, m, version)
	if err != nil {
		return err
	}
	if action == "deploy" && m.Stage == "imported" && !m.Resume {
		desired := runtimeResources(rc.Cluster.Spec.Components.Engine.Resources)
		if op, changed, err := engine.PrepareInitialDeploymentResources(ctx, rc, m.Name, spec, desired, m.Resources); changed || err != nil {
			if err == nil {
				s.Pending = &pending{Op: op, Action: "resources", Member: m.Name, Resources: desired, Version: version}
			}
			return err
		}
	}
	op, err := engine.Deploy(ctx, rc, m.Name, spec)
	if errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
		s.resetPlatformRetry(&pending{Action: action, Member: m.Name})
		return err
	}
	s.Pending = &pending{Op: op, Action: action, Member: m.Name, Version: version}
	if err != nil {
		return s.failPendingCause(rc, err)
	}
	return nil
}

// Sync changes at most one broker or platform action and persists observations even on retryable failures.
func (p *Provider) Sync(ctx context.Context, rc *engine.Context) (result engine.Result, err error) {
	result.RequeueAfter = time.Second
	s, e := loadState(rc.Cluster)
	if e != nil {
		return result, e
	}
	defer func() {
		if e := s.save(rc.Cluster); err == nil {
			err = e
		}
	}()
	if busy, e := p.retireUnplacedScaleDown(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if busy, e := drainRuntimeRecovery(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if busy, e := p.observePending(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if busy, e := p.retryPending(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if _, e := p.observeResumeDeployments(ctx, rc, s); e != nil {
		return result, e
	}
	if s.Suspended {
		s.Suspended = false
		s.CleanupBackups = false
		s.BackupCleanupStarted = nil
		s.RetainedBackups = nil
		s.CleanupSecrets = false
		s.Bootstrapped = false
		for _, m := range s.Members {
			if m.VolumeOrphaned {
				m.Removing = true
				m.RemoveStep = "delete-volume"
				continue
			}
			m.Stage = "planned"
			m.Resume = true
			m.Removing = false
			m.RemoveStep = ""
			m.CleanupStopReason = ""
			m.ChangeAction = ""
			m.ChangeStep = ""
			m.ChangeDisruptedAt = time.Time{}
			m.ChangeStartupMessage = ""
			m.PublicHash = ""
			m.ConfigVersion = 0
		}
		s.Phase = "resume"
		return result, nil
	}
	if s.UID == "" {
		uid, e := secrets.Token()
		if e != nil {
			return result, e
		}
		s.UID = uid
	}
	// A replacement owns its temporary extra member until the grow/shrink workflow finishes.
	if !s.ReplaceActive {
		active := []*member{}
		for _, m := range ordered(s) {
			if !m.Removing {
				active = append(active, m)
			}
		}
		desired := rc.Cluster.Spec.Components.Engine.Replicas
		for len(active) < desired {
			m, e := allocateNewMember(ctx, rc, s)
			if e != nil {
				return result, e
			}
			active = append(active, m)
			s.Rebalance = s.Bootstrapped
		}
		if len(active) > desired {
			for i := len(active) - 1; i >= desired; i-- {
				active[i].Removing = true
			}
		}
	}
	if e = p.ensureSecrets(ctx, rc, s); e != nil {
		return result, e
	}
	prepareCreation(s)
	// Access repair uses the cookie-authenticated CLI. It must run before
	// formation/recovery gates that now require the managed HTTP account.
	accessErr := p.reconcileManagedAccess(ctx, rc, s)
	if accessErr == nil {
		accessErr = p.reconcileDataAccess(ctx, rc, s)
	}
	// A missing native quorum may also need the runtime/rolling recovery below.
	// Preserve the repair error without preventing that work from progressing.
	defer func() { err = errors.Join(err, accessErr) }()
	if busy, err := p.recoverRuntimes(ctx, rc, s); busy || err != nil {
		return result, err
	}
	// Finish an in-flight rolling member before provisioning or changing any other member.
	for _, m := range ordered(s) {
		if m.ChangeAction != "" && m.ChangeAction != "password" {
			s.Phase = "upgrade"
			_, e = p.changeStep(ctx, rc, s, m)
			// A replacement allocation reports running before its prepare script
			// starts the agent, and the broker starts after the agent. These are
			// expected post-disruption observations, not failed mutations. Keep
			// their diagnostics visible without exponential error backoff.
			if rollingStartupPending(m.ChangeStep, e) {
				at := now(rc)
				// Old checkpoints have no disruption timestamp. Start elapsed
				// startup diagnostics once, without moving the checkpoint.
				if m.ChangeDisruptedAt.IsZero() {
					m.ChangeDisruptedAt = at
				}
				threshold := m.ChangeDisruptedAt.Add(engine.DefaultReadinessTimeout)
				message := fmt.Sprintf("member %s waiting for rolling startup at checkpoint %s (startup diagnostic threshold %s): %s", m.Name, m.ChangeStep, threshold.Format(time.RFC3339), e)
				if !at.Before(threshold) {
					message = fmt.Sprintf("member %s exceeded its %s rolling startup diagnostic threshold at checkpoint %s; inspect the runtime prepare/agent logs and broker startup logs; waiting within the operation's observed-time budget: %s", m.Name, engine.DefaultReadinessTimeout, m.ChangeStep, e)
				}
				// A slow replacement is still an expected startup observation.
				// The tracked operation's observed-time budget bounds this wait;
				// turning it into a counted error can fail a converging rollout.
				result.ConvergenceError = &engine.PlatformRetryWait{
					RetryAt: at.Add(engine.ReadinessInterval),
					Message: message,
					Cause:   e,
				}
				m.ChangeStartupMessage = result.ConvergenceError.Error()
				result.RequeueAfter = engine.ReadinessInterval
				return result, nil
			}
			if e == nil {
				m.ChangeStartupMessage = ""
			}
			if e == nil && len(m.QuorumBlockers) > 0 {
				result.RequeueAfter = quorumBlockedRequeueAfter
			}
			return result, e
		}
	}
	// Distributed initial platform fan-out is independent of broker capability.
	// Non-seeds stay supervisor-held until capability negotiation/native turns.
	// Retained provisioning keeps independent receipts and reverse stop order.
	provisioning := ordered(s)
	for _, m := range provisioning {
		if m.Resume {
			provisioning = resumeMembers(s)
			break
		}
	}
	var admissionWait error
provision:
	for _, m := range provisioning {
		if m.Removing || m.ResumeDeploy != nil || !m.Resume && (len(provisioning) == 1 || s.Bootstrapped && !m.InitialFormation) {
			continue
		}
		switch m.Stage {
		case "planned":
			s.Phase = "resume"
			if !m.Resume {
				s.Phase = "import"
			}
			return result, p.importMember(ctx, rc, s, m)
		case "imported":
			if resumeDeployments(s) >= resumeDeploymentLimit {
				continue
			}
			if spec, err := p.runtimeSpec(rc, m, rc.Cluster.Spec.Version); err == nil && engine.DeploymentBlocked(rc, m.Name, spec) {
				continue
			}
			s.Phase = "resume"
			if !m.Resume {
				s.Phase = "deploy"
			}
			if e = applyProvidedCertificate(ctx, rc, m); e != nil {
				return result, e
			}
			e = p.deploy(ctx, rc, s, m, "deploy", rc.Cluster.Spec.Version)
			if e == nil {
				m.ResumeDeploy = s.Pending
				s.Pending = nil
			}
			var waiting *infra.DeploymentWaitError
			if m.Resume && errors.As(e, &waiting) && waiting.Reason == "DeploymentSlot" {
				admissionWait = e
				break provision
			}
			return result, e
		}
	}
	if admissionWait != nil {
		// Admission must not starve already-deployed brokers. Preserve its
		// deadline alongside any start error so neither failure is hidden.
		defer func() { err = errors.Join(err, admissionWait) }()
	}
	// Fill a free retained-deployment slot before contacting a completed
	// broker: start can wait for native recovery and must not delay its peers.
	for _, m := range provisioning {
		if m.Removing || m.ResumeDeploy != nil {
			continue
		}
		switch m.Stage {
		case "resume-deployed":
			a, e := p.agent(ctx, rc, m.Name)
			if e != nil {
				return result, e
			}
			e = a.Maintenance(ctx, agentapi.Maintenance{Enabled: false})
			a.Close()
			if e != nil {
				return result, e
			}
			if e = p.action(ctx, rc, m.Name, "start", nil); e != nil {
				return result, e
			}
			m.Stage = "resume-revive"
			return result, nil
		}
	}
	if admissionWait != nil {
		return result, nil
	}
	// Agent capability timeouts must not sit ahead of an available import or
	// deployment slot. Native startup/verification can overlap peer deploys.
	p.observeCreationCapabilities(ctx, rc, s)
	for _, m := range s.Members {
		if m.Resume || !s.Bootstrapped && m.Name == s.CreationSeed {
			if m.ResumeDeploy != nil || m.Stage == "planned" || m.Stage == "imported" {
				return result, nil
			}
		}
		if s.CreationSeed == "" && m.ResumeDeploy != nil {
			return result, nil
		}
	}
	for _, m := range ordered(s) {
		if !m.Removing && m.InitialFormation && (m.ResumeDeploy != nil || m.Stage == "planned" || m.Stage == "imported") {
			// Native turns remain ordered, including legacy peers. Platform
			// fan-out above proceeds independently of these native turns.
			return result, nil
		}
		if m.Removing || m.ResumeDeploy != nil || m.Resume && (m.Stage == "planned" || m.Stage == "imported" || m.Stage == "resume-deployed") {
			continue
		}
		switch m.Stage {
		case "planned":
			s.Phase = "import"
			return result, p.importMember(ctx, rc, s, m)
		case "imported":
			if spec, err := p.runtimeSpec(rc, m, rc.Cluster.Spec.Version); err == nil && engine.DeploymentBlocked(rc, m.Name, spec) {
				continue
			}
			s.Phase = "deploy"
			if e = applyProvidedCertificate(ctx, rc, m); e != nil {
				return result, e
			}
			return result, p.deploy(ctx, rc, s, m, "deploy", rc.Cluster.Spec.Version)
		case "resume-revive":
			if e = p.action(ctx, rc, m.Name, "revive", nil); e != nil {
				return result, e
			}
			m.Resume = false
			m.Stage = "deployed"
			return result, nil
		case "deployed":
			s.Phase = "agents"
			if !s.Bootstrapped && s.CreationSeed != "" && m.Name != s.CreationSeed {
				return result, nil
			}
			if m.InitialFormation && s.CreationSeed != "" && m.Name != s.CreationSeed && !m.Resume && !m.FormationVerified && (!m.FormationGate || !s.find(s.CreationSeed).FormationGate) && !m.FormationLegacyRelease {
				// A mixed-version peer stays supervisor-held until its own
				// serial turn and a positively healthy established seed.
				if !m.FormationChecked || !s.Bootstrapped {
					return result, nil
				}
				seed := s.find(s.CreationSeed)
				if seed == nil || seed.Stage != "ready" {
					return result, nil
				}
				healthy, err := p.healthy(ctx, rc, seed)
				if err != nil || !healthy {
					return result, err
				}
				m.FormationLegacyRelease = true
				return result, nil
			}
			st, healthy, e := p.memberHealth(ctx, rc, m)
			if e != nil {
				return result, e
			}
			if !healthy {
				return result, nil
			}
			if !s.Bootstrapped {
				s.Phase = "bootstrap"
				if e = p.action(ctx, rc, m.Name, "bootstrap", nil); e != nil {
					return result, e
				}
				s.Bootstrapped = true
				s.SystemUsersReconciledAt = now(rc)
				s.SystemUsersError = ""
				m.Stage = "ready"
				m.FormationVerified = true
				return result, nil
			}
			s.Phase = "join"
			seed := p.availableMembers(ctx, rc, s)
			if len(seed) == 0 {
				return result, fmt.Errorf("no healthy seed member")
			}
			if !slices.Contains(st.RunningNodes, "rabbit@"+seed[0].Name) {
				return result, p.action(ctx, rc, m.Name, "join", rmqspec.JoinRequest{Seed: "rabbit@" + seed[0].Name})
			}
			m.Stage = "ready"
			m.FormationVerified = true
			s.Rebalance = true
			return result, nil
		}
	}
	// Retiring brokers keep their Resume marker until removal, but accepted
	// deployments must still become terminal before the safe removal workflow.
	for _, m := range s.Members {
		if m.ResumeDeploy != nil || m.Resume && !m.Removing {
			return result, nil
		}
	}
	if removing := removingMembers(s); len(removing) > 0 {
		s.Phase = "remove"
		return result, p.removeStep(ctx, rc, s, removing[0])
	}
	for _, m := range ordered(s) {
		r := rc.Cluster.Spec.Components.Engine.Resources
		if !reflect.DeepEqual(m.Resources, runtimeResources(r)) {
			s.Phase = "resources"
			if resourceUpdatesDeployRuntime(rc.Backend) || m.Resources.CPUMode != r.CPUMode || m.Resources.MinCPU != r.MinCPU || m.Resources.MaxCPU != r.MaxCPU || !reflect.DeepEqual(m.Resources.StartCPU, r.StartCPU) {
				beginChange(rc, s, m, "resources")
				return result, nil
			}
			return result, p.submit(ctx, rc, s, &pending{Action: "resources", Member: m.Name, Resources: runtimeResources(r)})
		}
		if m.Disk.MinDisk != r.MinDisk || m.Disk.MaxDisk != r.MaxDisk {
			s.Phase = "resources"
			return result, p.submit(ctx, rc, s, &pending{Action: "disk", Member: m.Name, Resources: r})
		}
		pending, e := hasPendingCertificate(ctx, rc, m)
		if e != nil {
			return result, e
		}
		if !pending {
			pending, e = providedCertificateChanged(ctx, rc, m)
			if e != nil {
				return result, e
			}
		}
		if m.Version != rc.Cluster.Spec.Version || m.TLSMode != rc.Cluster.Spec.TLS.Mode || m.EngineConfig != rc.Cluster.Spec.EngineConfig || hash(m.EngineOptions) != hash(rc.Cluster.Spec.EngineOptions) || !reflect.DeepEqual(m.ConfigResources, m.Resources) || m.ConfigDisk.MinDisk != m.Disk.MinDisk || m.ConfigDisk.MaxDisk != m.Disk.MaxDisk || pending {
			if m.Version != rc.Cluster.Spec.Version {
				s.FeatureFlags = true
			}
			beginChange(rc, s, m, "deploy")
			s.Phase = "upgrade"
			return result, nil
		}
	}
	if s.FeatureFlags {
		ms := s.runningMembers()
		if len(ms) == 0 {
			return result, engine.ErrNotFound
		}
		if e = p.action(ctx, rc, ms[0].Name, "enable-feature-flags", nil); e != nil {
			return result, e
		}
		s.FeatureFlags = false
		s.Rebalance = true
		return result, nil
	}
	if s.Rebalance {
		ms := s.runningMembers()
		if len(ms) > 0 {
			if e = p.action(ctx, rc, ms[0].Name, "rebalance", nil); e != nil {
				return result, e
			}
		}
		s.Rebalance = false
		return result, nil
	}
	if changed, e := p.syncExposure(ctx, rc, s); changed || e != nil {
		s.Phase = "exposure"
		return result, e
	}
	// A minority outage must not prevent claiming recovery operations. Status
	// reports incomplete membership as Degraded; removal retains its own gates.
	members := s.runningMembers()
	names := make([]string, len(members))
	samples := make([]struct {
		broker  rmqspec.StatusResponse
		healthy bool
		err     error
	}, len(members))
	var probes sync.WaitGroup
	for i, m := range members {
		names[i] = "rabbit@" + m.Name
		probes.Go(func() {
			samples[i].broker, samples[i].healthy, samples[i].err = p.memberHealth(ctx, rc, m)
		})
	}
	probes.Wait()
	healthyMembers := 0
	complete := true
	var observationErr error
	for _, sample := range samples {
		if sample.err != nil || !sample.healthy {
			complete = false
			if sample.err != nil && observationErr == nil {
				observationErr = sample.err
			}
			continue
		}
		healthyMembers++
		for _, name := range names {
			if !slices.Contains(sample.broker.RunningNodes, name) {
				complete = false
			}
		}
	}
	if !complete {
		if observationErr != nil && healthyMembers <= len(names)/2 {
			return result, observationErr
		}
		return result, nil
	}
	retained, e := rc.Store.ListRetainedVolumes(ctx, rc.Cluster.Ref())
	if e != nil {
		return result, e
	}
	for _, v := range retained {
		if e = rc.Store.DeleteRetainedVolume(ctx, rc.Cluster.Ref(), v.Member); e != nil && !missing(e) {
			return result, e
		}
	}
	if rc.Cluster.Spec.DataSource != nil && !engine.InitialDataSourceUnproven(rc.Cluster) && !s.CloneImported {
		s.Phase = "restore"
		if e = s.save(rc.Cluster); e != nil {
			return result, e
		}
		done, e := p.cloneStep(ctx, rc)
		if e != nil {
			return result, e
		}
		fresh, e := loadState(rc.Cluster)
		if e != nil {
			return result, e
		}
		s = fresh
		if !done {
			return result, nil
		}
	}
	if engine.InitialDataSourceUnproven(rc.Cluster) {
		s.Phase = "restore"
		result.RequeueAfter = 30 * time.Second
		return result, nil
	}
	s.Phase = "ready"
	rc.Cluster.Status.ObservedGeneration = rc.Cluster.Generation
	result.RequeueAfter = 30 * time.Second
	return result, nil
}

func rollingStartupPending(step string, err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if child != nil && !rollingStartupPending(step, child) {
				return false
			}
		}
		return true
	}
	if step == "reload" {
		return errors.Is(err, syscall.ECONNREFUSED)
	}
	if step == "wait" {
		var response *agentapi.HTTPError
		return errors.As(err, &response) && response.StatusCode == http.StatusServiceUnavailable &&
			response.Action == "/v1/rabbitmq/observe"
	}
	return false
}
func beginChange(rc *engine.Context, s *internalState, m *member, action string) {
	m.ChangeAction = action
	m.ChangeStep = "quorum"
	m.ChangeDisruptedAt = time.Time{}
	m.ChangeStartupMessage = ""
	m.TargetVersion = rc.Cluster.Spec.Version
	m.TargetResources = runtimeResources(rc.Cluster.Spec.Components.Engine.Resources)
	m.TargetTLS = rc.Cluster.Spec.TLS.Mode
	m.TargetConfig = rc.Cluster.Spec.EngineConfig
	m.TargetOptions = append(json.RawMessage(nil), rc.Cluster.Spec.EngineOptions...)
	if action == "restart" || action == "tls" {
		m.TargetVersion = m.Version
		m.TargetResources = m.ConfigResources
		m.TargetTLS = m.TLSMode
		m.TargetConfig = m.EngineConfig
		m.TargetOptions = append(json.RawMessage(nil), m.EngineOptions...)
	} else if m.Version != rc.Cluster.Spec.Version {
		s.FeatureFlags = true
	}
}
func (p *Provider) changeStep(ctx context.Context, rc *engine.Context, s *internalState, m *member) (bool, error) {
	if m.ChangeStep == "redeploy" || m.ChangeStep == "platform" && m.ChangeAction != "restart" && m.ChangeAction != "resources" {
		if spec, err := p.runtimeSpec(rc, m, m.TargetVersion); err == nil && engine.DeploymentBlocked(rc, m.Name, spec) {
			return false, nil
		}
	}
	// The current member has already crossed the disruption boundary. Let a
	// corrected configuration repair that member instead of waiting forever
	// for the invalid activated configuration to become healthy. Keep the
	// captured binary version and resources: this is not an implicit downgrade.
	if (m.ChangeStep == "reload" || m.ChangeStep == "start" || m.ChangeStep == "wait") &&
		(m.TargetConfig != rc.Cluster.Spec.EngineConfig || hash(m.TargetOptions) != hash(rc.Cluster.Spec.EngineOptions)) {
		m.TargetConfig = rc.Cluster.Spec.EngineConfig
		m.TargetOptions = append(json.RawMessage(nil), rc.Cluster.Spec.EngineOptions...)
		m.ChangeStep = "repair-stop"
	}
	switch m.ChangeStep {
	case "quorum":
		if len(s.runningMembers()) > 1 {
			if err := p.action(ctx, rc, m.Name, "await-quorum", rmqspec.AwaitQuorumRequest{TimeoutSeconds: 30}); err != nil {
				if blocked, checkErr := p.checkQuorum(ctx, rc, m); blocked || checkErr != nil {
					return false, checkErr
				}
				return false, err
			}
		}
		m.ChangeStep = "critical"
	case "critical":
		if len(s.runningMembers()) > 1 {
			if blocked, err := p.checkQuorum(ctx, rc, m); blocked || err != nil {
				return false, err
			}
		} else if err := clearQuorumBlockers(ctx, rc, m); err != nil {
			return false, err
		}
		m.ChangeStep = "drain"
	case "drain":
		if err := p.action(ctx, rc, m.Name, "drain", nil); err != nil {
			return false, err
		}
		m.ChangeStep = "stop"
	case "stop", "repair-stop":
		a, err := p.agent(ctx, rc, m.Name)
		if err != nil {
			return false, err
		}
		defer a.Close()
		if err = a.Maintenance(ctx, agentapi.Maintenance{Enabled: true}); err != nil {
			return false, err
		}
		if err = p.action(ctx, rc, m.Name, "stop", nil); err != nil {
			return false, err
		}
		if m.ChangeDisruptedAt.IsZero() {
			m.ChangeDisruptedAt = now(rc)
		}
		m.ChangeStep = "platform"
	case "platform": // New config and leaves are published only after the old broker is safely stopped.
		if err := publishRenewedCertificates(ctx, rc, m); err != nil {
			return false, err
		}
		if err := applyProvidedCertificate(ctx, rc, m); err != nil {
			return false, err
		}
		s.ConfigVersion++
		m.ConfigVersion = s.ConfigVersion
		m.DesiredVersion = m.TargetVersion
		m.TLSMode = m.TargetTLS
		m.EngineConfig = m.TargetConfig
		m.EngineOptions = append(json.RawMessage(nil), m.TargetOptions...)
		m.ConfigResources = m.TargetResources
		m.ConfigDisk = m.Disk
		if err := s.save(rc.Cluster); err != nil {
			return false, err
		}
		if m.ChangeAction == "resources" {
			return false, p.submit(ctx, rc, s, &pending{Action: "resources", Member: m.Name, Resources: m.TargetResources})
		}
		action := "upgrade"
		if m.ChangeAction == "restart" {
			action = "restart"
		}
		return false, p.submit(ctx, rc, s, &pending{Action: action, Member: m.Name, Version: m.TargetVersion})
	case "redeploy":
		return false, p.submit(ctx, rc, s, &pending{Action: "upgrade", Member: m.Name, Version: m.TargetVersion})
	case "reload":
		a, err := p.agent(ctx, rc, m.Name)
		if err != nil {
			return false, err
		}
		defer a.Close()
		if err = a.Reload(ctx); err != nil {
			return false, err
		}
		m.ChangeStep = "start"
	case "start":
		a, err := p.agent(ctx, rc, m.Name)
		if err != nil {
			return false, err
		}
		defer a.Close()
		if err = a.Maintenance(ctx, agentapi.Maintenance{Enabled: false}); err != nil {
			return false, err
		}
		if err = p.action(ctx, rc, m.Name, "start", nil); err != nil {
			return false, err
		}
		m.ChangeStep = "wait"
	case "wait":
		var st agentapi.Status
		var b rmqspec.StatusResponse
		var agentErr, brokerErr error
		var probes sync.WaitGroup
		probes.Go(func() { st, agentErr = p.agentStatus(ctx, rc, m.Name) })
		probes.Go(func() { b, brokerErr = p.brokerStatus(ctx, rc, m.Name) })
		probes.Wait()
		err := errors.Join(agentErr, brokerErr)
		if err != nil {
			return false, err
		}
		if !running(st) || st.Processes["rabbitmq-server"].Error != "" || st.ConfigVersion < m.ConfigVersion {
			return false, nil
		}
		if b.MembershipError != "" || !slices.Contains(b.RunningNodes, "rabbit@"+m.Name) || !noProblems(b.Alarms) || !noProblems(b.Partitions) {
			return false, nil
		}
		for _, peer := range s.runningMembers() {
			if !slices.Contains(b.RunningNodes, "rabbit@"+peer.Name) {
				return false, nil
			}
		}
		m.ChangeStep = "revive"
	case "revive":
		if err := p.action(ctx, rc, m.Name, "revive", nil); err != nil {
			return false, err
		}
		m.ChangeAction = ""
		m.ChangeStep = ""
		m.ChangeDisruptedAt = time.Time{}
		m.ChangeStartupMessage = ""
		m.Stage = "ready"
		return true, nil
	default:
		return false, fmt.Errorf("unknown member change checkpoint %q", m.ChangeStep)
	}
	return false, nil
}
func (p *Provider) removeStep(ctx context.Context, rc *engine.Context, s *internalState, m *member) (err error) {
	previousBlock := m.RemovalBlocked
	defer func() {
		if m.RemovalBlocked != "" && m.RemovalBlocked != previousBlock {
			if eventErr := warningEvent(ctx, rc, "MemberRemovalBlocked", "Member "+m.Name+": "+m.RemovalBlocked); eventErr != nil {
				err = eventErr
			}
		}
	}()
	survivors := s.runningMembers()
	if m.RemoveStep == "" {
		if m.Stage == "planned" || m.Stage == "imported" {
			m.RemoveStep = "delete-runtime"
		} else {
			m.RemoveStep = "check-queues"
		}
	}
	if m.RemoveStep != "delete-runtime" && m.RemoveStep != "delete-volume" {
		if len(survivors) == 0 {
			return blockRemoval(m, "no survivor for member removal")
		}
		healthy := make([]bool, len(survivors))
		var probes sync.WaitGroup
		for i, peer := range survivors {
			probes.Go(func() {
				ok, err := p.healthy(ctx, rc, peer)
				healthy[i] = err == nil && ok
			})
		}
		probes.Wait()
		for i, peer := range survivors {
			if !healthy[i] {
				return blockRemoval(m, fmt.Sprintf("remaining member %s is unavailable; removal is paused", peer.Name))
			}
		}
	}
	seed := ""
	if len(survivors) > 0 {
		seed = survivors[0].Name
	}
	switch m.RemoveStep {
	case "check-queues", "grow", "streams", "shrink", "drain", "stop-app":
		lost, err := p.departureUnavailable(ctx, rc, m)
		if err != nil {
			return err
		}
		m.Lost = m.Lost || lost
		if m.Lost {
			if err := p.guardLostMajority(ctx, rc, s, m, survivors); err != nil {
				return err
			}
		}
		if err := p.guardClassicQueues(ctx, rc, s, m); err != nil {
			return err
		}
	}
	switch m.RemoveStep {
	case "check-queues":
		m.RemoveStep = "grow"
	case "grow":
		if m.GrowIndex < len(survivors) {
			if err := p.action(ctx, rc, seed, "grow", rmqspec.NodeRequest{Node: "rabbit@" + survivors[m.GrowIndex].Name}); err != nil {
				return err
			}
			m.GrowIndex++
			return nil
		}
		m.RemoveStep = "streams"
	case "streams":
		done, err := p.migrateStreams(ctx, rc, s, m)
		if err != nil {
			return err
		}
		if done {
			m.RemoveStep = "shrink"
		}
	case "shrink":
		if err := p.action(ctx, rc, seed, "shrink", rmqspec.NodeRequest{Node: "rabbit@" + m.Name}); err != nil {
			return err
		}
		m.RemoveStep = "drain"
		if m.Lost {
			m.RemoveStep = "forget"
		}
	case "drain":
		if m.Lost {
			m.RemoveStep = "forget"
			return nil
		}
		if err := p.action(ctx, rc, m.Name, "drain", nil); err != nil {
			return err
		}
		m.RemoveStep = "stop-app"
	case "stop-app":
		if m.Lost {
			m.RemoveStep = "forget"
			return nil
		}
		a, err := p.agent(ctx, rc, m.Name)
		if err != nil {
			return err
		}
		err = a.Maintenance(ctx, agentapi.Maintenance{Enabled: true})
		a.Close()
		if err != nil {
			return err
		}
		// The Erlang VM remains a live Khepri voter until the survivor forgets it.
		if err = p.action(ctx, rc, m.Name, "stop-app", nil); err != nil {
			return err
		}
		m.RemoveStep = "forget"
	case "forget":
		lost, err := p.departureUnavailable(ctx, rc, m)
		if err != nil {
			return err
		}
		m.Lost = m.Lost || lost
		if m.Lost {
			if err := p.guardLostMajority(ctx, rc, s, m, survivors); err != nil {
				return err
			}
		}
		if err := p.action(ctx, rc, seed, "forget", rmqspec.NodeRequest{Node: "rabbit@" + m.Name}); err != nil {
			return err
		}
		m.RemovalBlocked = ""
		m.RemoveStep = "stop"
	case "stop":
		st, agentErr := p.agentStatus(ctx, rc, m.Name)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if agentErr == nil && running(st) {
			a, err := p.agent(ctx, rc, m.Name)
			if err != nil {
				return err
			}
			err = a.Maintenance(ctx, agentapi.Maintenance{Enabled: true})
			a.Close()
			if err != nil {
				return err
			}
			if err = p.action(ctx, rc, m.Name, "stop", nil); err != nil {
				return err
			}
		}
		m.RemoveStep = "delete-runtime"
	case "delete-runtime":
		if err := rc.Backend.DeletePublicPorts(ctx, m.Name); err != nil {
			return err
		}
		if m.ManagementPublic {
			return p.submit(ctx, rc, s, &pending{Action: "cleanup-management", Member: m.Name})
		}
		return p.submit(ctx, rc, s, &pending{Action: "delete-runtime", Member: m.Name})
	case "delete-volume":
		if m.VolumeOrphaned {
			if err := deleteMemberSecrets(ctx, rc, m.Name); err != nil {
				return err
			}
			removeRecord(s, m.Name)
			delete(rc.Cluster.Status.DeploymentFailures, m.Name)
			return nil
		}
		return p.submit(ctx, rc, s, &pending{Action: "delete-volume", Member: m.Name})
	default:
		return fmt.Errorf("unknown removal checkpoint %q", m.RemoveStep)
	}
	return nil
}

func blockRemoval(m *member, message string) error {
	m.RemovalBlocked = message
	return fmt.Errorf("%w: %s", engine.ErrConflict, message)
}

func (p *Provider) departureServicesUnavailable(ctx context.Context, rc *engine.Context, m *member) (bool, error) {
	for _, target := range []struct{ name, id string }{{m.Name, m.ServiceID}, {m.Volume, m.VolumeID}} {
		service, err := engine.ObserveRetiringService(ctx, rc.Backend, target.name, target.id, m.Volume, m.VolumeID, m.RuntimeRecovery.VolumeLost)
		if err != nil {
			return false, err
		}
		if service == nil {
			return true, nil
		}
	}
	return m.RuntimeRecovery.VolumeLost, nil
}

func (p *Provider) departureUnavailable(ctx context.Context, rc *engine.Context, m *member) (bool, error) {
	if lost, err := p.departureServicesUnavailable(ctx, rc, m); lost || err != nil {
		return lost, err
	}
	st, err := p.agentStatus(ctx, rc, m.Name)
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	return err != nil || !running(st), nil
}

func (p *Provider) guardClassicQueues(ctx context.Context, rc *engine.Context, s *internalState, m *member) error {
	if err := s.save(rc.Cluster); err != nil {
		return err
	}
	admin, err := p.adminClient(ctx, rc)
	if err != nil {
		return err
	}
	defer admin.Close()
	queues, err := admin.Queues(ctx)
	if err != nil {
		return err
	}
	blocked := []string{}
	node := "rabbit@" + m.Name
	for _, q := range queues {
		if q.Durable && (q.Type == "classic" || q.Type == "") && (q.Node == node || q.Leader == node) {
			blocked = append(blocked, fmt.Sprintf("%q/%q", q.Vhost, q.Name))
		}
	}
	if len(blocked) > 0 {
		slices.Sort(blocked)
		return blockRemoval(m, "durable classic queues on "+m.Name+" prevent automatic removal: "+strings.Join(blocked, ", ")+"; explicitly migrate or delete these queues before retrying")
	}
	m.RemovalBlocked = ""
	return nil
}
func (p *Provider) migrateStreams(ctx context.Context, rc *engine.Context, s *internalState, m *member) (bool, error) {
	admin, err := p.adminClient(ctx, rc)
	if err != nil {
		return false, err
	}
	defer admin.Close()
	queues, err := admin.Queues(ctx)
	if err != nil {
		return false, err
	}
	survivors := s.runningMembers()
	for _, q := range queues {
		if q.Type != "stream" || !slices.Contains(q.Members, "rabbit@"+m.Name) {
			continue
		}
		for _, peer := range survivors {
			if !slices.Contains(q.Members, "rabbit@"+peer.Name) {
				return false, p.streamReplica(ctx, rc, survivors[0].Name, rmqspec.StreamReplicasRequest{Action: "add", VHost: q.Vhost, Queue: q.Name, Node: "rabbit@" + peer.Name})
			}
		}
		return false, p.streamReplica(ctx, rc, survivors[0].Name, rmqspec.StreamReplicasRequest{Action: "delete", VHost: q.Vhost, Queue: q.Name, Node: "rabbit@" + m.Name})
	}
	return true, nil
}

func (p *Provider) streamReplica(ctx context.Context, rc *engine.Context, name string, req rmqspec.StreamReplicasRequest) error {
	var out rmqspec.StreamReplicasResponse
	return p.call(ctx, rc, name, "stream-replicas", req, &out)
}

// availableMembers skips failed brokers when choosing a replacement's join seed.
func (p *Provider) availableMembers(ctx context.Context, rc *engine.Context, s *internalState) []*member {
	members := s.runningMembers()
	available := make([]bool, len(members))
	var probes sync.WaitGroup
	for i, m := range members {
		probes.Go(func() {
			lost, err := p.departureServicesUnavailable(ctx, rc, m)
			if err != nil || lost {
				return
			}
			// memberHealth already observes the process and applied config;
			// repeating departureUnavailable's agent probe only adds latency.
			healthy, err := p.healthy(ctx, rc, m)
			available[i] = err == nil && healthy
		})
	}
	probes.Wait()
	out := make([]*member, 0, len(members))
	for i, m := range members {
		if available[i] {
			out = append(out, m)
		}
	}
	return out
}

func (p *Provider) guardLostMajority(ctx context.Context, rc *engine.Context, s *internalState, m *member, survivors []*member) error {
	if len(survivors) == 0 {
		return blockRemoval(m, "lost-node removal requires surviving brokers")
	}
	st, err := p.brokerStatus(ctx, rc, survivors[0].Name)
	if err != nil {
		return err
	}
	if st.MembershipError != "" {
		return blockRemoval(m, "lost-node removal requires fresh membership: "+st.MembershipError)
	}
	voters := max(len(st.DiskNodes), len(s.Members))
	online := 0
	for _, peer := range survivors {
		if slices.Contains(st.RunningNodes, "rabbit@"+peer.Name) {
			online++
		}
	}
	if online < voters/2+1 {
		return blockRemoval(m, "lost-node removal requires a Khepri voter majority among the surviving brokers")
	}
	return nil
}
