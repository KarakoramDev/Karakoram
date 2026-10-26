// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

type componentState struct {
	Members map[string]*componentMember `json:"members"`
	Pending *componentPending           `json:"pending,omitempty"`
}
type componentMember struct {
	Name            string                 `json:"name"`
	NativeReadiness *bool                  `json:"nativeReadiness,omitempty"`
	RuntimeRecovery engine.RuntimeRecovery `json:"runtimeRecovery,omitempty"`
	Component       string                 `json:"component"`
	ServiceID       string                 `json:"serviceId,omitempty"`
	// ImportBatch retains original membership and the applied resource/count
	// inputs until setup completes; the agent token remains in sealed storage.
	ImportBatch       *engine.ImportPlan          `json:"importBatch,omitempty"`
	Stage             engine.ComponentStage       `json:"stage"`
	Version           string                      `json:"version"`
	ConfigVersion     int64                       `json:"configVersion"`
	ConfigHash        string                      `json:"configHash"`
	Replicas          int                         `json:"replicas"`
	Resources         domain.Resources            `json:"resources"`
	Public            bool                        `json:"public,omitempty"`
	RuntimePort       int                         `json:"runtimePort,omitempty"`
	DeploymentFailure *componentDeploymentFailure `json:"deploymentFailure,omitempty"`
	RuntimeFailure    *componentRuntimeFailure    `json:"runtimeFailure,omitempty"`
}

// Keep terminal deployment evidence across retries and control-plane restarts.
// An imported stage describes lifecycle progress, not the platform's outcome.
type componentDeploymentFailure struct {
	Op     infra.Op `json:"op"`
	Status string   `json:"status"`
	Detail string   `json:"detail,omitempty"`
}

func (f *componentDeploymentFailure) message() string {
	message := fmt.Sprintf("deployment %s for service %s failed (%s); inspect the backend deployment and runtime preparation diagnostics", f.Op.ID, f.Op.ServiceID, f.Status)
	if f.Detail != "" {
		message += "; " + f.Detail
	}
	if f.Status == "DeploymentRetriesExhausted" {
		message += "; automatic deployment retries are exhausted; inspect the failure, then run karactl clusters retry-deployments to re-arm"
	}
	return message
}

type componentRuntimeFailure struct {
	Op     infra.Op             `json:"op"`
	Action string               `json:"action"`
	Target domain.ComponentSpec `json:"target"`
	Status string               `json:"status"`
	Detail string               `json:"detail,omitempty"`
}

func (f *componentRuntimeFailure) message() string {
	return fmt.Sprintf("infrastructure %s operation %s for service %s failed (%s): %s; restore placement or runtime health, then submit a corrective count/resource request", f.Action, f.Op.ID, f.Op.ServiceID, f.Status, f.Detail)
}

type componentPending struct {
	Op               infra.Op             `json:"op"`
	Component        string               `json:"component"`
	Action           string               `json:"action"`
	Replicas         int                  `json:"replicas,omitempty"`
	Resources        domain.Resources     `json:"resources"`
	Public           bool                 `json:"public,omitempty"`
	RuntimePort      int                  `json:"runtimePort,omitempty"`
	SubmittedAt      time.Time            `json:"submittedAt,omitempty"`
	PlatformStatus   string               `json:"platformStatus,omitempty"`
	StatusSince      time.Time            `json:"statusSince,omitempty"`
	Target           domain.ComponentSpec `json:"target,omitempty"`
	RetainedRecovery bool                 `json:"retainedRecovery,omitempty"`
	CorrectsFailure  *infra.Op            `json:"correctsFailure,omitempty"`
}

func (q *componentPending) correctsRuntimeFailure(component string, failure *componentRuntimeFailure, desired domain.ComponentSpec) bool {
	if q == nil || q.Component != component || !reflect.DeepEqual(q.Target, desired) {
		return false
	}
	if q.CorrectsFailure != nil {
		return reflect.DeepEqual(*q.CorrectsFailure, failure.Op)
	}
	// Legacy correction receipts have no acceptance/failure timestamps and Op
	// identities are opaque, not ordered. Conservatively wait only for a distinct
	// accepted receipt with the exact historical corrective shape and full target.
	// This read-only rule neither submits work nor fabricates explicit linkage.
	if q.Op.ID == "" || reflect.DeepEqual(q.Op, failure.Op) || !reflect.DeepEqual(failure.Target, desired) {
		return false
	}
	switch q.Action {
	case "resize":
		return failure.Action == "scale" && reflect.DeepEqual(q.Resources, desired.Resources)
	case "scale":
		return q.RetainedRecovery && q.Replicas == desired.Replicas && reflect.DeepEqual(q.Resources, desired.Resources)
	default:
		return false
	}
}

func (q *componentPending) deploymentMessage(rc *engine.Context) string {
	status := q.PlatformStatus
	if status == "" {
		status = "submitted (awaiting first platform observation)"
	}
	message := fmt.Sprintf("deployment %s for service %s: platform %s", q.Op.ID, q.Op.ServiceID, status)
	now := clockNow(rc)
	if !q.StatusSince.IsZero() {
		message += fmt.Sprintf(" observed for %s", max(time.Duration(0), now.Sub(q.StatusSince)).Round(time.Second))
	}
	if !q.SubmittedAt.IsZero() {
		message += fmt.Sprintf(" (submitted %s ago)", max(time.Duration(0), now.Sub(q.SubmittedAt)).Round(time.Second))
	}
	return message + "; awaiting a definitive platform outcome; automatic replay remains fenced"
}

func loadState(c *domain.Cluster) (*componentState, error) {
	s := &componentState{Members: map[string]*componentMember{}}
	if len(c.Internal) == 0 {
		return s, nil
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(c.Internal, &root); err != nil {
		return nil, err
	}
	if raw := root["supabase"]; len(raw) > 0 {
		if err := json.Unmarshal(raw, s); err != nil {
			return nil, err
		}
	}
	return s, nil
}
func saveState(c *domain.Cluster, s *componentState) error {
	root := map[string]json.RawMessage{}
	if len(c.Internal) > 0 {
		if err := json.Unmarshal(c.Internal, &root); err != nil {
			return err
		}
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	root["supabase"] = raw
	c.Internal, err = json.Marshal(root)
	return err
}
func verifyComponent(ctx context.Context, rc *engine.Context, m *componentMember) (*infra.Service, error) {
	service, err := rc.Backend.GetService(ctx, m.Name)
	if errors.Is(err, infra.ErrNotFound) {
		return nil, nil
	}
	if err != nil || service == nil {
		return service, err
	}
	token, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil {
		return nil, err
	}
	if token == nil {
		return nil, engine.UnverifiedOwnership(service)
	}
	if marker := service.Ownership[engine.OwnerVariable]; marker != "" && marker != engine.OwnerMarker(token.Token, rc.Cluster.Name, m.Name, service.Hostname) {
		return nil, engine.UnverifiedOwnership(service)
	}
	if service.ID != m.ServiceID && !engine.HasOwnership(service, token.Token, rc.Cluster.Name, m.Name) {
		return nil, engine.UnverifiedOwnership(service)
	}
	if err = engine.EnsureOwnerMarker(ctx, rc.Backend, service, token.Token, rc.Cluster.Name, m.Name); err != nil {
		return nil, err
	}
	m.ServiceID = service.ID
	return service, nil
}

func importComponent(ctx context.Context, rc *engine.Context, s *componentState, m *componentMember, desired domain.ComponentSpec) error {
	token, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil {
		return err
	}
	if token == nil {
		return engine.ErrNotFound
	}
	if m.ImportBatch == nil {
		service, err := verifyComponent(ctx, rc, m)
		if err != nil {
			return err
		}
		if service != nil {
			if service.Importing {
				return engine.Invalid("incomplete import for %s has no frozen batch; recover the original import intent before continuing", m.Name)
			}
			m.Stage = engine.ComponentImported
			return nil
		}
		m.ImportBatch = engine.NewImportPlan([]infra.ServiceSpec{engine.RuntimeServiceSpec(rc.Backend.Layout(), engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: token.Token, Resources: desired.Resources, Containers: desired.Replicas})})
		m.Replicas, m.Resources = desired.Replicas, desired.Resources
		// Persist this pass before submitting an import. On a lost response,
		// desired changes must not change the backend's original request hash.
		return nil
	}
	if len(m.ImportBatch.Specs) != 1 || m.ImportBatch.Specs[0].Hostname != m.Name {
		return engine.Invalid("invalid frozen import batch for %s", m.Name)
	}
	op, err := m.ImportBatch.Submit(ctx, rc, m.Name, token.Token)
	if err != nil {
		return err
	}
	s.Pending = &componentPending{Op: op, Component: m.Component, Action: "import"}
	return nil
}

func resumeComponentImport(ctx context.Context, rc *engine.Context, m *componentMember) (*infra.Op, error) {
	if m.ImportBatch == nil {
		service, err := rc.Backend.GetService(ctx, m.Name)
		if err != nil {
			return nil, err
		}
		if service != nil && service.Importing {
			return nil, engine.Invalid("incomplete import for %s has no frozen batch; recover the original import intent before continuing", m.Name)
		}
		return nil, nil
	}
	token, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil {
		return nil, err
	}
	if token == nil {
		return nil, engine.ErrNotFound
	}
	return engine.ResumeImportPlan(ctx, rc, m.ImportBatch, m.Name, token.Token)
}

// Terminal infrastructure receipts must keep their corrective-request and
// bounded-error policy even while independent database recovery is not Ready.
func (p *Provider) reconcileComponentRuntimeFailure(ctx context.Context, rc *engine.Context, s *componentState, m *componentMember, desired domain.ComponentSpec) (bool, error) {
	service, err := verifyComponent(ctx, rc, m)
	if err != nil {
		return true, err
	}
	// Correct resources before retrying a failed count. This is an operator's
	// changed target, not an automatic replay of the same failed rollout.
	if service != nil && !reflect.DeepEqual(service.Resources, desired.Resources) &&
		(m.RuntimeFailure.Action == "scale" || !reflect.DeepEqual(m.RuntimeFailure.Target, desired)) {
		op, err := rc.Backend.SetResources(ctx, m.Name, desired.Resources)
		if err != nil {
			return true, err
		}
		s.Pending = &componentPending{Op: op, Component: m.Component, Action: "resize", Resources: desired.Resources, Target: desired, CorrectsFailure: &m.RuntimeFailure.Op}
		return true, nil
	}
	if !reflect.DeepEqual(m.RuntimeFailure.Target, desired) {
		m.RuntimeFailure = nil
		return false, nil
	}
	if recoverer, ok := rc.Backend.(infra.FailedScaleRecoverer); ok && service != nil && reflect.DeepEqual(service.Resources, desired.Resources) {
		op, recovering, err := recoverer.RecoverFailedScale(ctx, m.Name, desired.Replicas, desired.Replicas)
		if err != nil {
			return true, err
		}
		if recovering {
			s.Pending = &componentPending{Op: op, Component: m.Component, Action: "scale", Replicas: desired.Replicas, Resources: desired.Resources, Target: desired, RetainedRecovery: true, CorrectsFailure: &m.RuntimeFailure.Op}
			return true, nil
		}
	}
	return true, errors.New(m.RuntimeFailure.message())
}

func (p *Provider) reconcileComponentRuntimeFailures(ctx context.Context, rc *engine.Context, s *componentState, canMutate bool) (bool, error) {
	if rc.Cluster.Deleting {
		return false, nil
	}
	for _, def := range stackComponents {
		m := s.Members[def.Name]
		desired := rc.Cluster.Spec.Components.Extra[def.Name]
		if m == nil || m.RuntimeFailure == nil || desired.Replicas <= 0 {
			continue
		}
		if len(rc.Cluster.Status.DeploymentFailures) > 0 {
			runtime, err := p.runtimeSpec(rc, m)
			if err != nil {
				return true, err
			}
			if engine.DeploymentBlocked(rc, m.Name, runtime) {
				continue
			}
		}
		if s.Pending.correctsRuntimeFailure(def.Name, m.RuntimeFailure, desired) {
			// Accepted corrections own their old failure until a terminal poll.
			continue
		}
		if !canMutate {
			// Accepted gateway revocation and other pending receipts keep
			// lifecycle ownership, but cannot hide an unchanged terminal error.
			if reflect.DeepEqual(m.RuntimeFailure.Target, desired) {
				return true, errors.New(m.RuntimeFailure.message())
			}
			continue
		}
		if handled, err := p.reconcileComponentRuntimeFailure(ctx, rc, s, m, desired); handled || err != nil {
			return handled, err
		}
	}
	return false, nil
}

func (p *Provider) observeComponent(ctx context.Context, rc *engine.Context, s *componentState) (bool, error) {
	q := s.Pending
	if q == nil {
		return false, nil
	}
	// Supersede before the gateway pre-pass's first pending observation, not
	// only in the later component reconciler. Imports and deletion receipts
	// retain their normal observation path; backend deletion fences mutations.
	if q.Action != "import" && q.Action != "delete" && rc.Cluster.Spec.Components.Extra[q.Component].Replicas == 0 && engine.CleanupSupersedesDeployment(rc.Backend, q.Op) {
		m := s.Members[q.Component]
		if m == nil {
			return true, engine.ErrNotFound
		}
		// Avoid an ordinary service observation reviving the disabled job.
		op, e := rc.Backend.DeleteServiceByID(ctx, m.Name, m.ServiceID)
		if e != nil {
			return true, e
		}
		s.Pending = &componentPending{Component: q.Component, Action: "delete", Op: op}
		rc.Cluster.Status.Message = "Scaling down: deleting disabled component " + q.Component
		return true, nil
	}
	if !rc.Cluster.Deleting && q.Action == "scale" {
		desired := rc.Cluster.Spec.Components.Extra[q.Component]
		if desired.Replicas > 0 && desired.Replicas < q.Replicas {
			m := s.Members[q.Component]
			if m == nil {
				return true, engine.ErrNotFound
			}
			if _, err := verifyComponent(ctx, rc, m); err != nil {
				return true, err
			}
			op, err := rc.Backend.SetContainerCount(ctx, m.Name, desired.Replicas, desired.Replicas)
			if err != nil {
				return true, err
			}
			s.Pending = &componentPending{Op: op, Component: q.Component, Action: "scale", Replicas: desired.Replicas, Target: desired, CorrectsFailure: q.CorrectsFailure}
			return true, nil
		}
	}
	if !rc.Cluster.Deleting && q.Action == "resize" {
		desired := rc.Cluster.Spec.Components.Extra[q.Component]
		if desired.Replicas > 0 {
			m := s.Members[q.Component]
			if m == nil {
				return true, engine.ErrNotFound
			}
			// Compare against the pending target, not the last successful
			// member resources: a revert can equal those old applied bounds.
			changed, err := engine.ResizeUnplacedDeployment(ctx, rc, m.Name, desired.Resources, &q.Resources, &q.Op)
			if err != nil {
				return true, err
			}
			if changed {
				q.Target.Resources = desired.Resources
				q.PlatformStatus = ""
				q.StatusSince = time.Time{}
				q.SubmittedAt = clockNow(rc)
				return true, nil
			}
		}
	}
	var err error
	if m := s.Members[q.Component]; q.Action == "deploy" && m != nil && !m.RuntimeRecovery.Initialized && m.Stage != engine.ComponentJoined {
		desired := rc.Cluster.Spec.Components.Extra[q.Component].Resources
		var changed bool
		changed, err = engine.ResizeUnplacedDeployment(ctx, rc, m.Name, desired, &m.Resources, &q.Op)
		if changed {
			return true, nil
		}
	}
	if q.Action == "import" {
		m := s.Members[q.Component]
		if m == nil {
			return true, engine.ErrNotFound
		}
		op, err := resumeComponentImport(ctx, rc, m)
		if err != nil {
			return true, err
		}
		if op != nil {
			q.Op = *op
			return true, nil
		}
	}
	var status infra.OpState
	if err == nil {
		status, err = rc.Backend.OpStatus(ctx, q.Op)
	}
	if status.Status == "DeploymentRetriesExhausted" && err == nil {
		err = &infra.DeploymentExhaustedError{Service: q.Op.ServiceID, LastError: status.Message}
	}
	if errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
		m := s.Members[q.Component]
		if m == nil {
			return true, engine.ErrNotFound
		}
		engine.ApplyDeploymentResourceRollback(err, &m.Resources)
		runtime, specErr := p.runtimeSpec(rc, m)
		if specErr != nil {
			return true, specErr
		}
		m.DeploymentFailure = &componentDeploymentFailure{Op: q.Op, Status: "DeploymentRetriesExhausted", Detail: infra.SafeDetail(err)}
		s.Pending = nil
		return false, engine.RecordDeploymentExhausted(ctx, rc, m.Name, runtime, err)
	}
	if err != nil {
		return true, err
	}
	if status.Status != "" && (q.PlatformStatus != status.Status || q.StatusSince.IsZero()) {
		q.PlatformStatus = status.Status
		q.StatusSince = clockNow(rc)
	}
	if rc.Cluster.Deleting && q.Action != "deploy" {
		rc.Cluster.Status.Message = fmt.Sprintf("Waiting for Supabase component %s %s process %s: %s", q.Component, q.Action, q.Op.ID, status.Status)
		if status.Message != "" {
			rc.Cluster.Status.Message += "; " + status.Message
		}
	}
	if !status.Done && !status.Failed {
		return true, nil
	}
	s.Pending = nil
	m := s.Members[q.Component]
	if m == nil {
		return true, engine.ErrNotFound
	}
	if status.Failed {
		if q.Action == "deploy" {
			if status.Status == "RETRY_REQUIRED" {
				// This attempt was never observed submitted. Retire the
				// handle for normal retry without inventing a platform
				// failure or replacing prior terminal deployment evidence.
				return true, nil
			}
			m.DeploymentFailure = &componentDeploymentFailure{Op: q.Op, Status: status.Status, Detail: status.Message}
			return true, errors.New(m.DeploymentFailure.message())
		}
		if q.Action == "scale" || q.Action == "resize" {
			target := q.Target
			if target.Replicas == 0 {
				target = domain.ComponentSpec{Replicas: m.Replicas, Resources: m.Resources}
				if q.Action == "scale" {
					target.Replicas = q.Replicas
				} else {
					target.Resources = q.Resources
				}
			}
			m.RuntimeFailure = &componentRuntimeFailure{Op: q.Op, Action: q.Action, Target: target, Status: status.Status, Detail: status.Message}
			return true, errors.New(m.RuntimeFailure.message())
		}
		return true, fmt.Errorf("infrastructure operation %s failed for Supabase component", q.Action)
	}
	switch q.Action {
	case "import":
		service, err := rc.Backend.GetService(ctx, m.Name)
		if err != nil {
			return true, err
		}
		if id := engine.ImportedServiceID(q.Op, status, service); id != "" {
			m.ServiceID = id
		}
		service, err = verifyComponent(ctx, rc, m)
		if err != nil {
			return true, err
		}
		if service == nil || service.Importing {
			return true, engine.ErrUnavailable
		}
		m.Stage = engine.ComponentImported
		m.ImportBatch = nil
	case "deploy":
		m.Stage = engine.ComponentDeployed
		m.DeploymentFailure = nil
		m.RuntimeFailure = nil
		m.RuntimePort = q.RuntimePort
	case "scale":
		m.Replicas = q.Replicas
		if q.RetainedRecovery {
			m.Resources = q.Resources
		}
		m.RuntimeFailure = nil
		m.Stage = engine.ComponentDeployed
	case "resize":
		m.Resources = q.Resources
		m.RuntimeFailure = nil
		if m.Stage != engine.ComponentImported {
			m.Stage = engine.ComponentDeployed
		}
	case "public":
		m.Public = q.Public
	case "delete":
		service, err := verifyComponent(ctx, rc, m)
		if err != nil {
			return true, err
		}
		if service != nil {
			return true, engine.ErrUnavailable
		}
		for _, key := range []string{"supabase/config/" + m.Name, "supabase/agent-cert/" + m.Name} {
			if err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); err != nil && !missing(err) {
				return true, err
			}
		}
		if err = rc.Secrets.DeleteAgentToken(ctx, rc.Cluster.Ref(), m.Name); err != nil && !missing(err) {
			return true, err
		}
		delete(s.Members, q.Component)
		delete(rc.Cluster.Status.DeploymentFailures, m.Name)
	}
	return true, nil
}
func (p *Provider) Sync(ctx context.Context, rc *engine.Context) (engine.Result, error) {
	certificateErr := p.renewPublishedCertificates(ctx, rc)
	// Restrict exposure and remove explicitly disabled services even when core
	// recovery is not Ready. New optional work stays behind the readiness gate.
	busy, componentErr := p.syncGatewayRestrictions(ctx, rc)
	observed := rc.Cluster.Status.ObservedGeneration
	result, err := p.Core.Sync(ctx, rc)
	// The core's applied generation does not include the Supabase services.
	rc.Cluster.Status.ObservedGeneration = observed
	if busy || componentErr != nil {
		// Optional infrastructure work serializes only other optional work,
		// never database provisioning, replacement, or recovery.
		result.RequeueAfter = 2 * time.Second
		return result, errors.Join(err, componentErr, certificateErr)
	}
	if err != nil || certificateErr != nil {
		return result, errors.Join(err, certificateErr)
	}
	status, err := p.Core.Status(ctx, rc)
	if err != nil {
		return result, err
	}
	coreReady := status.Phase == domain.PhaseReady
	// The shared overlay includes optional-component exhaustion in the
	// cluster phase. It must not turn a healthy PostgreSQL core into a
	// barrier for reconciling the other independent components.
	if !coreReady && len(status.DeploymentFailures) > 0 {
		for _, condition := range status.Conditions {
			if condition.Type == "Ready" && condition.Status == "True" {
				coreReady = true
				break
			}
		}
	}
	if !coreReady {
		return result, nil
	}
	busy, err = p.syncComponents(ctx, rc)
	if err == nil && !busy {
		status, err = p.Status(ctx, rc)
		if err == nil && status.Phase == domain.PhaseReady {
			rc.Cluster.Status.ObservedGeneration = rc.Cluster.Generation
		} else if err == nil {
			// Final gateway DNS convergence needs another observation even when
			// every sequential component lifecycle has already joined.
			busy = true
		}
	}
	if busy || err != nil {
		result.RequeueAfter = 2 * time.Second
	}
	return result, err
}

func (p *Provider) syncGatewayRestrictions(ctx context.Context, rc *engine.Context) (busy bool, err error) {
	s, err := loadState(rc.Cluster)
	if err != nil {
		return false, err
	}
	defer func() {
		// Exposure revocation owns priority over terminal failure handling.
		// Surface existing errors even while its receipt is pending, but do
		// not submit a competing corrective operation or overwrite that receipt.
		failureBusy, failureErr := p.reconcileComponentRuntimeFailures(ctx, rc, s, !busy && err == nil && s.Pending == nil)
		busy = busy || failureBusy
		err = errors.Join(err, failureErr)
		if e := saveState(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	if busy, e := p.observeComponent(ctx, rc, s); busy || e != nil {
		return true, e
	}
	m := s.Members["gateway"]
	if m == nil || !m.Public {
		return p.syncDisabledComponents(ctx, rc, s)
	}
	o, err := ParseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return false, err
	}
	if o.Gateway.PublicHTTPS && rc.Cluster.Spec.Components.Extra["gateway"].Replicas > 0 {
		return p.syncDisabledComponents(ctx, rc, s)
	}
	service, err := verifyComponent(ctx, rc, m)
	if err != nil {
		return true, err
	}
	if service == nil {
		m.Public = false
		return true, nil
	}
	access, err := engine.PublicHTTPS(rc.Backend)
	if err != nil {
		return true, err
	}
	op, err := access.SetSubdomainAccess(ctx, m.Name, false)
	if err != nil {
		return true, err
	}
	s.Pending = &componentPending{Op: op, Component: "gateway", Action: "public", Public: false}
	return true, nil
}

func (p *Provider) syncDisabledComponents(ctx context.Context, rc *engine.Context, s *componentState) (bool, error) {
	// The caller has observed pending receipts and given gateway revocation
	// priority. Only explicit removal is admitted before core readiness.
	for _, def := range stackComponents {
		desired, explicit := rc.Cluster.Spec.Components.Extra[def.Name]
		m := s.Members[def.Name]
		if !explicit || desired.Replicas > 0 || m == nil {
			continue
		}
		// Finish durable import intent before deleting; never start a fresh
		// import for a disabled component or compete with its receipt.
		if m.ImportBatch != nil {
			return true, importComponent(ctx, rc, s, m, desired)
		}
		return true, deleteComponent(ctx, rc, s, m)
	}
	return false, nil
}

func (p *Provider) syncComponents(ctx context.Context, rc *engine.Context) (handled bool, err error) {
	s, err := loadState(rc.Cluster)
	if err != nil {
		return false, err
	}
	defer func() {
		if e := saveState(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	if busy, e := p.observeComponent(ctx, rc, s); busy || e != nil {
		return true, e
	}
	if busy, e := p.reconcileComponentRuntimeFailures(ctx, rc, s, true); busy || e != nil {
		return true, e
	}
	for _, def := range stackComponents {
		m := s.Members[def.Name]
		desired := rc.Cluster.Spec.Components.Extra[def.Name]
		if m != nil && desired.Replicas > 0 && len(rc.Cluster.Status.DeploymentFailures) > 0 {
			runtime, specErr := p.runtimeSpec(rc, m)
			if specErr != nil {
				return true, specErr
			}
			if engine.DeploymentBlocked(rc, m.Name, runtime) {
				continue
			}
		}
		if m != nil && m.ImportBatch != nil {
			return true, importComponent(ctx, rc, s, m, desired)
		}
		current := engine.ComponentRuntime{HorizontalScaling: rc.Backend.Capabilities().HorizontalScaling}
		if m != nil && desired.Replicas > 0 && (m.Stage == engine.ComponentDeployed || m.Stage == engine.ComponentJoined) {
			if busy, e := recoverComponentRuntime(ctx, rc, m); busy || e != nil {
				return true, e
			}
		}
		if m != nil {
			service, e := verifyComponent(ctx, rc, m)
			if e != nil {
				return true, e
			}
			if service == nil || service.Importing {
				m.Stage = engine.ComponentPlanned
				m.ServiceID = ""
				m.Public = false
			}
			current.Exists = true
			current.Stage = m.Stage
			current.Replicas = m.Replicas
			current.Resources = m.Resources
		}
		if m != nil && desired.Replicas > 0 && m.Stage == engine.ComponentJoined {
			o, e := ParseOptions(rc.Cluster.Spec.EngineOptions)
			if e != nil {
				return true, e
			}
			if def.Name == "gateway" && m.Public != o.Gateway.PublicHTTPS {
				access, e := engine.PublicHTTPS(rc.Backend)
				if e != nil {
					return true, e
				}
				op, e := access.SetSubdomainAccess(ctx, m.Name, o.Gateway.PublicHTTPS)
				if e != nil {
					return true, e
				}
				s.Pending = &componentPending{Op: op, Component: def.Name, Action: "public", Public: o.Gateway.PublicHTTPS}
				return true, nil
			}
			// Previous Studio runtimes exposed raw port 3000. A config reload
			// cannot update backend port declarations, so redeploy them too.
			if def.Name == "studio" && m.RuntimePort != ports[def.Name] {
				m.Stage = engine.ComponentImported
				return true, nil
			}
			sp, e := p.desiredSpec(ctx, rc, m)
			if e != nil {
				return true, e
			}
			renew, e := componentCertificateNeedsRenewal(ctx, rc, m)
			if e != nil {
				return true, e
			}
			if sp.RestartToken != m.ConfigHash || renew {
				if sp.Version != m.Version {
					m.Stage = engine.ComponentImported
					return true, nil
				}
				if e = p.publishConfig(ctx, rc, m, sp); e != nil {
					return true, e
				}
				m.Stage = engine.ComponentDeployed
				p.reloadComponent(ctx, rc, m)
				return true, nil
			}
		}
		handled, e := engine.ReconcileStatelessComponent(ctx, def, current, desired, engine.ComponentCallbacks{
			Import: func(ctx context.Context) error {
				if m == nil {
					name := rc.Cluster.Name + codes[def.Name]
					token, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), name)
					if e != nil && !missing(e) {
						return e
					}
					if token == nil || missing(e) {
						value, e := secrets.Token()
						if e != nil {
							return e
						}
						token = &domain.AgentToken{
							Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name,
							Member: name, Token: value, Hash: secrets.Hash(value),
						}
						if e = rc.Secrets.SetAgentToken(ctx, token); e != nil {
							return e
						}
					}
					m = &componentMember{
						Name: name, Component: def.Name, Stage: engine.ComponentPlanned,
						Replicas: desired.Replicas, Resources: desired.Resources,
					}
					s.Members[def.Name] = m
					// Persist the identity before any infrastructure side effect. A
					// restart after import must never replace the ownership token.
					return nil
				}
				return importComponent(ctx, rc, s, m, desired)
			},
			Deploy: func(ctx context.Context) error {
				if _, e := verifyComponent(ctx, rc, m); e != nil {
					return e
				}
				sp, e := p.desiredSpec(ctx, rc, m)
				if e != nil {
					return e
				}
				if e = p.publishConfig(ctx, rc, m, sp); e != nil {
					return e
				}
				runtime, e := p.runtimeSpec(rc, m)
				if e != nil {
					return e
				}
				if m.Stage == engine.ComponentImported {
					if op, changed, e := engine.PrepareInitialDeploymentResources(ctx, rc, m.Name, runtime, desired.Resources, m.Resources); changed || e != nil {
						if e == nil {
							s.Pending = &componentPending{Op: op, Component: def.Name, Action: "resize", Resources: desired.Resources, Target: desired}
						}
						return e
					}
				}
				op, e := engine.Deploy(ctx, rc, m.Name, runtime)
				if e != nil {
					if errors.Is(e, infra.ErrDeploymentRetriesExhausted) {
						m.DeploymentFailure = &componentDeploymentFailure{Status: "DeploymentRetriesExhausted", Detail: infra.SafeDetail(e)}
					}
					return e
				}
				s.Pending = &componentPending{Op: op, Component: def.Name, Action: "deploy", RuntimePort: ports[def.Name], SubmittedAt: clockNow(rc)}
				return nil
			},
			AwaitReady: func(ctx context.Context) error {
				// A configuration correction must reach an instance even when
				// the old configuration prevents it from ever becoming Joined.
				sp, e := p.desiredSpec(ctx, rc, m)
				if e != nil {
					return e
				}
				if sp.RestartToken != m.ConfigHash {
					if sp.Version != m.Version {
						m.Stage = engine.ComponentImported
						return nil
					}
					if e = p.publishConfig(ctx, rc, m, sp); e != nil {
						return e
					}
					p.reloadComponent(ctx, rc, m)
					return nil
				}
				status, e := componentStatus(ctx, rc, m)
				if e != nil {
					return e
				}
				// Bootstrap readiness must not wait for gateway DNS: later
				// upstream services have not been created in this sequential loop.
				if status.Ready {
					m.Stage = engine.ComponentJoined
					m.NativeReadiness = nil
				} else {
					p.reloadComponent(ctx, rc, m)
				}
				return nil
			},
			Scale: func(ctx context.Context, replicas int) error {
				if _, e := verifyComponent(ctx, rc, m); e != nil {
					return e
				}
				op, e := rc.Backend.SetContainerCount(ctx, m.Name, replicas, replicas)
				if e != nil {
					return e
				}
				s.Pending = &componentPending{Op: op, Component: def.Name, Action: "scale", Replicas: replicas, Target: desired}
				return nil
			},
			Resize: func(ctx context.Context, resources domain.Resources) error {
				if _, e := verifyComponent(ctx, rc, m); e != nil {
					return e
				}
				op, e := rc.Backend.SetResources(ctx, m.Name, resources)
				if e != nil {
					return e
				}
				s.Pending = &componentPending{Op: op, Component: def.Name, Action: "resize", Resources: resources, Target: desired}
				return nil
			},
			Delete: func(ctx context.Context) error { return deleteComponent(ctx, rc, s, m) },
		})
		if handled || e != nil {
			return handled, e
		}
	}
	return false, nil
}
func deleteComponent(ctx context.Context, rc *engine.Context, s *componentState, m *componentMember) error {
	service, err := verifyComponent(ctx, rc, m)
	if err != nil {
		return err
	}
	var op infra.Op
	if service != nil {
		if m.Public {
			access, err := engine.PublicHTTPS(rc.Backend)
			if err != nil {
				return err
			}
			op, err = access.SetSubdomainAccess(ctx, m.Name, false)
			if err != nil {
				return err
			}
			s.Pending = &componentPending{Op: op, Component: m.Component, Action: "public"}
			return nil
		}
		op, err = rc.Backend.DeleteServiceByID(ctx, m.Name, m.ServiceID)
		if err != nil {
			return err
		}
	}
	s.Pending = &componentPending{Op: op, Component: m.Component, Action: "delete"}
	return nil
}

// componentObservation keeps agent reachability and child diagnostics separate
// from native application readiness, including every live instance.
type componentObservation struct {
	sbspec.Status
	agent     domain.AgentStatus
	processes map[string]map[string]agentapi.ProcessStatus
}

// Probe timestamps come from a different host. One second covers modest clock
// offsets without extending the five-second cache or fifteen-second age limit.
const storageObservationClockSkew = time.Second

func componentStatus(ctx context.Context, rc *engine.Context, m *componentMember) (componentObservation, error) {
	ctx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	replicas := max(m.Replicas, rc.Cluster.Spec.Components.Extra[m.Component].Replicas)
	var storageFailed atomic.Bool
	inventory, inventoryErr := rc.Backend.Containers(ctx, m.Name)
	var mu sync.Mutex
	agents := make(map[string]agentapi.Status)
	ready, count, err := engine.VisitObservedInstancesWithIdentity(ctx, rc, m.Name, engine.InstanceInventory{Containers: inventory, Err: inventoryErr}, func(client *agentapi.Client, instance string) (bool, error) {
		applied, err := client.Status(ctx)
		if err != nil {
			return false, err
		}
		mu.Lock()
		agents[instance] = applied
		mu.Unlock()
		if applied.Error != "" {
			return false, fmt.Errorf("configuration apply: %s", applied.Error)
		}
		if applied.ConfigVersion < m.ConfigVersion {
			return false, fmt.Errorf("configuration version %d is behind required %d", applied.ConfigVersion, m.ConfigVersion)
		}
		var status sbspec.Status
		if err := client.Engine(ctx, "supabase", "status", struct{}{}, &status); err != nil {
			return false, err
		}
		if m.Component == "storage" {
			if at := status.StorageS3CheckedAt; at != nil {
				now := clockNow(rc)
				if at.IsZero() || at.After(now.Add(storageObservationClockSkew)) || now.Sub(*at) > engine.ReadinessMaxAge {
					storageFailed.Store(true)
					return false, errors.New("storage S3 dependency: authenticated bucket observation is stale or invalid")
				}
			}
			if !status.Ready && status.Reason == "StorageS3Unavailable" {
				storageFailed.Store(true)
			}
		}
		if !status.Ready && status.Message != "" {
			return false, errors.New(status.Message)
		}
		if !status.Ready {
			return false, errors.New("application is not ready")
		}
		return status.Ready, nil
	})
	if count < replicas || count == 0 {
		ready = false
		err = errors.Join(err, fmt.Errorf("member %s has %d live instances; requires %d", m.Name, count, replicas))
	}
	status := componentObservation{
		Status:    sbspec.Status{Ready: ready},
		processes: make(map[string]map[string]agentapi.ProcessStatus, len(agents)),
	}
	status.agent.Healthy = count > 0 && len(agents) == count
	if len(agents) > 0 {
		status.agent.LastSeen = clockNow(rc)
	}
	instances := make([]string, 0, len(agents))
	for instance := range agents {
		instances = append(instances, instance)
	}
	sort.Strings(instances)
	for i, instance := range instances {
		applied := agents[instance]
		if i == 0 || applied.ConfigVersion < status.agent.ConfigVersion {
			status.agent.ConfigVersion = applied.ConfigVersion
		}
		status.processes[instance] = applied.Processes
		member := domain.MemberStatus{Name: m.Name, Health: status.Ready}
		if conditions := engine.ApplyProcessDiagnostics(&member, applied.Processes, instance, clockNow(rc)); len(conditions) > 0 {
			status.Ready = false
			err = errors.Join(err, errors.New(conditions[0].Message))
		}
	}
	if storageFailed.Load() {
		status.Reason = "StorageS3Unavailable"
	}
	if err != nil {
		// Observation failure is current health, not a failure to collect the
		// cluster snapshot: keep database conditions and other member diagnoses.
		status.Message = fmt.Sprintf("%v; inspect %s agent and native application health/connectivity; restore observations before serving traffic", err, m.Name)
	}
	return status, nil
}
func (p *Provider) reloadComponent(ctx context.Context, rc *engine.Context, m *componentMember) {
	if rc.Agents == nil {
		return
	}
	client, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
	if err == nil {
		defer client.Close()
		_ = client.Reload(ctx)
	}
}
func (p *Provider) Cleanup(ctx context.Context, rc *engine.Context, opts domain.DeleteOptions) (done bool, err error) {
	s, err := loadState(rc.Cluster)
	if err != nil {
		return false, err
	}
	rc.Cluster.Status.Message = "Removing Supabase component services and PostgreSQL infrastructure"
	defer func() {
		if e := saveState(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	for _, m := range s.Members {
		if m.RuntimeRecovery.Op == nil && m.RuntimeRecovery.ImportPlan == nil {
			continue
		}
		token, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
		if e != nil {
			return false, e
		}
		if token == nil {
			return false, engine.ErrNotFound
		}
		out, e := engine.ObserveRuntimeRecovery(ctx, rc, engine.RuntimeMember{MemberService: engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: token.Token, Resources: m.Resources, Containers: max(1, m.Replicas)}, ServiceID: &m.ServiceID, Recovery: &m.RuntimeRecovery})
		if e != nil || out.Busy {
			return false, e
		}
	}
	// Deletion supersedes a deployment's application-readiness wait.
	if s.Pending != nil && s.Pending.Action == "deploy" {
		s.Pending = nil
	}
	if s.Pending == nil {
		for _, m := range s.Members {
			if m.ImportBatch == nil {
				continue
			}
			op, e := resumeComponentImport(ctx, rc, m)
			if e != nil {
				return false, e
			}
			if op != nil {
				s.Pending = &componentPending{Op: *op, Component: m.Component, Action: "import"}
				return false, nil
			}
			m.ImportBatch = nil
		}
	}
	if busy, e := p.observeComponent(ctx, rc, s); busy || e != nil {
		return false, e
	}
	for i := len(stackComponents) - 1; i >= 0; i-- {
		if m := s.Members[stackComponents[i].Name]; m != nil {
			return false, deleteComponent(ctx, rc, s, m)
		}
	}
	done, err = p.Core.Cleanup(ctx, rc, opts)
	if err != nil || !done || opts.KeepVolumes {
		return done, err
	}
	for _, key := range []string{credentialsKey, functionsKey, "supabase/smtp-password", "supabase/bootstrap-digest"} {
		if err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); err != nil && !missing(err) {
			return false, err
		}
	}
	return true, nil
}
