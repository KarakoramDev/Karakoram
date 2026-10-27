// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

const restoreTimeout = 10 * time.Second
const restoreFence = time.Minute

var errObservationMutation = errors.New("runtime observation requires serialized reconciliation")

type swarmService struct {
	ID       string `json:"ID"`
	Endpoint struct {
		Ports []swarmPort `json:"Ports"`
	} `json:"Endpoint"`
	Spec struct {
		Name         string `json:"Name"`
		EndpointSpec struct {
			Mode  string      `json:"Mode"`
			Ports []swarmPort `json:"Ports"`
		} `json:"EndpointSpec"`
		Mode struct {
			Replicated *struct {
				Replicas *uint64 `json:"Replicas"`
			} `json:"Replicated"`
		} `json:"Mode"`
		TaskTemplate struct {
			ContainerSpec struct {
				Image  string            `json:"Image"`
				Labels map[string]string `json:"Labels"`
			} `json:"ContainerSpec"`
		} `json:"TaskTemplate"`
	} `json:"Spec"`
}
type swarmSummary struct {
	ID   string `json:"ID"`
	Name string `json:"Name"`
}

func restoreConflict(message string) error {
	return &APIError{Message: message, class: infra.ErrConflict}
}
func sameOperation(p *operation, o operation) bool {
	return p != nil && p.AppID == o.AppID && p.Action == o.Action && p.Issued.Equal(o.Issued)
}

func (b *Backend) inspectService(ctx context.Context, name string) (*swarmService, error) {
	// getConfig runs generic docker inspect: an application name can resolve to
	// a locally built image instead of its service. Resolve a service-scoped ID
	// first; inventory includes stopped services without relying on task containers.
	services, err := b.serviceInventory(ctx)
	if err != nil {
		return nil, err
	}
	for _, summary := range services {
		if summary.Name != name {
			continue
		}
		return b.inspectServiceSummary(ctx, summary)
	}
	// Dokploy excludes this prefix from its service inventory. Missing here
	// cannot establish absence, so never restore or mutate an uninspected service.
	if strings.HasPrefix(name, "dokploy-") {
		return nil, silentFailure("Swarm service name is excluded from Dokploy inventory; ownership inspection is unsupported")
	}
	return nil, nil
}

func (b *Backend) inspectServiceSummary(ctx context.Context, summary swarmSummary) (*swarmService, error) {
	// Dokploy passes this value to a shell. Accept only inventory ID tokens.
	for _, c := range summary.ID {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return nil, silentFailure("Swarm service inventory ID is invalid; retry observation")
		}
	}
	var service *swarmService
	if err := b.c.get(ctx, "docker.getConfig", map[string]string{"containerId": summary.ID}, &service); err != nil {
		return nil, err
	}
	if service == nil || service.Spec.Name != summary.Name || !strings.HasPrefix(service.ID, summary.ID) {
		return nil, silentFailure("Swarm service inspection is unavailable or ambiguous; retry observation")
	}
	return service, nil
}
func (b *Backend) ownsService(a application, m metadata, s *swarmService) bool {
	labels := s.Spec.TaskTemplate.ContainerSpec.Labels
	return a.Name == m.Hostname && a.AppName == s.Spec.Name && s.ID != "" &&
		labels["karakoram.managed"] == "true" && labels["karakoram.hostname"] == m.Hostname &&
		labels["karakoram.kind"] == string(m.Kind) && labels["karakoram.owner"] == m.Vars["KARAKORAM_OWNER"] &&
		(labels["karakoram.environment"] == "" || labels["karakoram.environment"] == b.target.ID) &&
		(labels["karakoram.application"] == "" || labels["karakoram.application"] == a.ID)
}

// Both task lookup and generic inspect swallow Docker errors in Dokploy. Even
// swarm.getNodeApps returns [] on error: only a nonempty, valid inventory is
// evidence of a successful read. An empty/null result is UNKNOWN, not absence.
func (b *Backend) serviceInventory(ctx context.Context) ([]swarmSummary, error) {
	var services []swarmSummary
	if err := b.c.get(ctx, "swarm.getNodeApps", nil, &services); err != nil {
		return nil, err
	}
	if len(services) == 0 {
		return nil, silentFailure("Swarm inventory is empty or unavailable; cannot confirm service absence; check the Dokploy manager Docker daemon")
	}
	for _, s := range services {
		if s.ID == "" || s.Name == "" {
			return nil, silentFailure("Swarm inventory is incomplete; refusing automatic restoration")
		}
	}
	return services, nil
}

func (b *Backend) orphanService(ctx context.Context, hostname string) (*infra.Service, error) {
	services, err := b.serviceInventory(ctx)
	if err != nil {
		return nil, err
	}
	for _, summary := range services {
		if !strings.HasPrefix(summary.Name, hostname+"-") {
			continue
		}
		s, err := b.inspectService(ctx, summary.Name)
		if err != nil {
			return nil, err
		}
		if s != nil && s.Spec.TaskTemplate.ContainerSpec.Labels["karakoram.managed"] == "true" && s.Spec.TaskTemplate.ContainerSpec.Labels["karakoram.hostname"] == hostname {
			return &infra.Service{Hostname: hostname, Kind: infra.KindRuntime, Status: infra.StatusFailed}, restoreConflict("owned Swarm service remains without its Dokploy application; restore the original application record or stop and remove the orphan before replacement; refusing adoption")
		}
	}
	return nil, nil
}

func (b *Backend) checkOrphans(ctx context.Context, prefix string, apps []application) error {
	services, err := b.serviceInventory(ctx)
	if err != nil {
		return err
	}
	for _, summary := range services {
		if !strings.HasPrefix(summary.Name, prefix) {
			continue
		}
		known := false
		for _, a := range apps {
			if m, ok := parseMetadata(a.Description); ok && strings.HasPrefix(summary.Name, m.Hostname+"-") {
				known = true
				break
			}
		}
		if known {
			continue
		}
		service, err := b.inspectService(ctx, summary.Name)
		if err != nil {
			return err
		}
		if service == nil {
			continue
		}
		labels := service.Spec.TaskTemplate.ContainerSpec.Labels
		if labels["karakoram.managed"] == "true" && labels["karakoram.environment"] == b.target.ID && strings.HasPrefix(labels["karakoram.hostname"], prefix) {
			return restoreConflict("owned Swarm service remains without its Dokploy application; restore the original application record or stop and remove the orphan before replacement; refusing adoption")
		}
	}
	return nil
}

func (b *Backend) checkStorage(ctx context.Context, a application, m metadata) error {
	if m.Volume == "" {
		return nil
	}
	v, vm, err := b.require(ctx, m.Volume)
	if err != nil {
		if !errors.Is(err, infra.ErrNotFound) {
			return err
		}
		return restoreConflict("original volume holder is absent; restore its record and verify retained data on the original node before restarting; refusing empty-volume creation")
	}
	// Engine ownership markers include the service hostname, so a legitimate
	// volume/runtime pair has different markers. Compare the holder against
	// its imported binding, never against the runtime's unrelated marker.
	// Older records retain the exact random named-volume mount check below.
	if vm.Kind != infra.KindVolume || vm.Importing || vm.Wiping || vm.Node == "" || vm.Node != m.Node ||
		m.VolumeID != "" && v.ID != m.VolumeID || m.VolumeOwner != "" && vm.Vars["KARAKORAM_OWNER"] != m.VolumeOwner {
		return restoreConflict("original volume holder ownership, node or lifecycle no longer matches; verify retained data before replacement")
	}
	holderMount, runtimeMount := false, false
	for _, mount := range v.Mounts {
		if mount.Type == "volume" && mount.VolumeName == v.AppName && mount.MountPath == "/data" {
			holderMount = true
		}
	}
	for _, mount := range a.Mounts {
		if mount.Type == "volume" && mount.VolumeName == v.AppName && mount.MountPath == b.Layout().VolumePath(m.Volume) {
			runtimeMount = true
		}
	}
	if !holderMount || !runtimeMount {
		return restoreConflict("original named-volume mount changed; verify retained data before restarting; refusing empty-volume creation")
	}
	return b.checkPinnedNode(ctx, m.Node)
}

// mutationReady is called under target-wide exclusion. An ambiguous reload keeps
// a durable delivery fence across control-plane restarts. A queued build has no
// bounded completion time, so its exact operation must finish before Stop/Delete
// or another deployment may supersede it. Never enqueue a second repair build.
func (b *Backend) mutationReady(ctx context.Context, a *application, m *metadata) error {
	if m.Importing {
		return restoreConflict("application import is incomplete; resume the original import before lifecycle mutations")
	}
	if err := b.checkPinnedNode(ctx, m.Node); err != nil {
		return err
	}
	if m.Kind == infra.KindRuntime && m.Deployed {
		service, err := b.inspectService(ctx, a.AppName)
		if err != nil {
			return err
		}
		if service != nil && !b.ownsService(*a, *m, service) {
			return restoreConflict("Swarm service ownership changed; refusing lifecycle mutation of a foreign service")
		}
	}
	if m.PortChange != nil && m.PortChange.RollbackPort != 0 {
		if err := b.rollbackPortChange(ctx, a, m); err != nil {
			return err
		}
		if err := b.c.get(ctx, "application.one", map[string]string{"applicationId": a.ID}, a); err != nil {
			return err
		}
	}
	if m.PortChange != nil && m.PortChange.ReloadPending {
		if err := b.resolvePortDelivery(ctx, a, m); err != nil {
			return err
		}
	}
	if b.nowTime().Before(m.MutationUntil) {
		return restoreConflict("restoration delivery is still fenced; retry the lifecycle operation after the delivery window")
	}
	if m.Pending == nil || m.Pending.Failed && !m.Pending.Uncertain {
		return nil
	}
	if m.Pending.Action != "deploy" && !(m.Desired == "deleting" && m.Pending.Action != "delete" && m.Pending.Action != "stop") {
		return nil
	}
	state, err := b.pollOperation(ctx, encodeOp(*m.Pending))
	if err != nil {
		return err
	}
	if state.Status == "stalled" {
		return restoreConflict(state.Message)
	}
	if !state.Done && !state.Failed {
		if _, cleanup := infra.CleanupIntentFromContext(ctx); cleanup && m.Desired == "deleting" {
			return deploymentWait("PreviousDeployment", []string{a.ID}, "waiting for the exact Dokploy mutation receipt before deletion")
		}
		return restoreConflict("a Dokploy mutation is queued or running; wait for its exact receipt before stopping, deleting or redeploying")
	}
	var current application
	if err = b.c.get(ctx, "application.one", map[string]string{"applicationId": a.ID}, &current); err != nil {
		return err
	}
	updated, ok := parseMetadata(current.Description)
	if !ok {
		return restoreConflict("application ownership metadata changed; refusing mutation")
	}
	*m = updated
	return nil
}

func (b *Backend) observeRuntime(ctx context.Context, a application, m metadata) (infra.Service, error) {
	return b.observeRuntimeMode(ctx, a, m, false)
}

// observeRuntimeMode may stop before a mutation so inventory workers can read
// concurrently. The caller must hold the target lock before permitting writes.
func (b *Backend) observeRuntimeMode(ctx context.Context, a application, m metadata, readOnly bool) (infra.Service, error) {
	if !readOnly {
		ctx = infra.WithoutObservation(ctx)
	}
	s := observe(a, m)
	// Cleanup needs identity, not deployment readiness. Do not restore a runtime
	// which the engine is about to remove, or require its failed build to heal.
	// DeleteService retains the durable receipt and waits for terminal delivery
	// before replacing it with the deletion operation.
	if intent, ok := infra.CleanupIntentFromContext(ctx); ok {
		if intent.Services[m.Hostname] != a.ID {
			return s, restoreConflict("cleanup application identity changed; refusing runtime observation")
		}
		if m.Pending != nil && m.Pending.Failed {
			s.Status = infra.StatusFailed
		}
		return s, nil
	}
	if m.PortChange != nil && m.PortChange.RollbackPort != 0 {
		if readOnly {
			return s, errObservationMutation
		}
		if err := b.rollbackPortChange(ctx, &a, &m); err != nil {
			return s, err
		}
		if err := b.c.get(ctx, "application.one", map[string]string{"applicationId": a.ID}, &a); err != nil {
			return s, err
		}
		s = observe(a, m)
	}
	if m.PortChange != nil && m.PortChange.ReloadPending {
		if readOnly {
			return s, errObservationMutation
		}
		if err := b.resolvePortDelivery(ctx, &a, &m); err != nil {
			s.Status = infra.StatusCreating
			return s, err
		}
		s = observe(a, m)
	}
	if readOnly && m.Pending != nil {
		return s, errObservationMutation
	}
	if m.Desired == "deleting" {
		s.Status = infra.StatusDeleting
		return s, nil
	}
	if m.Desired == "stopped" {
		s.Status = infra.StatusStopped
		if m.Pending != nil {
			state, err := b.pollOperation(ctx, encodeOp(*m.Pending))
			if err != nil {
				return s, err
			}
			if !state.Done || m.Pending.Action != "stop" {
				s.Status = infra.StatusCreating
			}
		}
		return s, nil
	}
	// A terminal endpoint repair failure leaves the existing tasks in place.
	// Observe them normally so the engine can prove native health and reach
	// the bounded repair retry path; do not relax other lifecycle failures.
	if m.Pending != nil && !terminalEndpointRepair(m.Pending) {
		if m.Pending.Failed && !m.Pending.Uncertain {
			s.Status = infra.StatusFailed
			return s, silentFailure("previous lifecycle operation failed; consult Dokploy deployment logs and explicitly retry the operation")
		}
		state, err := b.pollOperation(ctx, encodeOp(*m.Pending))
		if err != nil {
			return s, err
		}
		if state.Failed && (m.Pending.Action != "endpoint-repair" || state.Status == "stalled") {
			s.Status = infra.StatusFailed
			return s, silentFailure(state.Message)
		}
		if !state.Done && !state.Failed {
			s.Status = infra.StatusCreating
			return s, nil
		}
		// pollOperation persists completion. Do not write a stale pending handle
		// back while retaining the image discovered by the completing operation.
		var full application
		if err = b.c.get(ctx, "application.one", map[string]string{"applicationId": a.ID}, &full); err != nil {
			return s, err
		}
		a = full
		m, _ = parseMetadata(a.Description)
	}
	var service *swarmService
	var storageErr, inspectionErr error
	var ds []deploymentRecord
	readErr := infra.ObserveParallel(ctx, 3, func(i int) error {
		switch i {
		case 0:
			service, inspectionErr = b.inspectService(ctx, a.AppName)
			return inspectionErr
		case 1:
			storageErr = b.checkStorage(ctx, a, m)
			return storageErr
		default:
			var err error
			ds, err = b.deployments(ctx, a.ID)
			return err
		}
	})
	if service != nil && !b.ownsService(a, m, service) {
		s.Status = infra.StatusFailed
		return s, errors.Join(readErr, restoreConflict("Swarm service ownership does not match the Dokploy application; refusing adoption or overwrite; remove the foreign service or restore the original owned service"))
	}
	if storageErr != nil || inspectionErr != nil {
		s.Status = infra.StatusFailed
	}
	if readErr != nil {
		return s, readErr
	}
	var err error
	if a.Status == "error" || len(ds) > 0 && (ds[0].Status == "error" || ds[0].Status == "cancelled") {
		s.Status = infra.StatusFailed
		return s, silentFailure("Dokploy deployment failed or was cancelled; consult deployment logs and explicitly redeploy after correcting the failure")
	}
	for _, d := range ds {
		if d.Status == "running" || d.Status == "queued" {
			s.Status = infra.StatusCreating
			return s, nil
		}
	}
	if service != nil && (service.Spec.Mode.Replicated == nil || service.Spec.Mode.Replicated.Replicas == nil) {
		s.Status = infra.StatusFailed
		return s, restoreConflict("Swarm service mode is not replicated; refusing ambiguous restoration")
	}
	if m.Desired == "" {
		// Legacy deployed metadata cannot distinguish Stop from external loss.
		// Only a visible, owned, desired-running service establishes intent.
		if service == nil || *service.Spec.Mode.Replicated.Replicas == 0 || a.Status == "idle" {
			s.Status = infra.StatusFailed
			return s, restoreConflict("legacy application has no durable running intent; explicitly Start it in Karakoram after verifying ownership and retained data")
		}
		m.Desired = "running"
		m.Image = service.Spec.TaskTemplate.ContainerSpec.Image
		if readOnly {
			return s, errObservationMutation
		}
		if err = b.update(ctx, &a, m, nil); err != nil {
			return s, err
		}
	}
	if m.Desired != "running" {
		s.Status = infra.StatusFailed
		return s, restoreConflict("application desired state is unknown; refusing automatic restoration")
	}
	if service == nil {
		inventory, err := b.serviceInventory(ctx)
		if err != nil {
			s.Status = infra.StatusFailed
			return s, err
		}
		for _, entry := range inventory {
			if entry.Name == a.AppName {
				s.Status = infra.StatusFailed
				return s, silentFailure("Swarm service is listed but inspection is unavailable; check Docker on the manager; refusing ambiguous restoration")
			}
		}
		if readOnly {
			return s, errObservationMutation
		}
		return b.restoreRuntime(ctx, a, m)
	}
	if *service.Spec.Mode.Replicated.Replicas == 0 {
		if readOnly {
			return s, errObservationMutation
		}
		return b.restoreRuntime(ctx, a, m)
	}
	ts, err := b.tasks(ctx, a.AppName)
	if err != nil {
		return s, err
	}
	s.Status = infra.StatusCreating
	for _, t := range ts {
		if strings.EqualFold(t.State, "running") && strings.HasPrefix(strings.ToLower(t.CurrentState), "running") {
			s.Status = infra.StatusRunning
			break
		}
	}
	return s, nil
}

func (b *Backend) restoreRuntime(ctx context.Context, a application, m metadata) (infra.Service, error) {
	s := observe(a, m)
	s.Status = infra.StatusCreating
	if m.Volume != "" && !m.WriterFenced {
		s.Status = infra.StatusFailed
		return s, restoreConflict("legacy image has no volume-lifetime writer fence; explicitly redeploy from Karakoram before enabling automatic runtime restoration")
	}
	if b.nowTime().Before(m.MutationUntil) {
		return s, nil
	}
	// Inventory/inspect cannot exclude an out-of-band operator creating a foreign
	// service between this check and Dokploy's unconditional update-or-create.
	// The target must be exclusively managed by one Karakoram control plane.
	// Visible foreign objects are refused; the random Dokploy appName is retained.
	action := "deploy"
	if m.Image != "" {
		var images []struct {
			Repository string `json:"Repository"`
			Tag        string `json:"Tag"`
		}
		if err := b.c.get(ctx, "dockerImage.getImages", nil, &images); err != nil {
			return s, err
		}
		image := strings.Split(m.Image, "@")[0]
		for _, i := range images {
			if i.Repository+":"+i.Tag == image {
				action = "restart"
				break
			}
		}
	}
	if action == "deploy" && (a.SourceType != "drop" || a.BuildType != "dockerfile") {
		s.Status = infra.StatusFailed
		return s, restoreConflict("retained Dockerfile deployment source is unavailable; explicitly redeploy the member from Karakoram")
	}
	o, err := b.operation(ctx, &a, m, action)
	if err != nil {
		return s, err
	}
	if action == "restart" {
		tasks, err := b.tasks(ctx, a.AppName)
		if err != nil {
			return s, err
		}
		if runningTasks(tasks) {
			return s, nil
		}
	}
	var releaseSlot func(bool)
	submitted := false
	if action == "deploy" {
		releaseSlot, err = b.deploymentSlot(ctx, a.ID)
		if err != nil {
			return s, err
		}
		defer func() { releaseSlot(submitted) }()
	}
	work, cancel := context.WithTimeout(ctx, restoreTimeout)
	defer cancel()
	o.WriterFenced = m.WriterFenced
	m.Pending = &o
	m.MutationUntil = b.nowTime().Add(restoreFence)
	if err = b.update(work, &a, m, nil); err != nil {
		return s, err
	}
	proc := "application.deploy"
	body := map[string]any{"applicationId": a.ID}
	if action == "restart" {
		proc = "application.reload"
		body["appName"] = a.AppName
	}
	submitted = true
	if err = b.c.post(work, proc, body, nil); err != nil {
		if action == "deploy" && enqueueRefused(err) {
			submitted = false
			m.Pending = nil
			m.MutationUntil = time.Time{}
			if saveErr := b.update(ctx, &a, m, nil); saveErr != nil {
				return s, errors.Join(err, saveErr)
			}
		}
		return s, err
	}
	// Acknowledged synchronous reload needs no delivery margin. Queued builds
	// remain excluded by their pending operation, even across process restarts.
	m.MutationUntil = time.Time{}
	return s, b.update(ctx, &a, m, nil)
}
