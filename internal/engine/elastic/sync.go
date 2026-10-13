// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
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
	r.RetryAfter = now(rc).Add(engine.PlatformRetryDelay(r.Failures))
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
		op, err = rc.Backend.SetResources(ctx, m.Volume, v.Resources)
	case "containers":
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
		sub, ok := rc.Backend.(infra.SubdomainAccess)
		if !ok {
			return engine.ErrNotSupported
		}
		op, err = sub.SetSubdomainAccess(ctx, m.Name, v.Action == "subdomain-on")
	default:
		return invalid("unknown platform action")
	}
	v.Op = op
	if err != nil {
		return p.failPending(ctx, rc, s, v, err.Error())
	}
	s.Pending = v
	return nil
}
func (p *Provider) observePending(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	v := s.Pending
	if v == nil {
		return false, nil
	}
	if v.Op.ID != "" {
		status, err := rc.Backend.OpStatus(ctx, v.Op)
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
			if err := verifyMemberServices(ctx, rc, m); err != nil {
				return true, err
			}
			m.Stage = stageImported
			m.Resources = v.Resources
			m.Containers = v.Containers
			m.Disk = component(rc.Cluster.Spec, m).Resources
			if rc.Store != nil && m.Volume != "" {
				if err := rc.Store.DeleteRetainedVolume(ctx, rc.Cluster.Ref(), m.Name); err != nil && !missing(err) {
					return true, err
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
	if r := s.PlatformRetries[retryKey(v)]; r != nil {
		if delay := r.RetryAfter.Sub(now(rc)); delay > 0 {
			return true, delay, &engine.PlatformRetryWait{RetryAt: r.RetryAfter, Message: "waiting for scheduled platform retry"}
		}
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
	if m.Volume != "" {
		volume, err := rc.Backend.GetService(ctx, m.Volume)
		if err != nil {
			return err
		}
		if volume == nil {
			specs = append(specs, infra.ServiceSpec{Hostname: m.Volume, Type: "local-storage:single@1", Resources: domain.Resources{MinDisk: comp.Resources.MinDisk, MaxDisk: comp.Resources.MaxDisk}, Env: ownershipEnv(rc.Cluster.Name, m.Name, tok.Token)})
		}
	}
	service, err := rc.Backend.GetService(ctx, m.Name)
	if err != nil {
		return err
	}
	containers := 1
	if !isES(m) {
		containers = comp.Replicas
	}
	r := runtimeResources(m, comp.Resources)
	if service == nil {
		specs = append(specs, infra.ServiceSpec{Hostname: m.Name, Type: "ubuntu@24.04", Resources: r, MinContainers: containers, MaxContainers: containers, Env: ownershipEnv(rc.Cluster.Name, m.Name, tok.Token), Volume: m.Volume})
	}
	if len(specs) == 0 {
		m.Stage = stageImported
		m.Resources = r
		m.Containers = containers
		m.Disk = comp.Resources
		return nil
	}
	op, err := rc.Backend.ImportServices(ctx, specs)
	v := &pending{Op: op, Action: "import", Member: m.Name, Resources: r, Containers: containers}
	if err != nil {
		return p.failPending(ctx, rc, s, v, err.Error())
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
	url := rc.AgentURL
	if url == "" {
		url = p.options.ControlPlaneURL
	}
	doc, err := ZeropsYAML(YAMLInput{Hostname: m.Name, Cluster: rc.Cluster.Name, Component: product(m), Version: version, Volume: m.Volume, AgentURL: url, InternalCAPEM: p.options.InternalCAPEM})
	if err != nil {
		return err
	}
	artifact, err := rc.Artifact.Open(ctx)
	if err != nil {
		return err
	}
	defer artifact.Close()
	op, err := rc.Backend.Deploy(ctx, m.Name, infra.Deployment{Artifact: artifact, ZeropsYAML: doc, SetupName: m.Name, Filename: "karakoram.tar.gz"})
	v := &pending{Op: op, Action: action, Member: m.Name, Version: version}
	if err != nil {
		return p.failPending(ctx, rc, s, v, err.Error())
	}
	s.Pending = v
	return nil
}
func (p *Provider) componentReady(ctx context.Context, rc *engine.Context, m *member) bool {
	a, err := p.agentClient(ctx, rc, m.Name)
	if err != nil {
		return false
	}
	defer a.Close()
	status, err := a.Status(ctx)
	return err == nil && strings.EqualFold(status.Processes[product(m)].State, "running") && status.ConfigVersion >= m.ConfigVersion
}

// Sync makes at most one disruptive platform or database transition per call.
func (p *Provider) Sync(ctx context.Context, rc *engine.Context) (result engine.Result, err error) {
	result.RequeueAfter = 5 * time.Second
	s, err := load(rc.Cluster)
	if err != nil {
		return result, err
	}
	defer func() {
		if e := save(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	if s.Phase == "ready" {
		s.Phase = "updating"
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
		s.Suspended = false
		s.CleanupBackups = false
		s.CleanupSecrets = false
		for _, m := range s.Members {
			m.Stage = stagePlanned
		}
		s.Phase = "provisioning"
		return result, nil
	}
	if err = p.skipOccupiedPlans(ctx, rc, s); err != nil {
		return result, err
	}
	if err = p.ensureSecrets(ctx, rc, s); err != nil {
		return result, err
	}
	if s.Rolling != nil {
		_, _, err = p.rollingStep(ctx, rc, s)
		return result, err
	}
	for _, m := range s.esNodes() {
		if m.Removing {
			return result, p.removeStep(ctx, rc, s, m)
		}
		switch m.Stage {
		case stagePlanned:
			return result, p.importMember(ctx, rc, s, m)
		case stageImported:
			return result, p.deploy(ctx, rc, s, m, "deploy", rc.Cluster.Spec.Version)
		}
	}
	c, e := p.es(ctx, rc, s)
	if errors.Is(e, errWaiting) {
		return result, nil
	}
	if e != nil {
		return result, e
	}
	defer c.Close()
	nodes, e := c.Nodes(ctx)
	if e != nil {
		return result, e
	}
	for _, m := range s.esNodes() {
		found := false
		for _, node := range nodes {
			if node.Name == m.Name {
				found = true
				m.Stage = stageJoined
			}
		}
		if !found {
			s.Phase = "forming"
			return result, nil
		}
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
	if rc.Cluster.Spec.DataSource != nil && s.InitialRestore != "done" {
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
				if existing.Phase == domain.PhaseSucceeded {
					s.InitialRestore = "done"
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
	for _, kind := range []string{kindKibana, kindLogstash, kindAPM} {
		comp := rc.Cluster.Spec.Components.Extra[kind]
		ms := s.members(kind)
		if len(ms) == 0 {
			if comp.Replicas > 0 {
				m, e := p.allocate(rc, s, kind)
				if e != nil {
					return result, e
				}
				if e = p.ensureSecrets(ctx, rc, s); e != nil {
					return result, e
				}
				return result, p.importMember(ctx, rc, s, m)
			}
			continue
		}
		m := ms[0]
		if comp.Replicas == 0 {
			return result, p.deleteComponent(ctx, rc, s, m)
		}
		switch m.Stage {
		case stagePlanned:
			return result, p.importMember(ctx, rc, s, m)
		case stageImported:
			return result, p.deploy(ctx, rc, s, m, "deploy", rc.Cluster.Spec.Version)
		case stageDeployed:
			if !p.componentReady(ctx, rc, m) {
				return result, nil
			}
			m.Stage = stageJoined
			return result, nil
		}
		if m.Containers != comp.Replicas {
			return result, p.issueAction(ctx, rc, s, &pending{Action: "containers", Member: m.Name, Containers: comp.Replicas})
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
	ok, e := p.healthy(ctx, c)
	if e != nil {
		return result, e
	}
	if !ok {
		s.Phase = "recovering"
		return result, nil
	}
	for _, m := range s.componentMembers() {
		if !p.componentReady(ctx, rc, m) {
			s.Phase = "components"
			return result, nil
		}
	}
	s.Phase = "ready"
	rc.Cluster.Status.Phase = domain.PhaseReady
	rc.Cluster.Status.ObservedGeneration = rc.Cluster.Generation
	result.RequeueAfter = 30 * time.Second
	return result, nil
}
