// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"slices"
	"strconv"
	"strings"
	"time"
)

const rabbitMQ435Warning = "RabbitMQ 4.3.5 is deprecated because of stream authorization disclosures GHSA-6jvc-jx8w-m757 and GHSA-342v-cj47-c9g8. Use RabbitMQ 4.3.6, which fixes both disclosures."

func mergePatch(target, patch any) any {
	p, yes := patch.(map[string]any)
	if !yes {
		return patch
	}
	t, yes := target.(map[string]any)
	if !yes {
		t = map[string]any{}
	}
	for k, v := range p {
		if v == nil {
			delete(t, k)
		} else {
			t[k] = mergePatch(t[k], v)
		}
	}
	return t
}
func tlsValues(t *api.TLSInput) (map[string][]byte, error) {
	if t == nil {
		return nil, nil
	}
	var values map[string][]byte
	for _, v := range []struct {
		key   string
		value *string
	}{{"tls/ca.crt", t.CaCertificate}, {"tls/ca.key", t.CaPrivateKey}, {"tls/user.crt", t.Certificate}, {"tls/user.key", t.PrivateKey}} {
		if v.value != nil {
			if values == nil {
				values = make(map[string][]byte, 4)
			}
			values[v.key] = []byte(*v.value)
		}
	}
	if t.CaCertificate != nil || t.CaPrivateKey != nil {
		if t.CaCertificate == nil || t.CaPrivateKey == nil {
			return nil, engine.ErrInvalid
		}
		if _, err := pki.LoadCA(values["tls/ca.crt"], values["tls/ca.key"]); err != nil {
			return nil, engine.ErrInvalid
		}
	}
	if t.Certificate != nil || t.PrivateKey != nil {
		if t.Certificate == nil || t.PrivateKey == nil {
			return nil, engine.ErrInvalid
		}
		if _, err := tls.X509KeyPair(values["tls/user.crt"], values["tls/user.key"]); err != nil {
			return nil, engine.ErrInvalid
		}
	}
	return values, nil
}
func (s *Server) enqueue(ctx context.Context, ref domain.ClusterRef, kind string, payload any) response {
	data, err := json.Marshal(payload)
	if err != nil {
		return s.failure(err)
	}
	actor := principal(ctx).Actor()
	op := domain.Operation{Namespace: ref.Namespace, Cluster: ref.Name, Kind: kind, Payload: data, Actor: actor}
	if err = s.Store.EnqueueOperation(ctx, &op); err != nil {
		return s.failure(err)
	}
	s.trigger(ctx, ref)
	return response{status: 202, body: operationAPI(op), operationID: op.ID}
}
func (s *Server) validateBackendCapabilities(ctx context.Context, p engine.Provider, c *domain.Cluster, previousExposure *domain.Exposure) *response {
	kind := "zerops"
	namespace := domain.NormalizedNamespace(c.Namespace)
	n, err := s.Store.GetNamespace(ctx, namespace)
	if err != nil && !(namespace == domain.DefaultNamespace && errors.Is(err, store.ErrNotFound)) {
		r := s.failure(err)
		return &r
	}
	if n != nil && n.Backend != "" {
		kind = n.Backend
	}
	factory, err := s.Backends.Get(kind)
	if err != nil {
		r := s.namespaceFailure(err)
		return &r
	}
	info := factory.Info()
	have := info.Capabilities
	if s.namespaces != nil {
		if backend, err := s.namespaces.Backend(namespace); err == nil && backend != nil {
			have = backend.Capabilities()
			unchangedExposure := previousExposure != nil &&
				previousExposure.Public == c.Spec.Exposure.Public &&
				previousExposure.IPType == c.Spec.Exposure.IPType &&
				slices.Equal(previousExposure.Allowlist, c.Spec.Exposure.Allowlist)
			if !unchangedExposure {
				if _, err := engine.CheckExposureSupport(ctx, backend, c.Spec.Exposure); err != nil {
					r := fail(400, "invalid_request", err.Error())
					return &r
				}
			}
		}
	}
	if err := engine.CheckCapabilities(p, c.Spec, info.Kind, have); err != nil {
		r := fail(400, "invalid_request", err.Error())
		return &r
	}
	return nil
}

func (s *Server) cluster(ctx context.Context, name string, in input) response {
	switch name {
	case "ListProviders":
		out := []api.Provider{}
		for _, d := range s.Registry.List() {
			p, err := s.Registry.Get(d.Name)
			if err != nil {
				return s.failure(err)
			}
			out = append(out, providerAPI(p))
		}
		return ok(page[api.Provider]{Items: out})
	case "GetProvider":
		p, err := s.Registry.Get(in.Provider)
		if err != nil {
			return fail(404, "not_found", "Provider not found")
		}
		return ok(providerAPI(p))
	case "ListClusters":
		items, err := s.Store.ListClusters(ctx, namespaceFilter(ctx))
		out := make([]api.Cluster, 0, len(items))
		for _, c := range items {
			if s.allowed(ctx, "clusters", "read", clusterObject(c.Namespace, c.Name)) {
				out = append(out, s.clusterAPI(c))
			}
		}
		return listResponse(s, ctx, out, err)
	case "CreateCluster":
		b := in.Body.(*api.CreateClusterRequest)
		namespace := domain.NormalizedNamespace(namespaceFilter(ctx))
		if err := domain.ValidateClusterName(b.Name); err != nil {
			return fail(400, "invalid_request", err.Error())
		}
		if err := domain.ValidateNamespaceName(namespace); err != nil {
			return fail(400, "invalid_request", err.Error())
		}
		if failure := s.usableNamespace(namespace); failure != nil {
			return *failure
		}
		if _, err := s.Store.GetCluster(ctx, clusterRef(ctx, b.Name)); err == nil {
			return fail(409, "conflict", "Cluster already exists")
		} else if !errors.Is(err, store.ErrNotFound) {
			return s.failure(err)
		}
		c := domain.Cluster{Namespace: namespace, Name: b.Name, Spec: specDomain(b.Spec)}
		if c.Spec.Provider == "" {
			return invalidField("spec.provider", domain.ReasonRequired, "spec.provider is required; list providers with GET /api/v1/providers")
		}
		if d := c.Spec.DataSource; d != nil {
			d.Namespace = namespaceOrDefaultSource(d.Namespace, namespace)
			source := domain.ClusterRef{Namespace: d.Namespace, Name: d.Cluster}
			if failure := s.restorePermission(ctx, namespace, source); failure != nil {
				return *failure
			}
			if d.Backup != "" {
				if failure := s.retainedRestoreSource(ctx, source, d.Backup); failure != nil {
					return *failure
				}
				backup, err := s.Store.GetBackup(ctx, source, d.Backup)
				if err != nil {
					return s.backupDependencyFailure(err, source, d.Backup)
				}
				if !backup.ProviderMatches(c.Spec.Provider) {
					return s.failure(engine.InvalidField("spec.dataSource.backup", domain.ReasonConflict, "backup provider does not match target provider"))
				}
			}
		}
		p, err := s.Registry.Get(c.Spec.Provider)
		if err != nil {
			return invalidField("spec.provider", domain.ReasonUnsupported, "spec.provider is not offered; list providers with GET /api/v1/providers")
		}
		p.ApplyDefaults(&c.Spec)
		preserveExplicitSpecValues(&c.Spec, b.Spec)
		if err = domain.ValidateExposure(c.Spec.Exposure); err != nil {
			return s.failure(err)
		}
		c.Spec.Exposure.AllowAllAddresses = c.Spec.Exposure.Public && domain.ExposureCoversFamily(c.Spec.Exposure)
		if err = p.Validate(ctx, nil, &c); err != nil {
			return s.failure(err)
		}
		if failure := s.validateBackendCapabilities(ctx, p, &c, nil); failure != nil {
			return *failure
		}
		if err = s.validateStorages(ctx, &c); err != nil {
			return s.failure(err)
		}
		values, err := tlsValues(b.Spec.Tls)
		if err != nil {
			return s.failure(err)
		}
		op := domain.Operation{Kind: domain.OperationCreate, Actor: principal(ctx).Actor()}
		setIdempotencyRecovery(ctx, func(op *domain.Operation) response {
			accepted := c
			accepted.OperationID, accepted.Generation = op.ID, op.TargetGeneration
			return response{status: 201, body: s.clusterAPI(accepted), operationID: op.ID}
		})
		ctx = store.WithOperation(ctx, &op)
		if c.Spec.Provider == "rabbitmq" && c.Spec.Version == "4.3.5" {
			c.Status.Conditions = append(c.Status.Conditions, domain.Condition{
				Type: "DeprecatedVersion", Status: "True", Reason: "StreamAuthorizationDisclosures",
				Message: rabbitMQ435Warning, LastTransitionTime: engine.RealClock{}.Now(),
			})
		}
		if err = s.Store.CreateClusterWithSecrets(ctx, &c, values); err != nil {
			return s.failure(err)
		}
		if c.Spec.Provider == "rabbitmq" && c.Spec.Version == "4.3.5" {
			if err = s.recordAudit(ctx, domain.Event{
				Namespace: namespace, Cluster: c.Name, Resource: "clusters", Target: c.Name,
				Actor: principal(ctx).Actor(), Type: "Warning", Reason: "DeprecatedVersion",
				Message: rabbitMQ435Warning,
			}); err != nil {
				return s.failure(err)
			}
		}
		if d := c.Spec.DataSource; d != nil && d.Namespace != namespace {
			if err = s.crossNamespaceRestoreEvents(ctx, domain.ClusterRef{Namespace: namespace, Name: c.Name}, domain.ClusterRef{Namespace: d.Namespace, Name: d.Cluster}, "clone", op.ID); err != nil {
				return s.failure(err)
			}
		}
		s.trigger(ctx, clusterRef(ctx, c.Name))
		c.OperationID = op.ID
		return response{status: 201, body: s.clusterAPI(c), operationID: op.ID}
	}
	if name == "ListBackups" || name == "GetBackup" || name == "DeleteBackup" {
		if _, err := s.Store.GetCluster(ctx, clusterRef(ctx, in.Cluster)); errors.Is(err, store.ErrNotFound) {
			return s.retainedBackup(ctx, name, in)
		} else if err != nil {
			return s.failure(err)
		}
	}
	rc, p, err := s.context(ctx, clusterRef(ctx, in.Cluster))
	if err != nil {
		return s.failure(err)
	}
	c := rc.Cluster
	ctx = store.WithClusterSnapshot(ctx, c)
	if kind := operationKind(name); kind != "" && !slices.Contains(p.Describe().Operations, kind) {
		return s.unsupportedClusterAction(name, p)
	}
	switch name {
	case "GetCluster":
		return ok(s.clusterAPI(*c))
	case "GetClusterStatus":
		status := s.projectedClusterStatus(*c)
		if projector, ok := p.(interface {
			ProjectCredentialsStatus(context.Context, *engine.Context, domain.ClusterStatus) domain.ClusterStatus
		}); ok {
			status = projector.ProjectCredentialsStatus(ctx, rc, status)
		}
		return ok(s.statusAPI(clusterRef(ctx, c.Name), status))
	case "ReplaceCluster", "PatchCluster":
		var spec api.ClusterInputSpec
		if name == "ReplaceCluster" {
			spec = in.Body.(*api.ReplaceClusterRequest).Spec
		} else {
			patch := map[string]any(*in.Body.(*api.ClusterMergePatch))
			if len(patch) != 1 {
				return invalid()
			}
			v, exists := patch["spec"]
			if !exists || v == nil {
				return invalid()
			}
			if _, yes := v.(map[string]any); !yes {
				return invalid()
			}
			base := c.Spec
			// Acknowledgement belongs to this request, not the merge-patch base.
			// Unchanged legacy routing is still accepted by ValidateExposureUpdate.
			base.Exposure.AllowAllAddresses = false
			data, err := json.Marshal(map[string]any{"spec": specPatchBase(base)})
			if err != nil {
				return s.failure(err)
			}
			var target any
			if err = json.Unmarshal(data, &target); err != nil {
				return s.failure(err)
			}
			merged := mergePatch(target, patch)
			data, err = json.Marshal(merged)
			if err != nil {
				return invalid()
			}
			var envelope api.ReplaceClusterRequest
			if err = json.Unmarshal(data, &envelope); err != nil {
				return invalid()
			}
			spec = envelope.Spec
		}
		next := *c
		next.Spec = specDomain(spec)
		next.Spec.RetryEpoch = c.Spec.RetryEpoch
		if next.Spec.Provider == "" {
			return invalidField("spec.provider", domain.ReasonRequired, "spec.provider is required; list providers with GET /api/v1/providers")
		}
		if next.Spec.Provider != c.Spec.Provider {
			return invalidField("spec.provider", domain.ReasonInvalid, "spec.provider is immutable")
		}
		if next.Spec.DataSource != nil {
			next.Spec.DataSource.Namespace = namespaceOrDefaultSource(next.Spec.DataSource.Namespace, c.Namespace)
		}
		p.ApplyDefaults(&next.Spec)
		preserveExplicitSpecValues(&next.Spec, spec)
		if next.Spec.Exposure.Public {
			if err = domain.ValidateExposureUpdate(&c.Spec.Exposure, next.Spec.Exposure); err != nil {
				return s.failure(err)
			}
		}
		next.Spec.Exposure.AllowAllAddresses = next.Spec.Exposure.Public && domain.ExposureCoversFamily(next.Spec.Exposure)
		if err = p.Validate(ctx, c, &next); err != nil {
			return s.failure(err)
		}
		if failure := s.validateBackendCapabilities(ctx, p, &next, &c.Spec.Exposure); failure != nil {
			return *failure
		}
		if next.Spec.Version != c.Spec.Version {
			if err = s.validateUpgradeBackup(ctx, c, next.Spec.Version, p); err != nil {
				return s.failure(err)
			}
		}
		if err = s.validateStorages(ctx, &next); err != nil {
			return s.failure(err)
		}
		values, err := tlsValues(spec.Tls)
		if err != nil {
			return s.failure(err)
		}
		// Existing clusters retain their active trust root. Leaf rotation does not
		// implement a CA trust-overlap rollout, so CA replacement must fail closed.
		for _, key := range []string{"tls/ca.crt", "tls/ca.key"} {
			if proposed, supplied := values[key]; supplied {
				active, readErr := s.Store.GetClusterSecret(ctx, clusterRef(ctx, c.Name), key)
				if readErr != nil && !errors.Is(readErr, store.ErrNotFound) {
					return s.failure(readErr)
				}
				if readErr != nil || !bytes.Equal(active, proposed) {
					return fail(409, "conflict", "Existing cluster CA changes require a coordinated CA rotation flow")
				}
			}
		}
		kind := domain.OperationUpdate
		if next.Spec.Version != c.Spec.Version {
			kind = domain.OperationUpgrade
		} else if !sameTLS(c.Spec.TLS, next.Spec.TLS) || len(values) != 0 {
			kind = domain.OperationTLS
		}
		op := domain.Operation{Kind: kind, Actor: principal(ctx).Actor()}
		setIdempotencyRecovery(ctx, func(op *domain.Operation) response {
			accepted := next
			accepted.OperationID, accepted.Generation = op.ID, op.TargetGeneration
			return response{status: 200, body: s.clusterAPI(accepted), operationID: op.ID}
		})
		ctx = store.WithOperation(ctx, &op)
		audit := &store.SpecUpdateAudit{Build: func(before, after domain.ClusterSpec, generation int64, secrets []store.SpecSecretChange) domain.Event {
			e := clusterSpecAuditEvent(c.Name, generation, before, after, secrets)
			e.Namespace, e.Actor, e.Operation = c.Namespace, principal(ctx).Actor(), op.ID
			return e
		}}
		ctx = store.WithSpecUpdateAudit(ctx, audit)
		if err = s.Store.UpdateClusterSpecWithSecrets(ctx, clusterRef(ctx, c.Name), next.Spec, values); err != nil {
			return s.failure(err)
		}
		updated, err := s.Store.GetCluster(ctx, clusterRef(ctx, c.Name))
		if err != nil {
			return s.failure(err)
		}
		s.trigger(ctx, clusterRef(ctx, c.Name))
		updated.OperationID = op.ID
		return response{status: 200, body: s.clusterAPI(*updated), operationID: op.ID, auditEvent: audit.Event}
	case "DeleteCluster":
		q := request(ctx).URL.Query()
		opts := domain.DeleteOptions{KeepVolumes: q.Get("keepVolumes") == "true", KeepBackups: q.Get("keepBackups") == "true"}
		op := domain.Operation{Kind: domain.OperationDelete, Actor: principal(ctx).Actor()}
		ctx = store.WithOperation(ctx, &op)
		if err = s.Store.MarkClusterDeleting(ctx, clusterRef(ctx, c.Name), opts); err != nil {
			return s.failure(err)
		}
		s.trigger(ctx, clusterRef(ctx, c.Name))
		return response{status: 202, body: operationAPI(op), operationID: op.ID}
	case "ResumeCluster":
		op := domain.Operation{Kind: domain.OperationResume, Actor: principal(ctx).Actor()}
		setIdempotencyRecovery(ctx, func(op *domain.Operation) response {
			accepted := *c
			accepted.OperationID, accepted.Generation = op.ID, op.TargetGeneration
			accepted.Status.Phase = domain.PhaseCreating
			accepted.Status.Message = "Creating: queued runtime resume from retained volumes"
			return response{status: 202, body: s.clusterAPI(accepted), operationID: op.ID}
		})
		ctx = store.WithOperation(ctx, &op)
		if err = s.Store.ResumeCluster(ctx, clusterRef(ctx, c.Name)); err != nil {
			return s.failure(err)
		}
		updated, err := s.Store.GetCluster(ctx, clusterRef(ctx, c.Name))
		if err != nil {
			return s.failure(err)
		}
		s.trigger(ctx, clusterRef(ctx, c.Name))
		updated.OperationID = op.ID
		return response{status: 202, body: s.clusterAPI(*updated), operationID: op.ID}
	case "RetryClusterDeployments":
		var exhausted []string
		for component, failure := range c.Status.DeploymentFailures {
			if failure.RetryEpoch == c.Spec.RetryEpoch {
				exhausted = append(exhausted, component)
			}
		}
		if len(exhausted) == 0 {
			return fail(409, "conflict", "No platform deployments are in DeploymentRetriesExhausted for the current retry epoch")
		}
		slices.Sort(exhausted)
		op := domain.Operation{Kind: domain.OperationUpdate, Actor: principal(ctx).Actor()}
		setIdempotencyRecovery(ctx, func(op *domain.Operation) response {
			accepted := *c
			accepted.Spec.RetryEpoch++
			accepted.OperationID, accepted.Generation = op.ID, op.TargetGeneration
			return response{status: 202, body: s.clusterAPI(accepted), operationID: op.ID}
		})
		ctx = store.WithOperation(ctx, &op)
		audit := &store.SpecUpdateAudit{Build: func(before, after domain.ClusterSpec, generation int64, _ []store.SpecSecretChange) domain.Event {
			return domain.Event{
				Namespace: c.Namespace, Cluster: c.Name, Resource: "clusters", Target: c.Name,
				Actor: principal(ctx).Actor(), Operation: op.ID, Type: "Normal", Reason: "DeploymentRetriesRearmed",
				Message: fmt.Sprintf("Deployment retry epoch %d -> %d; re-armed members/components: %s", before.RetryEpoch, after.RetryEpoch, strings.Join(exhausted, ", ")),
			}
		}}
		ctx = store.WithSpecUpdateAudit(ctx, audit)
		if err = s.Store.IncrementClusterRetryEpoch(ctx, c.Ref()); err != nil {
			return s.failure(err)
		}
		updated, err := s.Store.GetCluster(ctx, c.Ref())
		if err != nil {
			return s.failure(err)
		}
		s.trigger(ctx, c.Ref())
		updated.OperationID = op.ID
		return response{status: 202, body: s.clusterAPI(*updated), operationID: op.ID, auditEvent: audit.Event}
	case "RestartCluster":
		return s.enqueue(ctx, clusterRef(ctx, c.Name), "restart", map[string]string{})
	case "RotateClusterTLS":
		return s.enqueue(ctx, clusterRef(ctx, c.Name), "rotate-tls", map[string]string{})
	case "RaiseClusterFCV":
		b := in.Body.(*api.RaiseFCVRequest)
		if validator, yes := p.(engine.FCVValidator); yes {
			if err := validator.ValidateFCV(ctx, rc, b.Version); err != nil {
				return s.failure(err)
			}
		} else if b.Version == "" {
			return invalidField("version", domain.ReasonRequired, "version is required")
		}
		return s.enqueue(ctx, clusterRef(ctx, c.Name), "raise-fcv", map[string]string{"version": b.Version})
	case "ReplaceClusterMember":
		found := false
		for _, m := range c.Status.Members {
			if m.Name == in.Member {
				found = true
			}
		}
		if !found {
			return fail(404, "not_found", "Member not found")
		}
		return s.enqueue(ctx, clusterRef(ctx, c.Name), "replace-member", map[string]string{"member": in.Member})
	case "RotateSystemUser":
		return s.enqueue(ctx, clusterRef(ctx, c.Name), "rotate-system-user", map[string]string{"user": in.User})
	case "ListClusterMembers":
		c.Status = s.freshStatus(c.Status)
		out := make([]api.Member, 0, len(c.Status.Members))
		for _, m := range c.Status.Members {
			out = append(out, s.memberAPI(clusterRef(ctx, c.Name), m))
		}
		return ok(page[api.Member]{Items: out})
	case "GetClusterMember":
		c.Status = s.freshStatus(c.Status)
		for _, m := range c.Status.Members {
			if m.Name == in.Member {
				return ok(s.memberAPI(clusterRef(ctx, c.Name), m))
			}
		}
		return fail(404, "not_found", "Member not found")
	case "ListClusterOperations":
		items, err := s.Store.ListOperations(ctx, clusterRef(ctx, c.Name))
		out := make([]api.Operation, 0, len(items))
		for _, o := range items {
			if phase := request(ctx).URL.Query().Get("phase"); phase == "" || phase == string(o.Phase) {
				out = append(out, operationAPI(o))
			}
		}
		return listResponse(s, ctx, out, err)
	case "ListBackups":
		items, err := s.Store.ListBackups(ctx, clusterRef(ctx, c.Name))
		out := make([]api.Backup, 0, len(items))
		for _, b := range items {
			out = append(out, backupAPI(b))
		}
		return listResponse(s, ctx, out, err)
	case "GetBackup":
		b, err := s.Store.GetBackup(ctx, clusterRef(ctx, c.Name), in.Backup)
		if err != nil {
			return s.failure(err)
		}
		return ok(backupAPI(*b))
	case "CreateBackup":
		b := in.Body.(*api.CreateBackupRequest)
		if c.Deleting || (c.Status.Phase != domain.PhaseReady && c.Status.Phase != domain.PhaseDegraded) || c.Status.ObservedGeneration < c.Generation {
			state := string(c.Status.Phase)
			if c.Deleting {
				state += "; deletion is pending"
			}
			if c.Status.ObservedGeneration < c.Generation {
				state += fmt.Sprintf("; generation %d is pending (observed generation %d)", c.Generation, c.Status.ObservedGeneration)
			}
			return failDetails(409, "conflict", "Backup requires a formed, active cluster with no pending changes; cluster "+c.Name+" is "+state, map[string]any{"resource": "cluster", "name": c.Name, "reason": domain.ReasonInvalidState, "state": string(c.Status.Phase), "generation": c.Generation, "observedGeneration": c.Status.ObservedGeneration})
		}
		if b.Name == "" {
			return invalidField("name", domain.ReasonRequired, "name is required")
		}
		supported := p.Describe().BackupTypes
		accepted := false
		for _, kind := range supported {
			if string(b.Type) == kind {
				accepted = true
				break
			}
		}
		if !accepted {
			return invalidField("type", domain.ReasonUnsupported, "type is not supported by "+p.Describe().DisplayName+"; supported backup types: "+strings.Join(supported, ", "))
		}
		if _, err = s.Store.GetBackupStorage(ctx, c.Namespace, b.Storage); err != nil {
			return s.failure(err)
		}
		v := domain.Backup{Namespace: c.Namespace, Name: b.Name, Cluster: c.Name, Storage: b.Storage, Type: string(b.Type), Trigger: "manual", Phase: domain.PhasePending}
		if c.Spec.Provider != "psmdb" {
			v.Provider = c.Spec.Provider
		}
		op := domain.Operation{Namespace: c.Namespace, Cluster: c.Name, Kind: "backup", Actor: principal(ctx).Actor(), Payload: json.RawMessage(`{"backup":` + strconv.Quote(b.Name) + `}`)}
		setIdempotencyRecovery(ctx, func(op *domain.Operation) response {
			return response{status: 202, body: backupAPI(v), operationID: op.ID}
		})
		if err = s.Store.EnqueueBackup(ctx, &v, &op); err != nil {
			return s.failure(err)
		}
		s.trigger(ctx, clusterRef(ctx, c.Name))
		return response{status: 202, body: backupAPI(v), operationID: op.ID}
	case "DeleteBackup":
		b, err := s.Store.GetBackup(ctx, clusterRef(ctx, c.Name), in.Backup)
		if err != nil {
			return s.failure(err)
		}
		op := domain.Operation{Namespace: c.Namespace, Cluster: c.Name, Kind: "delete-backup", Actor: principal(ctx).Actor(), Payload: json.RawMessage(`{"backup":` + strconv.Quote(b.Name) + `}`)}
		if err = s.Store.EnqueueBackupDeletion(ctx, b, &op); err != nil {
			return s.failure(err)
		}
		s.trigger(ctx, clusterRef(ctx, c.Name))
		return response{status: 202, body: operationAPI(op), operationID: op.ID}
	case "ListRestores":
		items, err := s.Store.ListRestores(ctx, clusterRef(ctx, c.Name))
		out := make([]api.Restore, 0, len(items))
		for _, r := range items {
			out = append(out, restoreAPI(r))
		}
		return listResponse(s, ctx, out, err)
	case "GetRestore":
		r, err := s.Store.GetRestore(ctx, clusterRef(ctx, c.Name), in.Restore)
		if err != nil {
			return s.failure(err)
		}
		return ok(restoreAPI(*r))
	case "CreateRestore":
		b := in.Body.(*api.CreateRestoreRequest)
		source := value(b.Source.SourceCluster)
		if source == "" {
			source = c.Name
		}
		sourceRef := domain.ClusterRef{Namespace: namespaceOrDefaultSource(value(b.SourceNamespace), c.Namespace), Name: source}
		if failure := s.restorePermission(ctx, c.Namespace, sourceRef); failure != nil {
			return *failure
		}
		if b.Name == "" {
			return invalidField("name", domain.ReasonRequired, "name is required")
		}
		if (value(b.Source.Backup) == "") == (b.Source.PitrTime == nil) {
			return invalidField("source", domain.ReasonConflict, "set exactly one of source.backup or source.pitrTime")
		}
		if b.Source.PitrTime != nil && !slices.Contains(p.Describe().Capabilities, "pitr") {
			return s.failure(engine.Invalid("%s does not support PITR (point-in-time restore)", p.Describe().DisplayName))
		}
		if b.Source.Backup != nil {
			if failure := s.retainedRestoreSource(ctx, sourceRef, *b.Source.Backup); failure != nil {
				return *failure
			}
			backup, err := s.Store.GetBackup(ctx, sourceRef, *b.Source.Backup)
			if err != nil {
				return s.backupDependencyFailure(err, sourceRef, *b.Source.Backup)
			}
			if !backup.ProviderMatches(c.Spec.Provider) {
				return s.failure(engine.InvalidField("source.backup", domain.ReasonConflict, "backup provider does not match target provider"))
			}
			if p.Name() == "rabbitmq" && (backup.Type != "definitions" || backup.Phase != domain.PhaseSucceeded || backup.Location == "") {
				return s.failure(engine.Invalid("restore source is not a successful RabbitMQ definitions backup"))
			}
		}
		if b.Source.PitrTime != nil {
			sourceRC, sourceProvider, err := s.context(ctx, sourceRef)
			if err != nil {
				return s.failure(err)
			}
			if failure := s.providerContext(sourceRC); failure != nil {
				return *failure
			}
			if validator, yes := sourceProvider.(engine.PITRRestoreValidator); yes {
				windows, err := validator.ValidatePITRRestore(ctx, sourceRC, *b.Source.PitrTime)
				if err != nil {
					var field *domain.FieldError
					if errors.As(err, &field) {
						details := map[string]any{"field": field.Field, "reason": field.Reason}
						if len(windows) > 0 {
							earliest, latest := windows[0].Start, windows[0].End
							for _, w := range windows[1:] {
								if w.Start.Before(earliest) {
									earliest = w.Start
								}
								if w.End.After(latest) {
									latest = w.End
								}
							}
							details["earliest"], details["latest"] = earliest, latest
						}
						return failDetails(400, "invalid_request", field.Message, details)
					}
					return s.failure(err)
				}
			}
		}
		r := domain.Restore{Namespace: c.Namespace, SourceNamespace: sourceRef.Namespace, Name: b.Name, Cluster: c.Name, Source: domain.RestoreSource{Backup: value(b.Source.Backup), PITRTime: b.Source.PitrTime, SourceCluster: source}, Phase: domain.PhasePending}
		op := domain.Operation{Namespace: c.Namespace, Cluster: c.Name, Kind: "restore", Actor: principal(ctx).Actor(), Payload: json.RawMessage(`{"restore":` + strconv.Quote(r.Name) + `}`)}
		setIdempotencyRecovery(ctx, func(op *domain.Operation) response {
			return response{status: 202, body: restoreAPI(r), operationID: op.ID}
		})
		if err = s.Store.EnqueueRestore(ctx, &r, &op); err != nil {
			return s.failure(err)
		}
		if sourceRef.Namespace != c.Namespace {
			if err = s.crossNamespaceRestoreEvents(ctx, clusterRef(ctx, c.Name), sourceRef, "restore", op.ID); err != nil {
				return s.failure(err)
			}
		}
		s.trigger(ctx, clusterRef(ctx, c.Name))
		return response{status: 202, body: restoreAPI(r), operationID: op.ID}
	}
	if failure := s.providerContext(rc); failure != nil {
		return *failure
	}
	if strings.Contains(name, "RabbitMQ") {
		return s.rabbitmq(ctx, name, in, rc, p)
	}
	if strings.Contains(name, "Postgres") {
		return s.postgres(ctx, name, in, rc, p)
	}
	if strings.Contains(name, "Supabase") {
		return s.supabase(ctx, name, in, rc, p)
	}
	dp, yes := p.(engine.DataPlane)
	if !yes {
		return s.unsupportedClusterAction(name, p)
	}
	switch name {
	case "GetClusterConnection":
		v, err := dp.Connection(ctx, rc)
		if err != nil {
			return s.failure(err)
		}
		a := api.Connection{Connection: connectionAPI(v.Connection), CaCertificate: &v.CACertificate, Credentials: []api.ConnectionCredential{}}
		for _, cr := range v.Credentials {
			a.Credentials = append(a.Credentials, api.ConnectionCredential{Username: cr.Username, Database: cr.Database, Password: ptr(cr.Password), Uri: ptr(cr.URI)})
		}
		return response{status: 200, body: a, noStore: true}
	case "GetClusterBalancer":
		b, err := dp.Balancer(ctx, rc)
		if err != nil {
			return s.failure(err)
		}
		return ok(api.Balancer{Enabled: b.Enabled, Running: &b.Running, Mode: &b.Mode})
	case "ReplaceClusterBalancer":
		b := in.Body.(*api.Balancer)
		if err = dp.SetBalancer(ctx, rc, b.Enabled); err != nil {
			if errors.Is(err, engine.ErrNotSupported) {
				return s.unsupportedClusterAction(name, p)
			}
			return s.failure(err)
		}
		v, err := dp.Balancer(ctx, rc)
		if err != nil {
			return s.failure(err)
		}
		return ok(api.Balancer{Enabled: v.Enabled, Running: &v.Running, Mode: &v.Mode})
	case "GetPITRWindow":
		windows, err := dp.PITRWindow(ctx, rc)
		if err != nil {
			if errors.Is(err, engine.ErrNotSupported) {
				return invalidField("source.pitrTime", domain.ReasonUnsupported, p.Describe().DisplayName+" does not support PITR for cluster "+c.Name)
			}
			return s.failure(err)
		}
		if len(windows) == 0 {
			msg := "No recoverable window yet: PITR needs a successful backup in the current backup chain and continuously archived logs covering a later point; check the cluster's backup conditions"
			if !c.Spec.Backup.PITR.Enabled {
				msg = "PITR is not enabled for this cluster (spec.backup.pitr.enabled is false)"
			}
			return ok(api.PITRWindow{Available: false, Message: &msg})
		}
		latest := windows[0]
		for _, w := range windows[1:] {
			if w.End.After(latest.End) {
				latest = w
			}
		}
		return ok(api.PITRWindow{Available: true, Start: &latest.Start, End: &latest.End})
	case "GetClusterPlacement":
		v, err := dp.Placement(ctx, rc)
		if err != nil {
			return s.failure(err)
		}
		return ok(placementAPI(v))
	case "GetMemberLogs":
		q := request(ctx).URL.Query()
		lines := 100
		if v := q.Get("lines"); v != "" {
			lines, err = strconv.Atoi(v)
			if err != nil || lines < 1 || lines > 10000 {
				return invalid()
			}
		}
		proc := q.Get("process")
		processes := p.Describe().Processes
		if proc == "" {
			proc = "agent"
			if len(processes) > 0 {
				proc = processes[0]
			}
		}
		if proc != "agent" && !slices.Contains(processes, proc) {
			return invalid()
		}
		logs, err := dp.MemberLogs(ctx, rc, in.Member, proc, lines)
		if err != nil {
			return s.failure(err)
		}
		a := api.MemberLogs{Member: in.Member, Lines: []api.LogLine{}}
		for _, l := range logs {
			a.Lines = append(a.Lines, api.LogLine{Timestamp: l.Timestamp, Process: l.Process, Message: l.Message})
		}
		return response{status: 200, body: a, noStore: true}
	default:
		return s.database(ctx, name, in, rc, dp)
	}
}

// upgradeBackupMaxAge bounds the recovery point required by a catalog upgrade path.
const upgradeBackupMaxAge = 24 * time.Hour

func (s *Server) validateUpgradeBackup(ctx context.Context, c *domain.Cluster, version string, p engine.Provider) error {
	for _, path := range p.Describe().UpgradePaths {
		if path.From != c.Spec.Version || path.To != version || !path.RequiresBackup {
			continue
		}
		now := time.Now().UTC()
		cutoff := now.Add(-upgradeBackupMaxAge)
		if c.CreatedAt.After(cutoff) {
			cutoff = c.CreatedAt
		}
		// When the observed version matches the current spec, successful convergence
		// records bound when that version was reached. UpdatedAt and Ready condition
		// transitions also change for unrelated updates, so they are not version clocks.
		if c.Status.Version == c.Spec.Version {
			ops, err := s.Store.ListOperations(ctx, c.Ref(), domain.PhaseSucceeded)
			if err != nil {
				return err
			}
			for _, op := range ops {
				if (op.Kind == domain.OperationCreate || op.Kind == domain.OperationUpgrade) &&
					op.TargetGeneration <= c.Status.ObservedGeneration &&
					op.CompletedAt != nil && op.CompletedAt.After(cutoff) {
					cutoff = *op.CompletedAt
				}
			}
		}
		backups, err := s.Store.ListBackups(ctx, c.Ref())
		if err != nil {
			return err
		}
		for _, b := range backups {
			if b.Phase == domain.PhaseSucceeded && b.CompletedAt != nil &&
				!b.CompletedAt.Before(cutoff) && !b.CompletedAt.After(now) {
				return nil
			}
		}
		return domain.Conflict("cluster", c.Name, domain.ReasonInvalidState, engine.ErrConflict,
			"Upgrade %s → %s requires a Succeeded backup of cluster %s completed within the last 24 hours and after its current version was reached when known; create a backup via POST /api/v1/clusters/%s/backups in the cluster's namespace, wait for it to succeed, and retry",
			path.From, path.To, c.Name, c.Name)
	}
	return nil
}

func sameTLS(a, b domain.TLSConfig) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return bytes.Equal(left, right)
}

func operationKind(name string) string {
	switch name {
	case "RotateSupabaseJWTSecret":
		return "rotate-jwt-secret"
	case "RestartCluster":
		return domain.OperationRestart
	case "RotateClusterTLS":
		return domain.OperationRotateTLS
	case "RaiseClusterFCV":
		return domain.OperationRaiseFCV
	case "ReplaceClusterMember":
		return domain.OperationReplaceMember
	case "RotateSystemUser":
		return domain.OperationRotateSystemUser
	case "CreateBackup":
		return domain.OperationBackup
	case "DeleteBackup":
		return domain.OperationDeleteBackup
	case "CreateRestore":
		return domain.OperationRestore
	}
	return ""
}

func namespaceOrDefaultSource(source, target string) string {
	if source == "" {
		return domain.NormalizedNamespace(target)
	}
	return source
}

func (s *Server) restorePermission(ctx context.Context, targetNamespace string, source domain.ClusterRef) *response {
	if failure := s.requireNamespace(ctx, source.Namespace); failure != nil {
		return failure
	}
	if err := source.Validate(); err != nil || domain.ValidateClusterName(source.Name) != nil {
		r := invalid()
		return &r
	}
	object := clusterObject(source.Namespace, source.Name)
	if source.Namespace != domain.NormalizedNamespace(targetNamespace) && !s.allowed(ctx, "backups", "export", object) {
		r := fail(403, "forbidden", "Cross-namespace restore requires backups/export permission on "+object)
		return &r
	}
	if !s.allowed(ctx, "backups", "read", object) {
		r := fail(403, "forbidden", "Permission denied: backups/read on "+object)
		return &r
	}
	return nil
}

func (s *Server) validateStorages(ctx context.Context, c *domain.Cluster) error {
	p, err := s.Registry.Get(c.Spec.Provider)
	if err != nil {
		return err
	}
	for i, schedule := range c.Spec.Backup.Schedules {
		if !slices.Contains(p.Describe().BackupTypes, schedule.Type) {
			return engine.InvalidField(fmt.Sprintf("spec.backup.schedules[%d].type", i), domain.ReasonUnsupported, "backup schedule %q has unsupported type %q", schedule.Name, schedule.Type)
		}
	}
	if c.Spec.Backup.Storage != "" {
		if _, err := s.Store.GetBackupStorage(ctx, c.Namespace, c.Spec.Backup.Storage); err != nil {
			return err
		}
	}
	for _, schedule := range c.Spec.Backup.Schedules {
		storage := schedule.Storage
		if storage == "" {
			storage = c.Spec.Backup.Storage
		}
		if _, err := s.Store.GetBackupStorage(ctx, c.Namespace, storage); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) backupDependencyFailure(err error, source domain.ClusterRef, name string) response {
	if errors.Is(err, store.ErrNotFound) {
		return failDetails(404, "not_found", fmt.Sprintf("backup %q not found in source cluster %q, namespace %q", name, source.Name, source.Namespace),
			map[string]any{"resource": "backup", "name": name, "reason": domain.ReasonNotFound, "cluster": source.Name, "namespace": source.Namespace})
	}
	return s.failure(err)
}

func (s *Server) crossNamespaceRestoreEvents(ctx context.Context, target, source domain.ClusterRef, kind, operation string) error {
	actor := principal(ctx).Actor()
	message := "Cross-namespace " + kind + " from " + clusterObject(source.Namespace, source.Name) + " to " + clusterObject(target.Namespace, target.Name)
	for _, ref := range []domain.ClusterRef{target, source} {
		if err := s.recordAudit(ctx, domain.Event{Namespace: ref.Namespace, Cluster: ref.Name, Resource: "restores", Target: clusterObject(target.Namespace, target.Name), Type: "Audit", Reason: "CrossNamespaceRestore", Message: message, Actor: actor, Operation: operation}); err != nil {
			return err
		}
	}
	return nil
}
