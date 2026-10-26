// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package namespaces

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/relay"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// RelayRuntime contains public configuration only; enrollment uses env secrets.
func RelayRuntime(publicURL, namespace string) infra.RuntimeSpec {
	return infra.RuntimeSpec{
		Base: infra.BaseUbuntu2404, Args: []string{"relay"},
		Ports:           []infra.Port{{Port: 9443, Protocol: "tcp"}},
		Env:             map[string]string{"KARAKORAM_RELAY_URL": publicURL, "KARAKORAM_RELAY_NAMESPACE": namespace},
		Readiness:       &infra.Readiness{Args: []string{"relay", "readiness"}, FailureTimeout: 300 * time.Second, RetryPeriod: 10 * time.Second},
		StopBeforeStart: true,
	}
}

// Run reconciles projects independently. One unavailable project cannot stall another.
func (r *Registry) Run(ctx context.Context) error {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	var workers sync.WaitGroup
	defer workers.Wait()
	tick := func() {
		if err := r.renewHubCertificate(ctx, time.Now()); err != nil {
			r.o.Logger.Error("Relay hub certificate renewal failed", "error", err)
		}
		all, err := r.o.Store.ListNamespacesForUpdate(ctx)
		if err != nil {
			r.o.Logger.Error("Namespace listing failed", "error", err)
			return
		}
		for _, n := range all {
			name := n.Name
			m := r.lock(name)
			if !m.TryLock() {
				continue
			}
			workers.Add(1)
			go func() { defer workers.Done(); defer m.Unlock(); r.step(ctx, name) }()
		}
	}
	tick()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			tick()
		case <-r.wake:
			tick()
		case notice := <-r.notices:
			r.sessionEvent(ctx, notice)
		}
	}
}
func (r *Registry) sessionEvent(ctx context.Context, notice sessionNotice) {
	// State synchronization on the periodic loop repairs any dropped notice.
	if notice.event.Type == "refused" {
		n, err := r.Get(ctx, notice.ns)
		if err != nil || n.Phase == "Deleting" {
			return
		}
		if time.Since(r.refused[n.Name]) >= time.Minute {
			r.refused[n.Name] = time.Now()
			// Targets come from control-plane policy; relay text is limited to fixed reasons.
			r.event(ctx, n, "RelayTargetRefused", fmt.Sprintf("Relay could not open %s: %s", notice.event.Target, relay.RefusalReason(notice.event.Reason)))
		}
		return
	}
	m := r.lock(notice.ns)
	if !m.TryLock() {
		return
	}
	defer m.Unlock()
	n, err := r.o.Store.GetNamespaceForUpdate(ctx, notice.ns)
	if err != nil || n.Phase == "Deleting" {
		return
	}
	r.syncSession(ctx, n)
	_ = r.save(ctx, n)
}
func (r *Registry) syncSession(ctx context.Context, n *domain.Namespace) {
	r.syncSessionSnapshot(ctx, n, r.Hub.Status(n.Name))
}

func (r *Registry) syncSessionSnapshot(ctx context.Context, n *domain.Namespace, s relay.SessionStatus) {
	if n.Phase == "Deleting" {
		r.Hub.Revoke(n.Name)
		s = relay.SessionStatus{}
	}
	for s.CertificateSerial != "" && (!acceptsSerial(n, s.CertificateSerial) || !r.Hub.AcceptsSerial(n.Name, s.CertificateSerial)) {
		// Publication may have replaced this snapshot. Only revoke its serial,
		// then reconcile the returned status so current enrollment is persisted.
		s = r.Hub.RevokeIfSerial(n.Name, s.CertificateSerial)
	}
	was := n.Relay.Connected
	if s.CertificateSerial != "" && s.CertificateSerial == n.ClientSerial {
		n.PreviousClientSerial = ""
		n.PreviousCertNotAfter = time.Time{}
	}
	n.Relay.Connected = s.Connected
	n.Relay.ConnectedSince = s.ConnectedSince
	n.Relay.LastSeenAt = s.LastSeen
	n.Relay.Version = s.Hello.Version
	n.Relay.Reconnects = int64(s.Reconnects)
	if s.Connected {
		n.Relay.Phase = "Ready"
		n.Relay.LastError = ""
	} else if n.DeployedExecutableHash != "" && n.PendingOpID == "" {
		n.Relay.Phase = "Error"
		state := "has no authenticated session observed by this control plane"
		if was || !s.LastSeen.IsZero() || s.Hello.Version != "" {
			state = "authenticated session dropped"
		}
		n.Relay.LastError = fmt.Sprintf("Relay %s; dial target %s/relay/v1/connect; inspect relay runtime logs for DNS, TCP, outer HTTPS trust/hostname, WebSocket upgrade or inner mTLS failure (pre-connect failures are not visible to the control plane)", state, relay.DiagnosticEndpoint(r.o.PublicURL))
	}
	updatePhase(n)
	if was != s.Connected {
		if s.Connected {
			r.event(ctx, n, "RelayConnected", "Relay connected")
		} else {
			r.event(ctx, n, "RelayDisconnected", "Relay disconnected")
		}
	}
}

func (r *Registry) step(ctx context.Context, name string) {
	n, err := r.o.Store.GetNamespaceForUpdate(ctx, name)
	if err != nil {
		return
	}
	b := r.backend(name)
	if b == nil {
		return
	}
	if n.Phase != "Deleting" && time.Since(n.Token.LastValidatedAt) >= 5*time.Minute {
		token := r.o.DefaultTarget.Token
		if !n.Default {
			token, err = r.o.Store.GetNamespaceTokenForUpdate(ctx, name)
		}
		if err != nil && !errors.Is(err, store.ErrNotFound) && !errors.Is(err, store.ErrSecretUnreadable) {
			// No credential was inspected. Preserve health and the validation
			// deadline so the next reconciliation tick retries the store read.
			r.o.Logger.Warn("Namespace credential read failed; retrying validation next tick", "namespace", name)
		} else {
			var validation error
			var project infra.Project
			if err != nil {
				validation = &credentialError{message: "Stored project credentials are unavailable"}
			} else {
				_, project, validation = r.o.Backends.Validate(ctx, r.target(n, token))
			}
			was := n.Token.Valid
			applyTokenValidation(n, project, validation, time.Now().UTC(), r.displayName(n.Backend))
			if was != n.Token.Valid {
				if n.Token.Valid {
					r.event(ctx, n, "NamespaceValidated", "Project token validated")
				} else {
					r.event(ctx, n, "NamespaceValidationFailed", "Project token validation failed")
				}
			}
		}
	}
	if n.Default || !r.requiresRelay(n.Backend) {
		if n.Phase == "Deleting" {
			if err := r.o.Store.DeleteNamespace(ctx, n); err == nil {
				r.mu.Lock()
				delete(r.backends, name)
				r.mu.Unlock()
				r.event(ctx, n, "NamespaceDeleted", "Namespace deleted")
			}
			return
		}
		n.Relay.Phase = "NotRequired"
		updatePhase(n)
		_ = r.save(ctx, n)
		return
	}
	clusters, err := r.o.Store.ListClusters(ctx, name)
	var damaged *store.ClusterDataError
	if errors.As(err, &damaged) {
		r.o.Logger.Error("isolating unreadable clusters from relay policy", "namespace", name, "error", err)
		// Authorize only independently decoded rows, never the damaged row.
		err = nil
	}
	targets := []string{}
	if err == nil && r.o.RelayTargets != nil {
		for i := range clusters {
			targets = append(targets, r.o.RelayTargets(&clusters[i])...)
		}
	}
	if n.Phase == "Deleting" {
		r.Hub.Revoke(name)
	} else {
		r.Hub.SetPolicy(name, targets)
	}
	r.syncSession(ctx, n)
	if !n.RetryAt.After(time.Now()) && (n.Token.Valid || n.Phase == "Deleting") {
		err = r.reconcile(ctx, n, b)
		if errors.Is(err, store.ErrStale) {
			r.o.Logger.Debug("Dropping stale namespace reconciliation", "namespace", name)
			return
		}
		if err != nil {
			n.Failures++
			n.RetryAt = time.Now().Add(time.Second * time.Duration(1<<min(n.Failures, 8)))
			n.Relay.Phase = "Error"
			n.Relay.LastError = err.Error()
		} else {
			n.Failures = 0
			if !n.RetryAt.After(time.Now()) {
				n.RetryAt = time.Time{}
			}
		}
	}
	// Successful deletion already removed the record.
	if _, e := r.o.Store.GetNamespaceForUpdate(ctx, name); errors.Is(e, store.ErrNotFound) {
		return
	}
	updatePhase(n)
	if err = r.save(ctx, n); err != nil && !errors.Is(err, store.ErrStale) && ctx.Err() == nil {
		r.o.Logger.Error("Namespace state persistence failed", "namespace", name, "error", err)
	}
}
func (r *Registry) pending(ctx context.Context, n *domain.Namespace, b Backend, op infra.Op) error {
	n.PendingOpID = op.ID
	n.PendingOpKind = op.Kind
	if op.ID != "" {
		n.PendingOpStartedAt = time.Now().UTC()
	} else {
		n.PendingOpStartedAt = time.Time{}
	}
	if n.Phase != "Deleting" {
		if n.Relay.Phase != "Deploying" {
			r.event(ctx, n, "RelayDeploying", "Relay reconciliation operation started")
		}
		n.Relay.Phase = "Deploying"
	}
	if op.ID == "" && op.Kind == "deploy" {
		n.DeployedExecutableHash = n.PendingExecutableHash
		n.PendingExecutableHash = ""
	}
	return r.save(ctx, n)
}
func (r *Registry) deleteRelay(ctx context.Context, n *domain.Namespace, b Backend, id string) error {
	op, err := b.DeleteServiceByID(ctx, n.Relay.Hostname, id)
	if err != nil {
		// Backend errors may contain credentials. Keep diagnostics actionable
		// without rendering their raw text or discarding the previous receipt.
		return errors.New("backend refused fenced deletion of the owned relay; the pending operation is retained; check service identity, unresolved import and active lifecycle leases in the namespace backend; Karakoram will retry automatically")
	}
	n.Relay.ServiceID = id
	n.PendingExecutableHash = ""
	n.Relay.LastError = ""
	op.Kind = "delete"
	return r.pending(ctx, n, b, op)
}
func (r *Registry) reconcile(ctx context.Context, n *domain.Namespace, b Backend) error {
	if n.PendingOpID != "" {
		if n.Phase == "Deleting" && engine.CleanupSupersedesDeployment(b, infra.Op{Kind: n.PendingOpKind}) {
			// Keep the old receipt until fenced deletion is accepted. Repeating
			// this after a lost save is safe under DeploymentSuperseder's contract.
			service, err := b.GetService(ctx, n.Relay.Hostname)
			if err != nil {
				return errors.New("cannot inspect relay service before fenced deletion; the pending operation is retained")
			}
			id := n.Relay.ServiceID
			if service != nil {
				if !owned(n, service) || id != "" && id != service.ID {
					return errors.New("cannot supersede relay operation: service identity or ownership changed; the pending operation is retained")
				}
				id = service.ID
			}
			if id == "" {
				return errors.New("cannot supersede relay operation without the owned service ID; the pending operation is retained")
			}
			return r.deleteRelay(ctx, n, b, id)
		}
		if n.PendingOpStartedAt.IsZero() {
			n.PendingOpStartedAt = time.Now().UTC()
		}
		state, err := b.OpStatus(ctx, infra.Op{ID: n.PendingOpID, Kind: n.PendingOpKind, ServiceID: n.Relay.ServiceID})
		if err != nil {
			return fmt.Errorf("cannot inspect relay %s operation %q; check the operation in the namespace backend; automatic mutations remain fenced until its outcome is known", n.PendingOpKind, n.PendingOpID)
		}
		if !state.Done {
			n.Relay.LastError = fmt.Sprintf("Waiting for relay %s operation %q in the namespace backend; automatic mutations remain fenced until its outcome is known", n.PendingOpKind, n.PendingOpID)
			if time.Since(n.PendingOpStartedAt) >= 15*time.Minute {
				if n.Phase == "Deleting" {
					return fmt.Errorf("namespace deletion is waiting for relay %s operation %q for at least 15 minutes; this backend requires its terminal outcome before cleanup; inspect the operation and resolve backend deployment or import blockers; Karakoram will retry automatically; do not remove control-plane state", n.PendingOpKind, n.PendingOpID)
				}
				return fmt.Errorf("relay %s operation %q has not finished for at least 15 minutes; inspect or cancel it in the namespace backend, then allow Karakoram to observe its terminal outcome; do not remove control-plane state", n.PendingOpKind, n.PendingOpID)
			}
			return nil
		}
		kind := n.PendingOpKind
		n.PendingOpID = ""
		n.PendingOpKind = ""
		n.PendingOpStartedAt = time.Time{}
		if state.Failed {
			if kind == "restart" {
				n.SecretsPending = true
			}
			n.PendingExecutableHash = ""
			if err = r.save(ctx, n); err != nil {
				return err
			}
			return errors.New("Relay operation failed")
		}
		if kind == "deploy" {
			n.DeployedExecutableHash = n.PendingExecutableHash
			n.PendingExecutableHash = ""
		}
		if err = r.save(ctx, n); err != nil {
			return err
		}
		return nil
	}
	service, err := b.GetService(ctx, n.Relay.Hostname)
	if err != nil {
		return errors.New("Cannot inspect relay service")
	}
	if service != nil && !owned(n, service) {
		if service.Masked && service.Ownership["KARAKORAM_RELAY_OWNER"] == "" && n.Relay.ServiceID == "" {
			return errors.New("relay ownership cannot be verified: the relay was created by an earlier version and the namespace backend masks sensitive variables for the current credentials; delete the relay service in the namespace backend so Karakoram recreates it, or run one reconciliation with credentials that can read sensitive variables (a sudo-mode token on Zerops)")
		}
		return errors.New("Relay hostname belongs to a service this namespace does not own")
	}
	if service != nil {
		n.Relay.ServiceID = service.ID
		// A relay about to be deleted needs no marker.
		if n.Phase != "Deleting" && !service.Importing && (service.Ownership["KARAKORAM_RELAY_OWNER"] != n.OwnershipMarker || n.SecretsPending) {
			if err = b.SetEnvVars(ctx, n.Relay.Hostname, map[string]string{"KARAKORAM_RELAY_OWNER": n.OwnershipMarker}); err != nil {
				return errors.New("Cannot update relay ownership marker")
			}
		}
	}
	if n.Phase == "Deleting" {
		if service != nil {
			return r.deleteRelay(ctx, n, b, service.ID)
		}
		if err = r.o.Store.DeleteNamespace(ctx, n); err != nil {
			return err
		}
		r.Hub.Revoke(n.Name)
		r.mu.Lock()
		delete(r.backends, n.Name)
		r.mu.Unlock()
		r.event(ctx, n, "NamespaceDeleted", "Namespace deleted")
		return nil
	}
	leaf, err := relay.ParseCertificatePEM(n.ClientCert)
	if err != nil {
		return errors.New("cannot parse relay certificate; restore the persisted namespace identity")
	}
	if (service == nil || !service.Importing) && relay.NeedsRenewal(leaf, time.Now()) {
		key, err := r.issue(n)
		if err != nil {
			return errors.New("Cannot renew relay certificate")
		}
		token, err := r.o.Store.GetNamespaceTokenForUpdate(ctx, n.Name)
		if err != nil {
			return err
		}
		if err = r.o.Store.UpdateNamespaceCredentials(ctx, n, token, key); err != nil {
			return err
		}
		r.enroll(n)
		r.event(ctx, n, "RelayCertificateRenewed", "Relay certificate renewed")
		return nil
	}
	key, err := r.o.Store.GetNamespaceRelayKeyForUpdate(ctx, n.Name)
	if err != nil {
		return err
	}
	secrets := map[string]string{"KARAKORAM_RELAY_CERT": string(n.ClientCert), "KARAKORAM_RELAY_KEY": string(key), "KARAKORAM_RELAY_CA": string(r.pki.CACert)}
	if service == nil || service.Importing {
		n.DeployedExecutableHash = ""
		n.SecretsPending = false
		op, err := b.ImportServices(ctx, []infra.ServiceSpec{{Hostname: n.Relay.Hostname, Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404, MinContainers: 1, MaxContainers: 1, Resources: domain.Resources{MinCPU: 1, MaxCPU: 1, MinRAM: 1, MaxRAM: 1, MinDisk: 1, MaxDisk: 3}, Env: secrets, Vars: map[string]string{"KARAKORAM_RELAY_OWNER": n.OwnershipMarker}}})
		if err != nil {
			return errors.New("Cannot import relay service")
		}
		op.Kind = "import"
		n.Relay.ServiceID = op.Services[n.Relay.Hostname]
		return r.pending(ctx, n, b, op)
	}
	if n.SecretsPending {
		if err = b.SetEnvSecrets(ctx, n.Relay.Hostname, secrets); err != nil {
			return errors.New("Cannot update relay enrollment secrets")
		}
		op, err := b.Restart(ctx, n.Relay.Hostname)
		if err != nil {
			return errors.New("Cannot restart relay after certificate renewal")
		}
		n.SecretsPending = false
		op.Kind = "restart"
		return r.pending(ctx, n, b, op)
	}
	status := r.Hub.Status(n.Name)
	mismatch := status.Connected && status.Hello.ExecutableSHA256 != "" && status.Hello.ExecutableSHA256 != r.o.ExecutableHash
	if n.DeployedExecutableHash != r.o.ExecutableHash || mismatch || service.Status == infra.StatusReadyToDeploy {
		artifact, err := r.o.Artifact.Open(ctx)
		if err != nil {
			return errors.New("Cannot open relay executable artifact")
		}
		defer artifact.Close()
		op, err := b.Deploy(ctx, n.Relay.Hostname, infra.Deployment{Artifact: infra.Artifact{Reader: artifact, Name: engine.ArtifactName, Binary: engine.ArtifactBinary}, Runtime: RelayRuntime(r.o.PublicURL, n.Name)})
		if err != nil {
			return errors.New("Cannot deploy relay executable")
		}
		if service.Status == infra.StatusReadyToDeploy && n.DeployedExecutableHash == r.o.ExecutableHash {
			n.RetryAt = time.Now().Add(time.Minute)
		}
		n.PendingExecutableHash = r.o.ExecutableHash
		op.Kind = "deploy"
		return r.pending(ctx, n, b, op)
	}
	if !status.Connected && (service.Status == infra.StatusStopped || service.Status == infra.StatusFailed) {
		op, err := b.Restart(ctx, n.Relay.Hostname)
		if err != nil {
			return errors.New("cannot restart stopped relay runtime")
		}
		// Persist a cooldown even for synchronous restarts; stale inventory must
		// not produce a restart on every reconciliation or after a CP restart.
		n.RetryAt = time.Now().Add(time.Minute)
		op.Kind = "restart"
		return r.pending(ctx, n, b, op)
	}
	return nil
}

func (r *Registry) ClusterCount(ctx context.Context, namespace string) (int, error) {
	clusters, err := r.o.Store.ListClusters(ctx, namespace)
	return len(clusters), err
}
