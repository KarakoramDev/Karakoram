// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package namespaces

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/relay"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"gopkg.in/yaml.v3"
)

// RelayYAML contains public configuration only; enrollment credentials use env secrets.
func RelayYAML(hostname, publicURL, namespace string) (string, error) {
	run := map[string]any{"base": "ubuntu@24.04", "ports": []map[string]any{{"port": 9443, "protocol": "tcp"}}, "envVariables": map[string]string{"KARAKORAM_RELAY_URL": publicURL, "KARAKORAM_RELAY_NAMESPACE": namespace}, "start": "./karakoram relay"}
	doc := map[string]any{"zerops": []map[string]any{{"setup": hostname, "deploy": map[string]any{"temporaryShutdown": true, "readinessCheck": map[string]any{"exec": map[string]string{"command": "/var/www/karakoram relay readiness"}, "failureTimeout": "300s", "retryPeriod": "10s"}}, "run": run}}}
	data, err := yaml.Marshal(doc)
	return string(data), err
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
			r.event(ctx, n, "RelayTargetRefused", "Relay refused a target outside its policy")
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
		n.Relay.LastError = "Relay is disconnected"
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
		z := r.o.Zerops
		z.ProjectID = n.ProjectID
		if !n.Default {
			z.Token, err = r.o.Store.GetNamespaceTokenForUpdate(ctx, name)
		}
		var validation error
		var project infra.Project
		if err != nil {
			validation = errors.New("Project token is unavailable")
		} else {
			_, project, validation = r.o.Validate(ctx, z)
		}
		was := n.Token.Valid
		n.Token.Valid = validation == nil
		n.Token.LastValidatedAt = time.Now().UTC()
		n.Token.LastError = ""
		if validation != nil {
			n.Token.LastError = validation.Error()
		} else {
			n.ProjectName = project.Name
		}
		if was != n.Token.Valid {
			if n.Token.Valid {
				r.event(ctx, n, "NamespaceValidated", "Project token validated")
			} else {
				r.event(ctx, n, "NamespaceValidationFailed", "Project token validation failed")
			}
		}
	}
	if n.Default {
		updatePhase(n)
		_ = r.save(ctx, n)
		return
	}
	clusters, err := r.o.Store.ListClusters(ctx, name)
	targets := []string{}
	if err == nil {
		for i := range clusters {
			targets = append(targets, psmdb.RelayTargets(&clusters[i])...)
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
			n.RetryAt = time.Time{}
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
func (r *Registry) reconcile(ctx context.Context, n *domain.Namespace, b Backend) error {
	if n.PendingOpID != "" {
		state, err := b.OpStatus(ctx, infra.Op{ID: n.PendingOpID, Kind: n.PendingOpKind, ServiceID: n.Relay.ServiceID})
		if err != nil {
			return errors.New("Cannot inspect relay operation")
		}
		if !state.Done {
			return nil
		}
		kind := n.PendingOpKind
		n.PendingOpID = ""
		n.PendingOpKind = ""
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
		return errors.New("Relay hostname belongs to a service this namespace does not own")
	}
	if n.Phase == "Deleting" {
		if service != nil {
			deleter, ok := b.(interface {
				DeleteServiceByID(context.Context, string, string) (infra.Op, error)
			})
			if !ok {
				return errors.New("Relay backend cannot safely delete by service identity")
			}
			op, err := deleter.DeleteServiceByID(ctx, n.Relay.Hostname, service.ID)
			if err != nil {
				return errors.New("Cannot delete owned relay service")
			}
			op.Kind = "delete"
			return r.pending(ctx, n, b, op)
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
	if time.Until(n.Relay.CertNotAfter) < 30*24*time.Hour {
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
	secrets := map[string]string{"KARAKORAM_RELAY_CERT": string(n.ClientCert), "KARAKORAM_RELAY_KEY": string(key), "KARAKORAM_RELAY_CA": string(r.pki.CACert), "KARAKORAM_RELAY_OWNER": n.OwnershipMarker}
	if service == nil {
		n.DeployedExecutableHash = ""
		n.SecretsPending = false
		op, err := b.ImportServices(ctx, []infra.ServiceSpec{{Hostname: n.Relay.Hostname, Type: "ubuntu@24.04", MinContainers: 1, MaxContainers: 1, Resources: domain.Resources{MinCPU: 1, MaxCPU: 1, MinRAM: 1, MaxRAM: 1, MinDisk: 1, MaxDisk: 3}, Env: secrets}})
		if err != nil {
			return errors.New("Cannot import relay service")
		}
		op.Kind = "import"
		n.Relay.ServiceID = op.ServiceID
		return r.pending(ctx, n, b, op)
	}
	n.Relay.ServiceID = service.ID
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
	if n.DeployedExecutableHash != r.o.ExecutableHash || mismatch {
		config, err := RelayYAML(n.Relay.Hostname, r.o.PublicURL, n.Name)
		if err != nil {
			return err
		}
		artifact, err := r.o.Artifact.Open(ctx)
		if err != nil {
			return errors.New("Cannot open relay executable artifact")
		}
		defer artifact.Close()
		op, err := b.Deploy(ctx, n.Relay.Hostname, infra.Deployment{Artifact: artifact, ZeropsYAML: config, SetupName: n.Relay.Hostname, Filename: "karakoram.tar.gz"})
		if err != nil {
			return errors.New("Cannot deploy relay executable")
		}
		n.PendingExecutableHash = r.o.ExecutableHash
		op.Kind = "deploy"
		return r.pending(ctx, n, b, op)
	}
	return nil
}

func (r *Registry) ClusterCount(ctx context.Context, namespace string) (int, error) {
	clusters, err := r.o.Store.ListClusters(ctx, namespace)
	return len(clusters), err
}
