// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Provider defines idempotent engine reconciliation and resumable operations.
type Provider interface {
	Name() string
	Describe() domain.ProviderInfo
	ApplyDefaults(*domain.ClusterSpec)
	Validate(context.Context, *domain.Cluster, *domain.Cluster) error
	Sync(context.Context, *Context) (Result, error)
	// Status may return a useful partial snapshot with a secondary error when
	// Phase is nonempty. Reconciliation publishes its safety observations but
	// does not declare readiness or advance the applied generation on that error.
	Status(context.Context, *Context) (domain.ClusterStatus, error)
	RunOperation(context.Context, *Context, *domain.Operation) (OpResult, error)
	Cleanup(context.Context, *Context, domain.DeleteOptions) (bool, error)
}

// RelayTargeter lists host:port targets a namespace relay may dial for a cluster.
type RelayTargeter interface {
	RelayTargets(c *domain.Cluster) []string
}

// MetricsCatalog maps agent exposition metric names to monitoring series.
type MetricsCatalog interface {
	Metrics() []domain.MetricDescriptor
}

// RetainedBackupDeleter deletes backup artifacts after their cluster is deleted.
type RetainedBackupDeleter interface {
	DeleteRetainedBackup(ctx context.Context, storage domain.BackupStorage, b domain.Backup) error
}

// RetentionPolicy optionally protects recovery chains from scheduled expiration.
// FilterRetention may only remove candidates; backups contains the full inventory.
// Explicit user deletions are not subject to schedule retention policy.
type RetentionPolicy interface {
	FilterRetention(backups, candidates []domain.Backup) []domain.Backup
}

// OperationClaimPolicy optionally reserves ordinary convergence's maintenance
// ownership before a pending operation is claimed. False leaves queued work
// pending without consuming attempts or its execution retry budget. Opt-in
// providers' imperative deadlines start at StartedAt, not time spent queued.
// Running operations still resume before Sync.
type OperationClaimPolicy interface {
	CanClaimOperation(*domain.Cluster) (bool, error)
}

// OperationObservationInvalidator discards transient observation windows before
// an attempt, including failures resolving dependencies before provider entry.
// Only a successful fresh provider observation may renew the prior checkpoint.
type OperationObservationInvalidator interface {
	InvalidateOperationObservation(*domain.Operation) error
}

// OperationSyncPolicy optionally permits ordinary Sync after a nonterminal,
// successful RunOperation call. Without this policy, Running operations exclude
// Sync and Status, which may be unsafe while restore stops database processes.
// The decision uses the newly returned checkpoint. PSMDB must opt in only at
// replacement provisioning/join checkpoints so Sync can create the new member;
// restore, restart and credential-recovery checkpoints must remain isolated.
type OperationSyncPolicy interface {
	SyncDuringOperation(op *domain.Operation) bool
}

// OperationCanceller is optional: only the serialized worker executes cancellation.
// CanCancelOperation must be side-effect free and explain unsafe checkpoints.
// CancelOperation cooperatively restores safe invariants before returning Done.
type OperationCanceller interface {
	CanCancelOperation(context.Context, *domain.Operation) error
	CancelOperation(context.Context, *Context, *domain.Operation) (OpResult, error)
}

// Result requests a delay before the next level-triggered reconciliation.
type Result struct {
	RequeueAfter time.Duration
	// ConvergenceError fails tracking operations for the current generation
	// without blocking independent queued operations or retrying reconciliation
	// as an error. Providers retain diagnostics in status and own warning deduplication.
	ConvergenceError error
}

// OpResult reports operation completion and the next durable checkpoint.
type OpResult struct {
	Done bool
	// Recovered confirms a successful native outcome during failure cleanup.
	// Only a completed backup may override its exhausted initiation retries.
	Recovered    bool
	RequeueAfter time.Duration
	Step         string
	Message      string
	Checkpoint   json.RawMessage
}

// SecretStore provides encrypted cluster and agent credential persistence.
type SecretStore interface {
	GetClusterSecret(context.Context, domain.ClusterRef, string) ([]byte, error)
	SetClusterSecret(context.Context, domain.ClusterRef, string, []byte) error
	DeleteClusterSecret(context.Context, domain.ClusterRef, string) error
	GetAgentToken(context.Context, domain.ClusterRef, string) (*domain.AgentToken, error)
	SetAgentToken(context.Context, *domain.AgentToken) error
	DeleteAgentToken(context.Context, domain.ClusterRef, string) error
}

// ClusterStore exposes the inventory and cluster reads needed by a provider.
type ClusterStore interface {
	GetCluster(context.Context, domain.ClusterRef) (*domain.Cluster, error)
	ListClusters(context.Context, string) ([]domain.Cluster, error)
	GetBackupStorage(context.Context, string, string) (*domain.BackupStorage, error)
	GetBackup(context.Context, domain.ClusterRef, string) (*domain.Backup, error)
	ListBackups(context.Context, domain.ClusterRef) ([]domain.Backup, error)
	UpdateBackup(context.Context, *domain.Backup) error
	DeleteBackup(context.Context, domain.ClusterRef, string) error
	CreateBackup(context.Context, *domain.Backup) error
	CreateRestore(context.Context, *domain.Restore) error
	// EnqueueOperation durably queues an operation for the cluster's worker.
	EnqueueOperation(context.Context, *domain.Operation) error
	// EnqueueBackup atomically creates a Pending backup record and its backup operation.
	EnqueueBackup(context.Context, *domain.Backup, *domain.Operation) error
	// EnqueueRestore atomically creates a Pending restore record and its restore operation.
	EnqueueRestore(context.Context, *domain.Restore, *domain.Operation) error
	// ClaimInitialDataSource atomically closes provisioning-amendment admission
	// before a provider executes its first native initial import.
	ClaimInitialDataSource(context.Context, domain.ClusterRef) error
	GetOperation(context.Context, string) (*domain.Operation, error)
	GetRestore(context.Context, domain.ClusterRef, string) (*domain.Restore, error)
	UpdateRestore(context.Context, *domain.Restore) error
	ListRetainedVolumes(context.Context, domain.ClusterRef) ([]domain.RetainedVolume, error)
	CreateRetainedVolume(context.Context, *domain.RetainedVolume) error
	DeleteRetainedVolume(context.Context, domain.ClusterRef, string) error
	PutOrphanedVolume(context.Context, *domain.OrphanedVolume) error
	GetOrphanedVolume(context.Context, string, string) (*domain.OrphanedVolume, error)
	ListOrphanedVolumes(context.Context, string) ([]domain.OrphanedVolume, error)
	DeleteOrphanedVolume(context.Context, string, string) error
}

// Recorder persists an event before publishing it to live subscribers.
type Recorder interface {
	Record(context.Context, domain.Event) error
}

// AgentDialer constructs authenticated, CA-pinned member clients.
type AgentDialer interface {
	Dial(context.Context, domain.ClusterRef, string) (*agentapi.Client, error)
}

// ArtifactSource opens a stream of the deployable agent tar.gz artifact.
type ArtifactSource interface {
	Open(context.Context) (io.ReadCloser, error)
}

// Clock supplies timestamps and supports deterministic tests.
type Clock interface{ Now() time.Time }

// RealClock supplies wall-clock UTC timestamps.
type RealClock struct{}

// Now returns the current UTC wall-clock timestamp.
func (RealClock) Now() time.Time { return time.Now().UTC() }

// ContextDialer routes connections through a namespace's network.
type ContextDialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
}

// Namespaces resolves project-scoped infrastructure and transport dependencies.
type Namespaces interface {
	Backend(namespace string) (infra.Backend, error)
	Dialer(namespace string) ContextDialer
	AgentURL(namespace string) string
}

// StaticNamespaces preserves single-project installations.
type StaticNamespaces struct {
	DefaultBackend  infra.Backend
	DefaultAgentURL string
}

func (s StaticNamespaces) Backend(namespace string) (infra.Backend, error) {
	if namespace != "" && namespace != domain.DefaultNamespace {
		return nil, fmt.Errorf("namespace %q does not exist", namespace)
	}
	return s.DefaultBackend, nil
}

func (s StaticNamespaces) Dialer(string) ContextDialer { return nil }
func (s StaticNamespaces) AgentURL(namespace string) string {
	if namespace == "" || namespace == domain.DefaultNamespace {
		return s.DefaultAgentURL
	}
	return ""
}

// Context contains the dependencies and mutable state for one provider invocation.
type Context struct {
	Namespace string
	Dialer    ContextDialer
	AgentURL  string
	Cluster   *domain.Cluster
	// Operation is set only while an imperative operation owns this invocation.
	Operation *domain.Operation
	Backend   infra.Backend
	Secrets   SecretStore
	Store     ClusterStore
	Events    Recorder
	Agents    AgentDialer
	Artifact  ArtifactSource
	Logger    *slog.Logger
	Clock     Clock
	// ObservationEpoch is unique per CP process and shared with the API.
	// Availability embeddings must supply the same nonempty value to both;
	// timestamps alone cannot distinguish old snapshots after clock steps.
	ObservationEpoch string
	// AvailabilityOnly excludes inventory adoption/reconciliation from Status.
	// An imperative operation may temporarily own a different native backup
	// destination; probing availability must not reinterpret that inventory.
	AvailabilityOnly bool
	// DeploymentWait retains non-terminal platform backpressure for status.
	DeploymentWait       string
	DeploymentWaitReason string
	DeploymentWaitKey    string
	// SyncWait describes the current generation barrier, independently of health.
	// Providers replace it on every attempt; it is not an availability condition.
	SyncWait string

	maintenanceWaitOperation string
}

// Registry owns providers indexed by their unique names.
type Registry struct {
	mu        sync.RWMutex
	providers map[string]Provider
}

// NewRegistry registers providers, rejecting duplicate or empty names.
func NewRegistry(providers ...Provider) (*Registry, error) {
	r := &Registry{providers: make(map[string]Provider)}
	for _, p := range providers {
		if err := r.Register(p); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// Register adds a uniquely named provider.
func (r *Registry) Register(p Provider) error {
	if p == nil || p.Name() == "" {
		return fmt.Errorf("provider name is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.providers == nil {
		r.providers = make(map[string]Provider)
	}
	if _, ok := r.providers[p.Name()]; ok {
		return fmt.Errorf("provider %q already registered", p.Name())
	}
	r.providers[p.Name()] = p
	return nil
}

// Get returns the provider registered under a name.
func (r *Registry) Get(name string) (Provider, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.providers[name]
	if !ok {
		return nil, fmt.Errorf("unknown provider %q", name)
	}
	return p, nil
}

// List returns provider descriptions in name order.
func (r *Registry) List() []domain.ProviderInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]domain.ProviderInfo, 0, len(r.providers))
	for _, p := range r.providers {
		out = append(out, p.Describe())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
