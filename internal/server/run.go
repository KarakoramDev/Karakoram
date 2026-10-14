// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
	"github.com/KarakoramDev/Karakoram/internal/events"
	"github.com/KarakoramDev/Karakoram/internal/infra/zerops"
	"github.com/KarakoramDev/Karakoram/internal/monitoring"
	"github.com/KarakoramDev/Karakoram/internal/namespaces"
	"github.com/KarakoramDev/Karakoram/internal/orphanvolumes"
	"github.com/KarakoramDev/Karakoram/internal/rbac"
	"github.com/KarakoramDev/Karakoram/internal/reconciler"
	"github.com/KarakoramDev/Karakoram/internal/scheduler"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

// Run opens sealed persistence, initializes PKI/auth, then runs both listeners and controllers.
// Cancellation gracefully stops HTTP traffic before closing the database.
func Run(ctx context.Context, c Config, logger *slog.Logger) error {
	if err := c.Validate(); err != nil {
		return err
	}
	if logger == nil {
		logger = slog.Default()
	}
	if err := os.MkdirAll(c.DataDir, 0700); err != nil {
		return err
	}
	sealer, err := secrets.FromEnv()
	if err != nil {
		return err
	}
	st, err := store.Open(c.DatabasePath(), sealer)
	if err != nil {
		return err
	}
	defer st.Close()
	a, err := auth.New(auth.Options{Store: st, Sealer: sealer, Issuer: c.PublicURL, PublicURL: c.PublicURL, SecureCookies: len(c.PublicURL) >= 8 && c.PublicURL[:8] == "https://", Logger: logger})
	if err != nil {
		return err
	}
	if c.RecoveryPassword != "" {
		if _, err = a.RecoverAdmin(ctx, c.RecoveryAccount, c.RecoveryPassword, os.Getenv("KARAKORAM_MASTER_KEY")); err != nil {
			return err
		}
	} else {
		generated, err := a.Bootstrap(ctx, c.AdminPassword)
		if err != nil {
			return err
		}
		if generated != "" {
			logger.Warn("Initial Karakoram administrator password; change it after login", "password", generated)
		}
	}
	policy, err := rbac.New(st)
	if err != nil {
		return err
	}
	relayHostname := envDefault("KARAKORAM_RELAY_HOSTNAME", namespaces.RelayHostname(c.Hostname))
	tlsConfig, ca, err := internalTLS(ctx, st, sealer, c.Hostname, relayHostname)
	if err != nil {
		return err
	}
	backend, err := zerops.New(zerops.Options{Token: c.ZeropsToken, ProjectID: c.ZeropsProjectID, BaseURL: c.ZeropsAPIURL})
	if err != nil {
		return err
	}
	provider := psmdb.New(psmdb.Options{ControlPlaneURL: c.AdvertiseURL, InternalCAPEM: ca, Store: st, Logger: logger})
	registry, err := engine.NewRegistry(provider)
	if err != nil {
		return err
	}
	hub := events.NewHub(128)
	recorder := events.NewRecorder(st, hub)
	namespaceRegistry, err := namespaces.New(ctx, namespaces.Options{Store: st, DefaultBackend: backend, Zerops: zerops.Options{Token: c.ZeropsToken, ProjectID: c.ZeropsProjectID, BaseURL: c.ZeropsAPIURL}, PublicURL: c.PublicURL, AdvertiseURL: c.AdvertiseURL, RelayHostname: relayHostname, Events: recorder, Logger: logger})
	if err != nil {
		return err
	}
	defer namespaceRegistry.Close()
	dialer := NewAgentDialer(st, namespaceRegistry)
	defer dialer.Close()
	rc := engine.Context{Namespace: domain.DefaultNamespace, AgentURL: c.AdvertiseURL, Backend: backend, Secrets: st, Store: st, Events: recorder, Agents: dialer, Artifact: engine.ExecutableArtifact{}, Logger: logger, Clock: engine.RealClock{}}
	manager, err := reconciler.New(reconciler.Options{Store: st, Registry: registry, Namespaces: namespaceRegistry, Context: rc})
	if err != nil {
		return err
	}
	schedules, err := scheduler.New(scheduler.Options{Store: st, Trigger: manager, Logger: logger})
	if err != nil {
		return err
	}
	janitor := orphanvolumes.New(orphanvolumes.Options{Store: st, Namespaces: namespaceRegistry, Events: recorder, Logger: logger, Now: time.Now})
	collector := monitoring.New(monitoring.Options{Store: st, Dialer: dialer, Logger: logger, Targets: func(ctx context.Context) ([]monitoring.Target, error) {
		clusters, err := st.ListClusters(ctx, "")
		if err != nil {
			return nil, err
		}
		out := []monitoring.Target{}
		for _, c := range clusters {
			if !c.Spec.Monitoring.Enabled {
				continue
			}
			for _, m := range c.Status.Members {
				out = append(out, monitoring.Target{Namespace: domain.NormalizedNamespace(c.Namespace), Cluster: c.Name, Member: m.Name})
			}
		}
		return out, nil
	}})
	server, err := New(Options{Store: st, Auth: a, RBAC: policy, Registry: registry, Namespaces: namespaceRegistry, RelayHandler: namespaceRegistry.Hub.Handler(), Context: rc, Trigger: manager, Hub: hub, Monitoring: collector, PublicURL: c.PublicURL, Logger: logger})
	if err != nil {
		return err
	}
	publicListener, err := net.Listen("tcp", c.PublicListen)
	if err != nil {
		return err
	}
	defer publicListener.Close()
	internalListener, err := net.Listen("tcp", c.InternalListen)
	if err != nil {
		return err
	}
	defer internalListener.Close()
	public := &http.Server{Handler: server.Handler(), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10}
	internal := &http.Server{Handler: server.InternalHandler(), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 8 << 10, TLSConfig: tlsConfig}
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	public.BaseContext = func(net.Listener) context.Context { return child }
	internal.BaseContext = func(net.Listener) context.Context { return child }
	results := make(chan error, 9)
	var workers sync.WaitGroup
	launch := func(run func() error) { workers.Add(1); go func() { defer workers.Done(); results <- run() }() }
	launch(func() error { return public.Serve(publicListener) })
	launch(func() error { return internal.Serve(tls.NewListener(internalListener, tlsConfig)) })
	launch(func() error { return internal.Serve(tls.NewListener(namespaceRegistry.Hub.Listener(), tlsConfig)) })
	launch(func() error { return namespaceRegistry.Run(child) })
	launch(func() error { return manager.Run(child) })
	launch(func() error { return schedules.Run(child) })
	launch(func() error { return janitor.Run(child) })
	launch(func() error { return collector.Run(child) })
	launch(func() error { return server.runIdempotencyPurge(child) })
	logger.Info("Karakoram server listening", "public", c.PublicListen, "internal", c.InternalListen)
	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-results:
	}
	cancel()
	shutdown, stop := context.WithTimeout(context.Background(), 15*time.Second)
	defer stop()
	_ = public.Shutdown(shutdown)
	_ = internal.Shutdown(shutdown)
	_ = public.Close()
	_ = internal.Close()
	_ = namespaceRegistry.Close()
	workers.Wait()
	if errors.Is(runErr, http.ErrServerClosed) || errors.Is(runErr, context.Canceled) {
		return nil
	}
	return runErr
}
