// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package certificates provides secret-free expiry inventory and durable daily warnings.
package certificates

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/events"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type Expiry = store.CertificateExpiry

func Inventory(ctx context.Context, st *store.Store) ([]Expiry, error) {
	return st.CertificateInventory(ctx)
}

type Options struct {
	Store  *store.Store
	Hub    *events.Hub
	Now    func() time.Time
	Tick   <-chan time.Time
	Logger *slog.Logger
}

type Watcher struct{ opts Options }

func New(opts Options) *Watcher {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Watcher{opts: opts}
}

func (w *Watcher) Run(ctx context.Context) error {
	tick := w.opts.Tick
	if tick == nil {
		timer := time.NewTicker(time.Hour)
		defer timer.Stop()
		tick = timer.C
	}
	for {
		if err := w.Scan(ctx); err != nil && ctx.Err() == nil {
			w.opts.Logger.Error("certificate inventory incomplete; retrying", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick:
		}
	}
}

// Scan records at most one warning per certificate identity per UTC day, including
// across restarts. Warning messages never copy certificate subjects or condition text.
func (w *Watcher) Scan(ctx context.Context) error {
	now := w.opts.Now().UTC()
	inventory, failures := Inventory(ctx, w.opts.Store)
	for _, cert := range inventory {
		if cert.NotAfter.After(now.Add(30 * 24 * time.Hour)) {
			continue
		}
		message := "Certificate expires at " + cert.NotAfter.UTC().Format(time.RFC3339)
		if !cert.NotAfter.After(now) {
			message = "Certificate expired at " + cert.NotAfter.UTC().Format(time.RFC3339)
		}
		failures = errors.Join(failures, w.warning(ctx, cert.Namespace, cert.Cluster, cert.Name, message, now))
	}
	clusters, err := w.opts.Store.ListClusters(ctx, "")
	if err != nil {
		failures = errors.Join(failures, errors.New("cannot inspect all engine TLS conditions"))
	}
	for _, cluster := range clusters {
		for _, condition := range cluster.Status.Conditions {
			if condition.Type != "TLSCertificate" || (condition.Reason != "ExpiringSoon" && condition.Reason != "Expired") {
				continue
			}
			// A parsed user certificate already covers this same engine condition.
			covered := false
			for _, cert := range inventory {
				if cert.Namespace == domain.NormalizedNamespace(cluster.Namespace) && cert.Cluster == cluster.Name && cert.Name == "tls/user.crt" && !cert.NotAfter.After(now.Add(30*24*time.Hour)) {
					covered = true
					break
				}
			}
			if !covered {
				failures = errors.Join(failures, w.warning(ctx, domain.NormalizedNamespace(cluster.Namespace), cluster.Name, "engine-tls", "Engine TLS certificate condition: "+condition.Reason, now))
			}
		}
	}
	return failures
}

func (w *Watcher) warning(ctx context.Context, namespace, cluster, name, message string, now time.Time) error {
	identity := sha256.Sum256([]byte(namespace + "\x00" + cluster + "\x00" + name))
	key := fmt.Sprintf("certificates/warnings/%x/%s", identity, now.Format("2006-01-02"))
	event := domain.Event{Namespace: namespace, Cluster: cluster, Resource: "certificates", Target: name, Type: "Warning", Reason: "CertificateExpiring", Message: message, Actor: "system:certificates", CreatedAt: now}
	inserted, err := w.opts.Store.AppendDeduplicatedEvent(ctx, key, &event)
	if err == nil && inserted && w.opts.Hub != nil {
		w.opts.Hub.Publish(event)
	}
	return err
}
