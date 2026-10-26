// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/nomad"
)

type queuedCleanupBackend struct {
	*fakeBackend
	observationError error
}

func (b queuedCleanupBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "delete-process" {
		return infra.OpState{Status: "PENDING", Message: "waiting for Zerops delete process delete-process: PENDING (age 44m0s); same-service platform process deploy-process (stack.deploy) RUNNING"}, b.observationError
	}
	return b.fakeBackend.OpStatus(ctx, op)
}

func TestCleanupPreservesAcceptedDeleteAndReportsPlatformProgress(t *testing.T) {
	p, rc, backend := setup(t, "single")
	s := converge(t, p, rc)
	s.Pending = &pending{Op: infra.Op{ID: "delete-process", ServiceID: s.data()[0].ServiceID, Kind: "delete"}, Member: s.data()[0].Name, Action: "delete-runtime"}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Deleting = true
	queued := &queuedCleanupBackend{fakeBackend: backend}
	rc.Backend = queued
	for range 2 {
		done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
		if err != nil || done {
			t.Fatalf("accepted delete was not retained: done=%v err=%v", done, err)
		}
	}
	for _, want := range []string{"delete-process", "44m0s", "deploy-process", "RUNNING"} {
		if !strings.Contains(rc.Cluster.Status.Message, want) {
			t.Errorf("cleanup hid %q: %q", want, rc.Cluster.Status.Message)
		}
	}
	reset := time.Now().Add(time.Hour)
	queued.observationError = &infra.RetryAfterError{Cause: infra.ErrUnavailable, At: reset}
	done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
	if done || !infra.RetryAt(err).Equal(reset) {
		t.Fatalf("cleanup lost accepted deletion observation cooldown: done=%v err=%v", done, err)
	}
	s, err = load(rc.Cluster)
	if err != nil || s.Pending == nil || s.Pending.Op.ID != "delete-process" {
		t.Fatalf("accepted deletion checkpoint changed: %+v %v", s, err)
	}
}

type tlsVolumeDeleteBackend struct {
	*fakeBackend
	transport infra.Backend
	observed  bool
}

func (b *tlsVolumeDeleteBackend) GetService(ctx context.Context, name string) (*infra.Service, error) {
	service, err := b.fakeBackend.GetService(ctx, name)
	if service != nil && service.Kind == infra.KindVolume {
		b.observed = true
	}
	return service, err
}

func (b *tlsVolumeDeleteBackend) DeleteServiceByID(ctx context.Context, name, id string) (infra.Op, error) {
	return b.transport.DeleteServiceByID(ctx, name, id)
}

func TestVolumeDeletionTLSFailurePreservesReferences(t *testing.T) {
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("deletion reached server despite invalid TLS identity")
	}))
	t.Cleanup(upstream.Close)
	roots := x509.NewCertPool()
	roots.AddCert(upstream.Certificate())
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "wrong-server.example.test"}
	t.Cleanup(transport.CloseIdleConnections)
	factory := nomad.NewFactory(nomad.WithAllowedAddrs(upstream.URL), nomad.WithHTTPClient(&http.Client{Transport: transport}))
	backend, err := factory.New(infra.Target{Kind: "nomad", ID: "default", Token: "test-token", Config: map[string]string{"address": upstream.URL}})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"cleanup", "retirement"} {
		t.Run(path, func(t *testing.T) {
			p, rc, b := setup(t, "single")
			s := converge(t, p, rc)
			m := s.data()[0]
			volume, volumeID := m.Volume, m.VolumeID
			delete(b.services, m.Name)
			m.ServiceID = ""
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			failing := &tlsVolumeDeleteBackend{fakeBackend: b, transport: backend}
			rc.Backend = failing
			var done bool
			if path == "cleanup" {
				done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
				s, _ = load(rc.Cluster)
				m = s.data()[0]
			} else {
				done, err = p.retireMember(t.Context(), rc, s, m)
			}
			if done || !errors.Is(err, infra.ErrConfiguration) || errors.Is(err, infra.ErrRejected) || !failing.observed {
				t.Fatalf("TLS failure after observation lost retry classification: done=%v observed=%v err=%v", done, failing.observed, err)
			}
			if m.Volume != volume || m.VolumeID != volumeID {
				t.Fatal("TLS deletion failure discarded volume references")
			}
			orphans, err := rc.Store.ListOrphanedVolumes(t.Context(), rc.Cluster.Namespace)
			if err != nil || len(orphans) != 0 {
				t.Fatalf("TLS failure orphaned volume: %+v %v", orphans, err)
			}
		})
	}
}
