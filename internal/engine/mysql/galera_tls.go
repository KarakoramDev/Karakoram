// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"time"
)

const galeraTrafficBase = "tls/galera/server"
const galeraTrafficPending = "mysql/galera-tls/pending"
const galeraTrafficCA = "tls/galera/ca"

var galeraTrafficFiles = [...]struct{ suffix, active string }{
	{".ca.key", galeraTrafficCA + ".key"}, {".ca.crt", galeraTrafficCA + ".crt"},
	{".key", galeraTrafficBase + ".key"}, {".crt", galeraTrafficBase + ".crt"},
}

// Galera/SST share one traffic leaf independently of each member's SQL leaf.
// Its static TLS contexts are replaced only with every data process stopped.
// All positions are recovered before the selected seed restarts; the history
// UUID remains unchanged across this encryption maintenance operation.
type galeraTLSRotation struct {
	Stage   string   `json:"stage"`
	Members []string `json:"members"`
}

func ensureGaleraTrafficCertificate(ctx context.Context, rc *engine.Context, s *state) error {
	cert, err := secret(ctx, rc, galeraTrafficBase+".crt")
	if missing(err) {
		for _, m := range s.data() {
			if m.Initialized {
				return engine.Invalid("initialized PXC cluster has no shared traffic certificate")
			}
		}
		if err := issueGaleraTrafficBundle(ctx, rc, false); err != nil {
			return err
		}
		publishRevision(s)
		return nil
	}
	if err != nil {
		return err
	}
	if s.GaleraTLS != nil {
		return nil
	}
	renew, err := pki.NeedsRenewal(cert, 30*24*time.Hour, now(rc))
	if err != nil || !renew {
		return err
	}
	if s.Rolling != nil || s.Replacement != nil || s.TLSRotation != nil {
		return nil
	}
	for _, m := range s.data() {
		if !m.Initialized || m.Stage != "joined" {
			return nil
		}
	}
	if err := maintenanceAllowed(ctx, rc, s); err != nil {
		// Renewal must not own the recovery path it is waiting to finish.
		// Store/observation failures still propagate; only a maintenance fence
		// defers this optional initiation.
		if errors.Is(err, engine.ErrUnavailable) {
			return nil
		}
		return err
	}
	return requestGaleraTrafficRotation(ctx, rc, s)
}
func issueGaleraTrafficBundle(ctx context.Context, rc *engine.Context, staged bool) error {
	// A separate trust root prevents SQL or agent leaves from authenticating
	// replication. Replacing this root revokes every previous traffic leaf.
	ca, err := pki.NewCA(rc.Cluster.Name+"-galera-ca", 0)
	if err != nil {
		return err
	}
	cert, key, err := ca.Issue(pki.CertRequest{CommonName: rc.Cluster.Name + "-galera", DNSNames: []string{rc.Cluster.Name + "-galera"}, Server: true, Client: true})
	if err != nil {
		return err
	}
	values := [...][]byte{ca.KeyPEM, ca.CertPEM, key, cert}
	for i, file := range galeraTrafficFiles {
		target := file.active
		if staged {
			target = galeraTrafficPending + file.suffix
		}
		if err := putSecret(ctx, rc, target, values[i]); err != nil {
			return err
		}
	}
	return nil
}
func requestGaleraTrafficRotation(ctx context.Context, rc *engine.Context, s *state) error {
	if s.GaleraTLS != nil {
		return nil
	}
	for _, m := range s.data() {
		if !m.Initialized || m.Stage != "joined" {
			return engine.ErrUnavailable
		}
	}
	if err := issueGaleraTrafficBundle(ctx, rc, true); err != nil {
		return err
	}
	q := &galeraTLSRotation{Stage: "fence"}
	for _, m := range s.data() {
		if m.Initialized {
			q.Members = append(q.Members, m.Name)
		}
	}
	if len(q.Members) == 0 {
		return engine.Invalid("traffic certificate rotation requires initialized Galera members")
	}
	s.GaleraTLS = q
	return nil
}
func (p *Core) reconcileGaleraTrafficRotation(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	q := s.GaleraTLS
	if q == nil {
		return false, nil
	}
	switch q.Stage {
	case "fence":
		for _, name := range q.Members {
			m := s.find(name)
			if m == nil {
				return true, engine.ErrConflict
			}
			m.Offline = true
		}
		publishRevision(s)
		q.Stage = "stopped"
	case "stopped":
		for _, name := range q.Members {
			m := s.find(name)
			if m == nil {
				return true, engine.ErrConflict
			}
			ok, err := configApplied(ctx, rc, m)
			if err != nil || !ok {
				return true, err
			}
			if !galeraStopped(ctx, rc, name) {
				return true, nil
			}
		}
		// Never publish mixed traffic keys to running members. Replaying these
		// copies is safe: no data process can start while its desired set is empty.
		for _, file := range galeraTrafficFiles {
			value, err := secret(ctx, rc, galeraTrafficPending+file.suffix)
			if err != nil {
				return true, err
			}
			if err := putSecret(ctx, rc, file.active, value); err != nil {
				return true, err
			}
		}
		publishRevision(s)
		q.Stage = "applied"
	case "applied":
		for _, name := range q.Members {
			m := s.find(name)
			if m == nil {
				return true, engine.ErrConflict
			}
			ok, err := configApplied(ctx, rc, m)
			if err != nil || !ok {
				return true, err
			}
			if !galeraStopped(ctx, rc, name) {
				return true, engine.ErrUnavailable
			}
		}
		s.Formation.Reformation = &reformation{State: "observing"}
		q.Stage = "reform"
	case "reform":
		busy, err := p.reconcileGaleraFormation(ctx, rc, s)
		if err != nil || busy {
			return true, err
		}
		if s.Formation.reforming() || s.Formation.State != "formed" {
			return true, nil
		}
		for _, name := range q.Members {
			m := s.find(name)
			if m == nil {
				return true, engine.ErrConflict
			}
			ok, err := ready(ctx, rc, m)
			if err != nil || !ok {
				return true, err
			}
		}
		q.Stage = "cleanup"
	case "cleanup":
		for _, file := range galeraTrafficFiles {
			if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), galeraTrafficPending+file.suffix); err != nil && !missing(err) {
				return true, err
			}
		}
		s.GaleraTLS = nil
	default:
		return true, engine.Invalid("unknown Galera traffic TLS checkpoint %s", q.Stage)
	}
	return true, nil
}
