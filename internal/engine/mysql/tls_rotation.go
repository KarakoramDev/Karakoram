// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"net"
	"time"
)

type tlsRotationState struct {
	Operation        string   `json:"operation"`
	Stage            string   `json:"stage"`
	Members          []string `json:"members"`
	Index            int      `json:"index"`
	TrafficRequested bool     `json:"trafficRequested,omitempty"`
}

func (p *Core) rotateTLS(ctx context.Context, rc *engine.Context, op *domain.Operation) (result engine.OpResult, err error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return result, err
	}
	defer func() {
		if e := save(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	if s.LastTLSRotation == op.ID {
		return engine.OpResult{Done: true}, nil
	}
	if s.TLSRotation == nil {
		var previous tlsRotationState
		if len(op.Checkpoint) > 0 {
			if err = json.Unmarshal(op.Checkpoint, &previous); err != nil {
				return result, err
			}
			if previous.Stage == "done" {
				return engine.OpResult{Done: true, Checkpoint: op.Checkpoint}, nil
			}
		}
		if err := maintenanceAllowed(ctx, rc, s); err != nil {
			return result, err
		}
		if s.Rolling != nil || s.Replacement != nil || s.Pending != nil {
			return result, engine.ErrUnavailable
		}
		cp := &tlsRotationState{Operation: op.ID, Stage: "issue"}
		for _, m := range s.Members {
			cp.Members = append(cp.Members, m.Name)
		}
		s.TLSRotation = cp
	}
	cp := s.TLSRotation
	if cp.Operation != op.ID {
		return result, engine.ErrConflict
	}
	prefix := "mysql/tls-rotation/" + secretsHash(op.ID) + "/"
	switch cp.Stage {
	case "issue":
		if cp.Index == len(cp.Members) {
			cp.Stage = "activate"
			cp.Index = 0
			break
		}
		name := cp.Members[cp.Index]
		crt, err := secret(ctx, rc, "tls/ca.crt")
		if err != nil {
			return result, err
		}
		key, err := secret(ctx, rc, "tls/ca.key")
		if err != nil {
			return result, err
		}
		ca, err := pki.LoadCA(crt, key)
		if err != nil {
			return result, err
		}
		ips := []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}
		if rc.Cluster.Spec.Exposure.Public {
			project, err := rc.Backend.ProjectInfo(ctx)
			if err != nil {
				return result, err
			}
			for _, raw := range []string{project.PublicIPv4, project.PublicIPv6} {
				if ip := net.ParseIP(raw); ip != nil {
					ips = append(ips, ip)
				}
			}
		}
		for _, kind := range []string{"agents", "servers"} {
			base := prefix + kind + "/" + name
			cert, key, err := ca.Issue(pki.CertRequest{CommonName: name, DNSNames: []string{name, "localhost"}, IPs: ips, Server: true, Client: kind == "servers"})
			if err != nil {
				return result, err
			}
			if err = putSecret(ctx, rc, base+".key", key); err != nil {
				return result, err
			}
			if err = putSecret(ctx, rc, base+".crt", cert); err != nil {
				return result, err
			}
		}
		cp.Index++
	case "activate":
		if cp.Index == len(cp.Members) {
			publishRevision(s)
			cp.Stage = "reload"
			cp.Index = 0
			break
		}
		if wait := rc.WaitForWindow(); wait.RequeueAfter > 0 {
			result := tlsRotationResult(cp)
			result.RequeueAfter = wait.RequeueAfter
			return result, nil
		}
		name := cp.Members[cp.Index]
		for _, kind := range []string{"agents", "servers"} {
			for _, suffix := range []string{".key", ".crt"} {
				value, err := secret(ctx, rc, prefix+kind+"/"+name+suffix)
				if err != nil {
					return result, err
				}
				if err = putSecret(ctx, rc, "tls/"+kind+"/"+name+suffix, value); err != nil {
					return result, err
				}
			}
		}
		cp.Index++
	case "reload":
		for _, name := range cp.Members {
			m := s.find(name)
			if m == nil {
				return result, engine.ErrNotFound
			}
			applied, err := configApplied(ctx, rc, m)
			if err != nil || !applied {
				return tlsRotationResult(cp), err
			}
		}
		if s.Topology == "cluster" {
			cp.Stage = "traffic"
		} else {
			cp.Stage = "cleanup"
		}
	case "traffic":
		if !cp.TrafficRequested {
			if err := requestGaleraTrafficRotation(ctx, rc, s); err != nil {
				return result, err
			}
			cp.TrafficRequested = true
			return tlsRotationResult(cp), nil
		}
		if busy, err := p.reconcileGaleraTrafficRotation(ctx, rc, s); busy || err != nil {
			return tlsRotationResult(cp), err
		}
		cp.Stage = "cleanup"
	case "cleanup":
		for _, name := range cp.Members {
			for _, kind := range []string{"agents", "servers"} {
				for _, suffix := range []string{".key", ".crt"} {
					if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), prefix+kind+"/"+name+suffix); err != nil && !missing(err) {
						return result, err
					}
				}
			}
		}
		cp.Stage = "done"
	case "done":
		result := tlsRotationResult(cp)
		result.Done = true
		s.LastTLSRotation = cp.Operation
		s.TLSRotation = nil
		return result, nil
	default:
		return result, engine.Invalid("unknown TLS rotation checkpoint")
	}
	return tlsRotationResult(cp), nil
}
func tlsRotationResult(cp *tlsRotationState) engine.OpResult {
	raw, _ := json.Marshal(cp)
	return engine.OpResult{Checkpoint: raw, Step: cp.Stage, RequeueAfter: 2 * time.Second}
}
