// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"net"
	"time"
)

// rotation journals intent only; both credentials and rolling targets stay encrypted.
type rotation struct {
	OperationID string `json:"operationId"`
	User        string `json:"user,omitempty"`
	Stage       string `json:"stage"`
	Position    int    `json:"position,omitempty"`
}

const rotationOld = "rotation/old"
const rotationNew = "rotation/new"

func (p *Provider) runRotation(ctx context.Context, rc *engine.Context, s *state, op *domain.Operation, tls bool) (engine.OpResult, error) {
	var payload struct {
		User string `json:"user"`
		Name string `json:"name"`
	}
	if len(op.Payload) > 0 {
		if e := json.Unmarshal(op.Payload, &payload); e != nil {
			return engine.OpResult{}, invalid("invalid rotation payload")
		}
	}
	if payload.User == "" {
		payload.User = payload.Name
	}
	if !tls && payload.User != AdminUser && payload.User != ClusterUser {
		return engine.OpResult{}, invalid("user must be %s or %s", AdminUser, ClusterUser)
	}
	q := s.Rotation
	if q == nil {
		if len(op.Checkpoint) > 0 {
			var cp operationState
			if e := json.Unmarshal(op.Checkpoint, &cp); e != nil {
				return engine.OpResult{}, e
			}
			if cp.Stage == "complete" {
				return operationResult(cp, true)
			}
		}
		if s.Rolling != nil || s.ScaleDown != nil {
			return engine.OpResult{}, engine.ErrConflict
		}
		var ready bool
		var e error
		if tls {
			// TLS rotation replaces every native leaf after this preflight.
			// Other rotations must not treat expired native TLS as ready.
			var expired map[string][]byte
			expired, e = expiredActiveLeaves(ctx, rc, s)
			if e == nil {
				ready, e = p.observeClusterReady(ctx, rc, s, expired)
			}
		} else {
			ready, e = p.clusterReady(ctx, rc, s)
		}
		if e != nil {
			return engine.OpResult{}, e
		}
		if !ready {
			return engine.OpResult{}, fmt.Errorf("%w: rotation requires synchronized healthy members and Raft quorum", engine.ErrConflict)
		}
		q = &rotation{OperationID: op.ID, User: payload.User, Stage: "prepare"}
		if tls {
			q.User = ""
		}
		s.Rotation = q
		return operationResult(operationState{Stage: q.Stage}, false)
	}
	if q.OperationID != op.ID {
		return engine.OpResult{}, engine.ErrConflict
	}
	result := func(e error) (engine.OpResult, error) {
		r, _ := operationResult(operationState{Stage: q.Stage}, false)
		if q.User == ClusterUser {
			r.Message = "Coordinated full restart for shared internal credentials; brief unavailability"
		}
		return r, e
	}
	if s.Rolling != nil {
		return result(p.rollingStep(ctx, rc, s))
	}
	switch q.Stage {
	case "prepare":
		if tls {
			q.Stage = "issue"
			return result(nil)
		}
		key := secretAdmin
		if q.User == ClusterUser {
			key = secretClusterPassword
		}
		old, e := secret(ctx, rc, key)
		if e != nil {
			return result(e)
		}
		if e = putSecret(ctx, rc, rotationOld, old); e != nil {
			return result(e)
		}
		_, e = secret(ctx, rc, rotationNew)
		if missing(e) {
			v, err := secrets.Password()
			if err != nil {
				return result(err)
			}
			e = putSecret(ctx, rc, rotationNew, []byte(v))
		}
		if e != nil {
			return result(e)
		}
		if q.User == AdminUser {
			q.Stage = "add"
		} else {
			q.Stage = "stop-all"
		}
		return result(nil)
	case "issue":
		members := ordered(s)
		if q.Position == len(members) {
			q.Position = 0
			q.Stage = "tls-roll"
			return result(nil)
		}
		caPEM, e := secret(ctx, rc, secretCA)
		if e != nil {
			return result(e)
		}
		key, e := secret(ctx, rc, secretCAKey)
		if e != nil {
			return result(e)
		}
		ca, e := pki.LoadCA(caPEM, key)
		if e != nil {
			return result(e)
		}
		m := members[q.Position]
		ips := []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}
		if rc.Cluster.Spec.Exposure.Public {
			project, e := rc.Backend.ProjectInfo(ctx)
			if e != nil {
				return result(e)
			}
			for _, v := range []string{project.PublicIPv4, project.PublicIPv6} {
				if ip := net.ParseIP(v); ip != nil {
					ips = append(ips, ip)
				}
			}
		}
		crt, leafKey, e := ca.Issue(pki.CertRequest{CommonName: m.Name, DNSNames: []string{m.Name, "localhost"}, IPs: ips, Server: true})
		if e != nil {
			return result(e)
		}
		base := "tls/members/" + m.Name
		if e = putSecret(ctx, rc, base+".key", leafKey); e != nil {
			return result(e)
		}
		if e = putSecret(ctx, rc, base+".crt", crt); e != nil {
			return result(e)
		}
		q.Position++
		return result(nil)
	case "tls-roll":
		if e := p.startRolling(ctx, rc, s, "rotate-tls", op.ID, ""); e != nil {
			return result(e)
		}
		q.Stage = "finish"
		return result(nil)
	case "add":
		if e := p.startRolling(ctx, rc, s, "rotate-root-add", op.ID, ""); e != nil {
			return result(e)
		}
		q.Stage = "switch"
		return result(nil)
	case "switch":
		next, e := secret(ctx, rc, rotationNew)
		if e != nil {
			return result(e)
		}
		if e = putSecret(ctx, rc, secretAdmin, next); e != nil {
			return result(e)
		}
		q.Stage = "verify"
		return result(nil)
	case "verify":
		// The native factory now reads the new CP key. Every member must accept it
		// before freezing any target that removes the old key.
		for _, m := range ordered(s) {
			c, e := p.native(ctx, rc, s, m.Name)
			if e != nil {
				return result(e)
			}
			e = c.Ready(ctx)
			c.Close()
			if e != nil {
				return result(e)
			}
		}
		q.Stage = "remove"
		return result(nil)
	case "remove":
		if e := p.startRolling(ctx, rc, s, "rotate-root-remove", op.ID, ""); e != nil {
			return result(e)
		}
		q.Stage = "finish"
		return result(nil)
	case "stop-all":
		// Upstream accepts one shared basic password, for both inbound and outbound
		// inter-node calls. A rolling mixed-password cluster is unsafe.
		rc.Cluster.Status.Message = "Rotating internal cluster credentials: coordinated full restart; brief unavailability"
		members := ordered(s)
		if q.Position == len(members) {
			q.Position = 0
			q.Stage = "cluster-switch"
			return result(nil)
		}
		m := members[q.Position]
		if e := verifyMemberServices(ctx, rc, m); e != nil {
			return result(e)
		}
		cfg, e := p.MemberConfig(ctx, rc, m.Name)
		if e != nil {
			return result(e)
		}
		cfg.DesiredProcesses = []string{}
		if e = activateConfig(ctx, rc, s, m, cfg); e != nil {
			return result(e)
		}
		s.Pending = &pending{Action: "stop", Member: m.Name}
		q.Position++
		return result(nil)
	case "cluster-switch":
		next, e := secret(ctx, rc, rotationNew)
		if e != nil {
			return result(e)
		}
		if e = putSecret(ctx, rc, secretClusterPassword, next); e != nil {
			return result(e)
		}
		q.Stage = "cluster-config"
		return result(nil)
	case "cluster-config":
		members := ordered(s)
		if q.Position == len(members) {
			q.Position = 0
			q.Stage = "start-all"
			return result(nil)
		}
		m := members[q.Position]
		cfg, e := p.desiredConfig(ctx, rc, s, m)
		if e != nil {
			return result(e)
		}
		if e = restartConfig(&cfg, s.ConfigVersion+1); e != nil {
			return result(e)
		}
		if e = activateConfig(ctx, rc, s, m, cfg); e != nil {
			return result(e)
		}
		q.Position++
		return result(nil)
	case "start-all":
		members := ordered(s)
		if q.Position == len(members) {
			q.Stage = "cluster-ack"
			return result(nil)
		}
		s.Pending = &pending{Action: "deploy", Member: members[q.Position].Name}
		q.Position++
		return result(nil)
	case "cluster-ack":
		ready, e := p.clusterReady(ctx, rc, s)
		if e != nil || !ready {
			return result(e)
		}
		for _, m := range s.Members {
			m.CleanupStep = ""
		}
		q.Stage = "finish"
		return result(nil)
	case "finish":
		ready, e := p.clusterReady(ctx, rc, s)
		if e != nil || !ready {
			return result(e)
		}
		if q.User != "" {
			if s.LastRotated == nil {
				s.LastRotated = map[string]time.Time{}
			}
			s.LastRotated[q.User] = now(rc)
		}
		for _, key := range []string{rotationOld, rotationNew} {
			if e = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); e != nil && !missing(e) {
				return result(e)
			}
		}
		s.Rotation = nil
		return operationResult(operationState{Stage: "complete"}, true)
	}
	return result(fmt.Errorf("unknown rotation stage %q", q.Stage))
}
