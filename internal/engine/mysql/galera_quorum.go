// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"time"
)

type quorumRecovery struct {
	Stage     string              `json:"stage"`
	Survivors []string            `json:"survivors"`
	Missing   []string            `json:"missing"`
	FenceOps  map[string]infra.Op `json:"fenceOps"`
	Seed      string              `json:"seed,omitempty"`
	UUID      string              `json:"uuid,omitempty"`
	Seqno     int64               `json:"seqno,omitempty"`
}

func (p *Core) recoverGaleraQuorum(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	var q quorumRecovery
	result := func(done bool) (engine.OpResult, error) {
		raw, err := json.Marshal(q)
		return engine.OpResult{Done: done, Checkpoint: raw, Step: q.Stage, RequeueAfter: 2 * time.Second}, err
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return engine.OpResult{}, err
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return engine.OpResult{}, err
	}
	if s.Suspended || s.Pending != nil || s.Rolling != nil || s.Replacement != nil || s.TLSRotation != nil || s.GaleraTLS != nil || bs.RestoreOperation != nil {
		return engine.OpResult{}, engine.ErrUnavailable
	}
	if len(op.Checkpoint) != 0 {
		if err := json.Unmarshal(op.Checkpoint, &q); err != nil {
			return engine.OpResult{}, err
		}
	}
	if q.Stage == "" {
		var request struct {
			Confirmation string `json:"confirmation"`
		}
		if err := json.Unmarshal(op.Payload, &request); err != nil || request.Confirmation != myspec.GaleraForcePrimaryConfirmation {
			return engine.OpResult{}, engine.Invalid("explicit absent-member fencing confirmation required")
		}
		q.FenceOps = map[string]infra.Op{}
		for _, m := range s.data() {
			st, err := galeraStatus(ctx, rc, m.Name)
			if err == nil && st.ClusterStatus == "Primary" {
				return engine.OpResult{}, engine.Invalid("a Primary component already exists")
			}
			if err == nil && st.Connected && myspec.ValidUUID(st.StateUUID) && st.LastCommitted >= 0 {
				if q.UUID != "" && q.UUID != st.StateUUID {
					return engine.OpResult{}, engine.Invalid("survivor Galera state UUIDs disagree")
				}
				q.UUID = st.StateUUID
				q.Survivors = append(q.Survivors, m.Name)
				if q.Seed == "" || st.LastCommitted > q.Seqno {
					q.Seed, q.Seqno = m.Name, st.LastCommitted
				}
			} else {
				q.Missing = append(q.Missing, m.Name)
			}
		}
		if q.Seed == "" || len(q.Missing) == 0 {
			return engine.OpResult{}, engine.Invalid("recovery requires a minority survivor and missing members")
		}
		q.Stage = "fence"
		return result(false)
	}
	if q.Stage == "fence" {
		for _, name := range q.Missing {
			m := s.find(name)
			if m == nil {
				return engine.OpResult{}, engine.ErrConflict
			}
			if err := verifyMember(ctx, rc, m); err != nil {
				return engine.OpResult{}, err
			}
			if _, exists := q.FenceOps[name]; !exists {
				receipt, err := rc.Backend.Stop(ctx, name)
				if err != nil {
					return engine.OpResult{}, err
				}
				q.FenceOps[name] = receipt
				return result(false)
			}
			status, err := rc.Backend.OpStatus(ctx, q.FenceOps[name])
			if err != nil {
				return engine.OpResult{}, err
			}
			if status.Failed {
				return engine.OpResult{}, engine.ErrUnavailable
			}
			if !status.Done {
				return result(false)
			}
			runtime, err := engine.ObserveRetiringService(ctx, rc.Backend, name, m.ServiceID, m.Volume, m.VolumeID, false)
			if err != nil {
				return engine.OpResult{}, err
			}
			if runtime != nil && runtime.Status != infra.StatusStopped {
				return result(false)
			}
			m.Offline = true
		}
		publishRevision(s)
		if err := save(rc.Cluster, s); err != nil {
			return engine.OpResult{}, err
		}
		q.Stage = "bootstrap"
		return result(false)
	}
	if q.Stage == "bootstrap" {
		// Reobserve after fencing; never promote a stale survivor or a different history.
		for _, name := range q.Survivors {
			st, err := galeraStatus(ctx, rc, name)
			if err != nil || st.StateUUID != q.UUID || st.LastCommitted < q.Seqno && name == q.Seed {
				return engine.OpResult{}, engine.ErrUnavailable
			}
			if name == q.Seed && st.Eligible() {
				q.Stage = "observe"
				return result(false)
			}
			if st.ClusterStatus == "Primary" {
				return engine.OpResult{}, engine.Invalid("unexpected Primary survivor during fenced recovery")
			}
			if st.LastCommitted > q.Seqno {
				q.Seed, q.Seqno = name, st.LastCommitted
				return result(false)
			}
		}
		err := mysqlAction(ctx, rc, q.Seed, myspec.ActionGaleraForcePrimary, myspec.GaleraForcePrimaryRequest{ClusterName: s.Formation.GroupName, FormationEpoch: s.Formation.FormationEpoch, Confirmation: myspec.GaleraForcePrimaryConfirmation}, nil)
		if err != nil {
			return engine.OpResult{}, err
		}
		q.Stage = "observe"
		return result(false)
	}
	if q.Stage == "observe" {
		st, err := galeraStatus(ctx, rc, q.Seed)
		if err != nil || !st.Eligible() {
			return result(false)
		}
		for _, name := range q.Missing {
			s.find(name).Offline = false
		}
		publishRevision(s)
		if err := save(rc.Cluster, s); err != nil {
			return engine.OpResult{}, err
		}
		// Infrastructure fencing remains until the normal deployment path restarts it.
		for _, name := range q.Missing {
			m := s.find(name)
			m.Stage = "imported"
			m.DeployOp = nil
		}
		if err := save(rc.Cluster, s); err != nil {
			return engine.OpResult{}, err
		}
		q.Stage = "done"
		return result(true)
	}
	if q.Stage == "done" {
		return result(true)
	}
	return engine.OpResult{}, engine.Invalid("unknown Galera quorum recovery checkpoint")
}
