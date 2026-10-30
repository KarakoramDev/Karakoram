// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

type pxcRestoreCertification struct {
	Phase                  string                                 `json:"phase"`
	Round                  int                                    `json:"round"`
	Attempts               map[string]int                         `json:"attempts,omitempty"`
	Target                 string                                 `json:"target,omitempty"`
	Before                 map[string]myspec.GaleraStatusResponse `json:"before,omitempty"`
	Probe                  *myspec.GaleraStatusResponse           `json:"probe,omitempty"`
	Evidence               []pxcRestoreGTIDEvidence               `json:"evidence,omitempty"`
	Certified              bool                                   `json:"certified,omitempty"`
	Negotiated             bool                                   `json:"negotiated,omitempty"`
	Legacy                 bool                                   `json:"legacy,omitempty"`
	CleanupPending         bool                                   `json:"cleanupPending,omitempty"`
	CleanupComplete        bool                                   `json:"cleanupComplete,omitempty"`
	LegacyCleanupPending   bool                                   `json:"legacyCleanupPending,omitempty"`
	FencesOwned            bool                                   `json:"fencesOwned,omitempty"`
	RecoveryCleanupPending bool                                   `json:"recoveryCleanupPending,omitempty"`
}

type pxcRestoreGTIDEvidence struct {
	Round   int                                    `json:"round"`
	Epoch   uint64                                 `json:"epoch"`
	Before  map[string]myspec.GaleraStatusResponse `json:"before"`
	After   map[string]myspec.GaleraStatusResponse `json:"after"`
	Probe   myspec.GaleraStatusResponse            `json:"probe"`
	Mapping map[string]string                      `json:"mapping"`
}

func reloadRestoreGTIDMembers(ctx context.Context, rc *engine.Context, s *state) error {
	for _, m := range s.data() {
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			return err
		}
		if err = a.Reload(ctx); err != nil {
			return err
		}
	}
	return nil
}

func fencePXCRestoreClients(ctx context.Context, rc *engine.Context, s *state, owner string) (bool, error) {
	if s.RestoreGTIDFence != owner {
		s.RestoreGTIDFence = owner
		publishRevision(s)
		if err := save(rc.Cluster, s); err != nil {
			return false, err
		}
	}
	for _, m := range s.Members {
		if m.Role != "proxy" || m.ServiceID == "" || m.Removing {
			continue
		}
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			return false, err
		}
		if err = a.Reload(ctx); err != nil {
			return false, err
		}
		if err = a.Process(ctx, agentapi.ProcessAction{Process: "proxysql", Action: "stop", RequestID: owner + "/gtid-client-fence/" + m.Name}); err != nil {
			return false, err
		}
		status, err := a.Status(ctx)
		if err != nil {
			return false, err
		}
		if status.Processes["proxysql"].PID != 0 {
			return false, nil
		}
	}
	return true, nil
}

// Terminal cleanup must attempt every proxy release even if a data agent is
// unavailable. Activation instead waits for confirmed native fence release.
func releaseRestoreGTIDFence(ctx context.Context, rc *engine.Context, s *state, owner string, terminal bool) error {
	if s.RestoreGTIDFence != "" && s.RestoreGTIDFence != owner {
		return engine.ErrConflict
	}
	s.RestoreGTIDFence = ""
	for _, m := range s.data() {
		m.GaleraSSTDonor = ""
	}
	publishRevision(s)
	if err := save(rc.Cluster, s); err != nil {
		return err
	}
	var releaseErr error
	remember := func(err error) {
		if releaseErr == nil {
			releaseErr = err
		}
	}
	for _, m := range s.data() {
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			remember(err)
			continue
		}
		if err = a.Reload(ctx); err != nil {
			remember(err)
			continue
		}
		if m.Offline {
			continue
		}
		status, err := galeraStatus(ctx, rc, m.Name)
		if err != nil {
			if terminal {
				processes, statusErr := a.Status(ctx)
				// Desired fence removal is already acknowledged. A stopped
				// mysqld has no live SQL fence; legacy reformation must be
				// allowed to start it rather than wait for SQL before start.
				if statusErr == nil && processes.Processes["mysqld"].PID == 0 {
					continue
				}
			}
			remember(err)
			continue
		}
		if status.OfflineMode {
			remember(engine.ErrUnavailable)
		}
	}
	if releaseErr != nil && !terminal {
		return releaseErr
	}
	for _, m := range s.Members {
		if m.Role != "proxy" || m.ServiceID == "" || m.Removing || m.Offline {
			continue
		}
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			remember(err)
			continue
		}
		if err = a.Reload(ctx); err != nil {
			remember(err)
			continue
		}
		remember(a.Process(ctx, agentapi.ProcessAction{Process: "proxysql", Action: "start", RequestID: owner + "/gtid-client-release/" + m.Name}))
	}
	return releaseErr
}

func blockRestoreGTID(ctx context.Context, rc *engine.Context, s *state, op *domain.Operation, cp *backupCheckpoint, reason string) error {
	s.RestoreGTIDBlocked = "PXC restore GTID certification blocked: " + reason + "; preserve member GTIDs and native logs, repair from the restored seed under operator control, then take a fresh full backup; never reset or inject GTIDs"
	cp.Step, cp.GTIDCertification.Phase = "certify-cleanup", "cleanup"
	cp.GTIDCertification.CleanupPending, cp.GTIDCertification.CleanupComplete = true, false
	if err := save(rc.Cluster, s); err != nil {
		return err
	}
	if err := persistRestore(rc, op, *cp); err != nil {
		return err
	}
	if err := completeRestoreGTIDCleanup(ctx, rc, s, op, cp); err != nil {
		return err
	}
	return engine.Invalid("%s", s.RestoreGTIDBlocked)
}

// Each phase is checkpointed by runRestore before the next irreversible action.
// This path is restore-only: the explicit restore already authorizes replacing
// follower data, while arbitrary live re-formations do not grant that authority.
func certifyPXCRestore(ctx context.Context, rc *engine.Context, s *state, op *domain.Operation, cp *backupCheckpoint) error {
	if cp.GTIDCertification == nil {
		cp.GTIDCertification = &pxcRestoreCertification{Phase: "fence", Attempts: map[string]int{}}
	}
	c := cp.GTIDCertification
	if c.Phase == "negotiated" {
		c.Phase = "fence"
	}
	if c.Attempts == nil {
		c.Attempts = map[string]int{}
	}
	if c.Phase == "blocked" {
		return engine.Invalid("%s", s.RestoreGTIDBlocked)
	}
	seed := s.find(s.Formation.Seed)
	if seed == nil {
		return engine.Invalid("PXC restore certification lacks its restored seed")
	}
	// Recovery after a cancelled/failed operation must reacquire its client fence
	// before any further probe or replacement. Native observations verify it below.
	if fenced, err := acquirePXCRestoreFences(ctx, rc, s, op, cp); err != nil || !fenced {
		return err
	}
	switch c.Phase {
	case "fence", "observe":
		if err := reloadRestoreGTIDMembers(ctx, rc, s); err != nil {
			return err
		}
		observed := observePXCQuorumGTIDs(ctx, rc, s)
		if !completeGTIDObservation(observed) {
			return nil
		}
		for _, status := range observed {
			if !status.OfflineMode {
				return nil
			}
		}
		c.Before, c.Probe = observed, nil
		c.Round++
		c.Phase = "probe"
	case "probe":
		var probe myspec.GaleraStatusResponse
		req := myspec.GaleraHistoryProbeRequest{Receipt: fmt.Sprintf("%s/gtid-probe/%d", op.ID, c.Round), ClusterName: s.Formation.GroupName, FormationEpoch: s.Formation.FormationEpoch}
		if err := mysqlAction(ctx, rc, seed.Name, myspec.ActionGaleraHistoryProbe, req, &probe); err != nil {
			return err
		}
		if !probe.Eligible() || !probe.OfflineMode || probe.StateUUID != c.Before[seed.Name].StateUUID {
			return engine.ErrUnavailable
		}
		c.Probe, c.Phase = &probe, "compare"
	case "compare":
		if c.Probe == nil {
			return engine.Invalid("PXC restore certification lacks its probe receipt")
		}
		observed := observePXCQuorumGTIDs(ctx, rc, s)
		if !completeGTIDObservation(observed) {
			return nil
		}
		mappings := make(map[string]string, len(observed))
		for _, m := range s.data() {
			status := observed[m.Name]
			if !status.OfflineMode || status.StateUUID != c.Probe.StateUUID || status.ServerUUID != c.Before[m.Name].ServerUUID || status.LastCommitted < c.Probe.LastCommitted {
				return nil
			}
			sid, err := myspec.PXCActiveSIDAdvance(c.Before[m.Name].GTIDExecuted, status.GTIDExecuted, status.StateUUID)
			if err != nil {
				return blockRestoreGTID(ctx, rc, s, op, cp, err.Error())
			}
			if sid == "" {
				return nil
			} // Wait for this member to execute the probe.
			mappings[m.Name] = sid
		}
		if len(c.Evidence) == 0 || c.Evidence[len(c.Evidence)-1].Round != c.Round {
			c.Evidence = append(c.Evidence, pxcRestoreGTIDEvidence{Round: c.Round, Epoch: s.Formation.FormationEpoch, Before: c.Before, After: observed, Probe: *c.Probe, Mapping: mappings})
			if err := persistRestore(rc, op, *cp); err != nil {
				return err
			}
		}
		// Preserve the real discontinuity before replacing any member's history.
		historyConflict := false
		if err := checkPXCQuorumGTIDs(ctx, rc, s); err != nil {
			if conflict, ok := err.(*myspec.GTIDQuorumError); !ok || !conflict.Diverged {
				return err
			}
			historyConflict = true
		}
		target := ""
		for _, m := range s.data() {
			if mappings[m.Name] != mappings[seed.Name] {
				target = m.Name
				break
			}
		}
		if target == "" {
			if err := myspec.CheckGTIDQuorum(observed); err != nil {
				return blockRestoreGTID(ctx, rc, s, op, cp, "members share an active SID mapping but their frozen executed histories differ")
			}
			if historyConflict {
				return blockRestoreGTID(ctx, rc, s, op, cp, "equal-position active GTID prefixes do not converge to the restored seed")
			}
			for _, status := range observed {
				if status.LastCommitted != observed[seed.Name].LastCommitted {
					return nil
				}
			}
			if s.GTIDPrefixLag != nil {
				return nil
			}
			c.Certified, c.Phase, cp.Step = true, "certified", "activate"
			return nil
		}
		if c.Attempts[target] >= 2 {
			return blockRestoreGTID(ctx, rc, s, op, cp, "member "+target+" still disagrees with the restored seed after two SST remedies")
		}
		c.Attempts[target]++
		c.Target, c.Phase = target, "stop"
		s.Formation.FormationEpoch++
		member := s.find(target)
		member.Offline, member.Initialized, member.Stage = true, false, "deployed"
		member.GaleraSSTDonor = seed.Name
		publishRevision(s)
		return save(rc.Cluster, s)
	case "stop":
		target := s.find(c.Target)
		if target == nil || target.Name == seed.Name {
			return engine.Invalid("invalid PXC restore SST remedy target")
		}
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), target.Name)
		if err != nil {
			return err
		}
		if err = a.Reload(ctx); err != nil {
			return err
		}
		if err = a.Process(ctx, agentapi.ProcessAction{Process: "mysqld", Action: "stop", RequestID: fmt.Sprintf("%s/gtid-remedy/%d/stop/%s", op.ID, c.Round, target.Name)}); err != nil {
			return err
		}
		status, err := a.Status(ctx)
		if err != nil {
			return err
		}
		if status.Processes["mysqld"].PID == 0 {
			c.Phase = "reset"
		}
	case "reset":
		target := s.find(c.Target)
		if target == nil || target.Name == seed.Name {
			return engine.Invalid("invalid PXC restore SST remedy target")
		}
		applied, err := configApplied(ctx, rc, target)
		if err != nil || !applied {
			return err
		}
		req := myspec.GaleraBootstrapRequest{ClusterName: s.Formation.GroupName, FormationEpoch: s.Formation.FormationEpoch}
		if err = mysqlAction(ctx, rc, target.Name, myspec.ActionGaleraPrepareJoin, req, nil); err != nil {
			return err
		}
		c.Phase = "join"
	case "join":
		target := s.find(c.Target)
		if target == nil || target.Name == seed.Name {
			return engine.Invalid("invalid PXC restore SST remedy target")
		}
		if target.Offline {
			target.Offline = false
			publishRevision(s)
			if err := save(rc.Cluster, s); err != nil {
				return err
			}
		}
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), target.Name)
		if err != nil {
			return err
		}
		if err = a.Reload(ctx); err != nil {
			return err
		}
		if err = a.Process(ctx, agentapi.ProcessAction{Process: "mysqld", Action: "start", RequestID: fmt.Sprintf("%s/gtid-remedy/%d/start/%s", op.ID, c.Round, target.Name)}); err != nil {
			return err
		}
		status, err := galeraStatus(ctx, rc, target.Name)
		if err != nil {
			return err
		}
		if !status.Eligible() || !status.OfflineMode || status.ClusterSize != len(s.data()) {
			return nil
		}
		target.Initialized, target.Stage, target.ServerUUID = true, "joined", status.ServerUUID
		target.GaleraSSTDonor = ""
		c.Target, c.Phase = "", "observe"
		publishRevision(s)
		return save(rc.Cluster, s)
	case "certified":
		cp.Step = "activate"
	default:
		return engine.Invalid("unknown PXC restore certification phase")
	}
	return nil
}

const restoreCertificationUnavailable = "Agents do not all advertise PXC restore certification support; agents are outdated or incompatible. Re-create the cluster to get certified restores. Legacy restore remains available, but its GTID history is still guarded and real divergence remains latched."

// Negotiate before any certification hold or native client fence is acquired.
// An absent optional flag means legacy compatibility, not a failed restoration.
func negotiatePXCRestoreCertification(ctx context.Context, rc *engine.Context, s *state, op *domain.Operation, cp *backupCheckpoint) error {
	if rc.Cluster.Spec.Provider != "pxc" {
		return nil
	}
	c := cp.GTIDCertification
	if c == nil || !c.Negotiated {
		if rc.Agents == nil {
			return engine.ErrUnavailable
		}
		supported := make([]bool, len(s.Members))
		unavailable := make([]bool, len(s.Members))
		participating := make([]bool, len(s.Members))
		var wg sync.WaitGroup
		for i, m := range s.Members {
			if m.Removing || m.Role != "data" && m.Role != "proxy" {
				continue
			}
			// Unprovisioned initial-restore proxies cannot serve clients or own a hold.
			// They are deployed only after the restored writer becomes available.
			if m.ServiceID == "" && m.Role == "proxy" {
				continue
			}
			participating[i] = true
			if m.ServiceID == "" {
				unavailable[i] = true
				continue
			}
			wg.Add(1)
			go func(i int, name string) {
				defer wg.Done()
				probe, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
				defer cancel()
				a, err := rc.Agents.Dial(probe, rc.Cluster.Ref(), name)
				if err != nil {
					unavailable[i] = true
					return
				}
				status, err := a.Status(probe)
				if err != nil {
					unavailable[i] = true
					return
				}
				supported[i] = status.MySQLRestoreCertification
			}(i, m.Name)
		}
		wg.Wait()
		var missing []string
		for i, m := range s.Members {
			if unavailable[i] {
				return engine.ErrUnavailable
			}
			if participating[i] && !supported[i] {
				missing = append(missing, m.Name)
			}
		}
		if c == nil {
			c = &pxcRestoreCertification{Phase: "negotiated", Attempts: map[string]int{}}
			cp.GTIDCertification = c
		}
		c.Negotiated, c.Legacy = true, len(missing) != 0
		if c.Legacy {
			s.RestoreGTIDUnavailable = restoreCertificationUnavailable + " Affected members: " + strings.Join(missing, ", ")
			c.LegacyCleanupPending = s.RestoreGTIDFence == op.ID
		} else {
			s.RestoreGTIDUnavailable = ""
		}
		if err := save(rc.Cluster, s); err != nil {
			return err
		}
		if err := persistRestore(rc, op, *cp); err != nil {
			return err
		}
	}
	if c.LegacyCleanupPending {
		if err := releaseRestoreGTIDFence(ctx, rc, s, op.ID, true); err != nil {
			return err
		}
		c.LegacyCleanupPending, c.FencesOwned = false, false
		if err := persistRestore(rc, op, *cp); err != nil {
			return err
		}
	}
	return nil
}

func legacyPXCRestore(cp backupCheckpoint) bool {
	return cp.GTIDCertification != nil && cp.GTIDCertification.Negotiated && cp.GTIDCertification.Legacy
}

// Both queued restore cleanup and Sync-owned initial restore recovery consume
// this durable pending transition. The blocked state is settled only afterward.
func completeRestoreGTIDCleanup(ctx context.Context, rc *engine.Context, s *state, op *domain.Operation, cp *backupCheckpoint) error {
	if cp.GTIDCertification == nil {
		cp.GTIDCertification = &pxcRestoreCertification{}
	}
	c := cp.GTIDCertification
	if c.CleanupComplete && !c.CleanupPending {
		return nil
	}
	c.CleanupPending, c.CleanupComplete, c.Phase = true, false, "cleanup"
	cp.Step = "certify-cleanup"
	if err := persistRestore(rc, op, *cp); err != nil {
		return err
	}
	if err := releaseRestoreGTIDFence(ctx, rc, s, op.ID, true); err != nil {
		return err
	}
	c.CleanupPending, c.CleanupComplete, c.Phase = false, true, "blocked"
	c.FencesOwned, c.RecoveryCleanupPending = false, false
	cp.Step = "certify-blocked"
	return persistRestore(rc, op, *cp)
}

// Negotiating support acquires nothing. Only the post-restore client-fencing
// window owns these controls; record that intent before any hold RPC.
func acquirePXCRestoreFences(ctx context.Context, rc *engine.Context, s *state, op *domain.Operation, cp *backupCheckpoint) (bool, error) {
	if cp.GTIDCertification == nil {
		return false, engine.Invalid("restore fencing lacks negotiated ownership")
	}
	if !cp.GTIDCertification.FencesOwned {
		cp.GTIDCertification.FencesOwned = true
		if err := persistRestore(rc, op, *cp); err != nil {
			return false, err
		}
	}
	return fencePXCRestoreClients(ctx, rc, s, op.ID)
}

// Before certification begins, terminal queue cleanup must release its client
// controls without replacing the frozen native restore/reformation checkpoint.
func completeRestoreGTIDRecoveryCleanup(ctx context.Context, rc *engine.Context, s *state, op *domain.Operation, cp *backupCheckpoint) error {
	if cp.GTIDCertification == nil {
		cp.GTIDCertification = &pxcRestoreCertification{Phase: "negotiated"}
	}
	c := cp.GTIDCertification
	c.RecoveryCleanupPending, c.FencesOwned = true, true
	if err := persistRestore(rc, op, *cp); err != nil {
		return err
	}
	if err := releaseRestoreGTIDFence(ctx, rc, s, op.ID, true); err != nil {
		return err
	}
	c.RecoveryCleanupPending, c.FencesOwned, c.LegacyCleanupPending = false, false, false
	return persistRestore(rc, op, *cp)
}
