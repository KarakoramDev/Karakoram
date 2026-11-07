// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"sort"
	"time"
)

func galeraStatus(ctx context.Context, rc *engine.Context, name string) (myspec.GaleraStatusResponse, error) {
	st, _, err := galeraStatusAt(ctx, rc, name)
	return st, err
}

func galeraStatusAt(ctx context.Context, rc *engine.Context, name string) (myspec.GaleraStatusResponse, time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	var st myspec.GaleraStatusResponse
	at, err := mysqlReadActionAt(ctx, rc, name, myspec.ActionGaleraStatus, struct{}{}, &st)
	return st, at, err
}

// ProxySQL, not wsrep's multi-primary role, owns the client writer designation.
func observeGalera(ctx context.Context, rc *engine.Context, name string, out *myspec.StatusResponse) error {
	// The agent's mysql/status already verifies Primary/Synced/ready/connected
	// before assigning PRIMARY. Do not repeat a Galera SQL sweep for readiness.
	online := out.Online(true)
	out.MemberState, out.MemberRole = "OFFLINE", "SECONDARY"
	if !online {
		return nil
	}
	out.MemberState = "ONLINE"
	s, err := load(rc.Cluster)
	if err != nil {
		return err
	}
	writer := ""
	deployedProxy := false
	for _, m := range s.Members {
		if m.Role == "proxy" && (m.Stage == "deployed" || m.Stage == "joined") {
			deployedProxy = true
			var proxy myspec.ProxyStatusResponse
			if err := mysqlAction(ctx, rc, m.Name, myspec.ActionProxyStatus, struct{}{}, &proxy); err != nil {
				return nil // Native readiness is independent of router availability.
			}
			candidate := ""
			for _, b := range proxy.Backends {
				if b.Hostgroup == myspec.ProxyWriterGroup && b.Status == "ONLINE" {
					if candidate != "" && candidate != b.Host {
						return nil // Ambiguous routing never designates a writer.
					}
					candidate = b.Host
				}
			}
			if candidate == "" || writer != "" && writer != candidate {
				return nil
			}
			writer = candidate
		}
	}
	if !deployedProxy {
		writer = s.Formation.Seed
	}
	if writer == name {
		out.MemberRole = "PRIMARY"
	}
	return nil
}

func selectGaleraPosition(observations map[string]myspec.WSRepRecoverResponse) (string, string, error) {
	names := make([]string, 0, len(observations))
	for name := range observations {
		names = append(names, name)
	}
	sort.Strings(names)
	seed, uuid := "", ""
	var highest int64 = -1
	for _, name := range names {
		o := observations[name]
		if !o.Stopped || !myspec.ValidUUID(o.UUID) || o.Seqno < 0 {
			return "", "", fmt.Errorf("every stopped member must provide a valid recovered Galera position")
		}
		if uuid != "" && uuid != o.UUID {
			return "", "", fmt.Errorf("recovered Galera state UUIDs disagree")
		}
		uuid = o.UUID
		if o.Seqno > highest {
			seed, highest = name, o.Seqno
		}
	}
	if seed == "" {
		return "", "", fmt.Errorf("no recovered Galera positions")
	}
	return seed, fmt.Sprintf("%s:%d", uuid, highest), nil
}

// checkpointGaleraFormation observes native progress without launching a member.
// Keep this independent of serving deployments and run it before admission expiry
// or fence cleanup: a Synced member must not lose its launch receipt on reload.
func checkpointGaleraFormation(ctx context.Context, rc *engine.Context, s *state) bool {
	f := &s.Formation
	if f.Reformation != nil && f.Reformation.State == "observing" {
		return false
	}
	if f.State == "bootstrapping" {
		seed := s.find(f.Seed)
		if seed == nil {
			return false
		}
		var st myspec.StatusResponse
		err := mysqlAction(ctx, rc, seed.Name, myspec.ActionStatus, struct{}{}, &st)
		native, nativeErr := galeraStatus(ctx, rc, seed.Name)
		if err != nil || nativeErr != nil || !native.Eligible() || st.DataIdentity.FormationEpoch < f.FormationEpoch {
			return false
		}
		seed.Initialized, seed.ServerUUID, seed.Stage = true, st.ServerUUID, "joined"
		f.State, f.Blocked, f.StartPosition = "formed", "", ""
		for _, m := range s.data() {
			m.Offline = false
		}
		if f.Reformation != nil {
			f.Reformation.State = "formed"
		}
		publishRevision(s)
		return true
	}
	changed := false
	if f.State == "formed" {
		for _, m := range s.data() {
			if m.Offline || m.Removing || m.ConfirmedVolumeLost || m.Stage != "deployed" {
				continue
			}
			st, err := live(ctx, rc, m.Name)
			native, nativeErr := galeraStatus(ctx, rc, m.Name)
			if err == nil && nativeErr == nil && st.MemberState == "ONLINE" && native.Eligible() {
				m.Stage, m.Initialized, m.ServerUUID = "joined", true, st.ServerUUID
				changed = true
			}
		}
	}
	if changed {
		publishRevision(s)
	}
	return changed
}

func (p *Core) reconcileGaleraFormation(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	f := &s.Formation
	if checkpointGaleraFormation(ctx, rc, s) {
		return true, nil
	}
	if r := f.Reformation; r != nil && r.State == "observing" {
		// Preserve configuration/reload failures as reconcile errors, before
		// starting native recovery on any member.
		for _, m := range s.data() {
			applied, err := configApplied(ctx, rc, m)
			if err != nil || !applied {
				r.Blocked = "Cannot fence every stopped member"
				return true, err
			}
		}
		observations, blocked := collectOfflineRecovery(ctx, rc, s.data(), func(probe context.Context, m *member) (myspec.WSRepRecoverResponse, error) {
			var out myspec.WSRepRecoverResponse
			a, err := rc.Agents.Dial(probe, rc.Cluster.Ref(), m.Name)
			if err != nil {
				return out, err
			}
			status, err := a.Status(probe)
			if err != nil {
				return out, err
			}
			if status.Processes["mysqld"].PID != 0 {
				return out, errOfflineNotStopped
			}
			err = a.Engine(probe, "mysql", myspec.ActionWSRepRecover, struct{}{}, &out)
			return out, err
		})
		r.GaleraObservations = observations
		if blocked != "" {
			r.Blocked = blocked
			return true, nil
		}
		seed, position, err := selectGaleraPosition(observations)
		if err != nil {
			r.Blocked = err.Error()
			return true, nil
		}
		f.Seed, f.StartPosition, f.State = seed, position, "bootstrapping"
		f.FormationEpoch++
		r.State, r.Blocked = "bootstrapping", ""
		for _, m := range s.data() {
			m.Offline = m.Name != seed
			// Recovered followers are confirmed stopped above; historical joined
			// receipts cannot admit several new-epoch joiners simultaneously.
			if m.Name != seed {
				m.Stage = "deployed"
			}
		}
		publishRevision(s)
		return true, nil
	}
	if f.State == "unformed" {
		for _, m := range s.data() {
			if m.Stage != "deployed" && m.Stage != "joined" {
				return false, nil
			}
		}
		seed := s.find(f.Seed)
		if seed == nil || seed.Initialized {
			return true, engine.Invalid("initial Galera bootstrap seed is not fresh")
		}
		f.State = "bootstrapping"
		publishRevision(s)
		return true, nil
	}
	if f.State == "bootstrapping" {
		seed := s.find(f.Seed)
		if seed == nil {
			return true, engine.ErrNotFound
		}
		applied, err := configApplied(ctx, rc, seed)
		if err != nil || !applied {
			return true, err
		}
		if !pxcJoinAllowed(s, seed) {
			return true, nil
		}
		// The native route durably claims each epoch before starting mysqld. Replay
		// observes that receipt; it must never generate another bootstrap epoch.
		err = mysqlAction(ctx, rc, seed.Name, myspec.ActionGaleraBootstrap, myspec.GaleraBootstrapRequest{ClusterName: f.GroupName, FormationEpoch: f.FormationEpoch, StartPosition: f.StartPosition}, nil)
		return true, err
	}
	// Joined members restart/rejoin through Galera IST/SST, never GR clone/start.
	if f.State == "formed" {
		for _, m := range s.data() {
			if m.Offline || m.Removing || m.ConfirmedVolumeLost || m.Stage != "deployed" && m.Stage != "joined" {
				continue
			}
			st, err := live(ctx, rc, m.Name)
			if err == nil && st.MemberState == "ONLINE" {
				continue
			}
			applied, err := configApplied(ctx, rc, m)
			if err != nil || !applied {
				return true, err
			}
			if !pxcJoinAllowed(s, m) {
				continue
			}
			a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
			if err != nil {
				return true, err
			}
			// Release a durable stop hold left by restore/recovery only after the
			// selected seed has formed; ordinary startup rejoins by IST or SST.
			return true, a.Process(ctx, agentapi.ProcessAction{Process: "mysqld", Action: "start", RequestID: fmt.Sprintf("galera-join/%s/%d/%s", s.UID, f.FormationEpoch, m.Name)})
		}
	}
	return false, nil
}

func galeraStopped(ctx context.Context, rc *engine.Context, name string) bool {
	status, _, err := mysqlAgentStatus(ctx, rc, name)
	if err != nil {
		return false
	}
	process, exists := status.Processes["mysqld"]
	return exists && process.PID == 0
}
