// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// An explicit restore/reformation action, never an automatic foreign-data bypass.
// The quarantine and durable intent make interruption resumable without erasing
// data that has already joined the new epoch.
func (e *Runner) galeraPrepareJoin(n *agent.Node, req myspec.GaleraBootstrapRequest) error {
	s, p := e.state()
	if s.Galera == nil || s.Galera.Bootstrap || req.ClusterName != s.Galera.ClusterName || req.FormationEpoch != s.Galera.FormationEpoch || req.StartPosition != "" || s.Galera.StartPosition != "" {
		return errors.New("SST reset intent mismatch")
	}
	c := n.Config()
	states, _ := n.Snapshot()
	if states["mysqld"].PID != 0 || !(n.Stopped("mysqld") || c.DesiredProcesses != nil && !slices.Contains(c.DesiredProcesses, "mysqld")) {
		return errors.New("SST reset requires CP-intended stop")
	}
	return prepareGaleraJoin(s, p)
}
func prepareGaleraJoin(s myspec.Spec, p paths) error {
	name := filepath.Join(p.root, fmt.Sprintf("galera-join-%d.json", s.Galera.FormationEpoch))
	var intent struct {
		ClusterName string
		Member      string
		Done        bool
	}
	raw, err := os.ReadFile(name)
	if os.IsNotExist(err) {
		var old galeraIdentity
		raw, err = os.ReadFile(filepath.Join(p.root, "sst-authorized.json"))
		if err != nil {
			return err
		}
		if err = json.Unmarshal(raw, &old); err != nil {
			return err
		}
		if old.Member != s.Member || old.FormationEpoch >= s.Galera.FormationEpoch {
			return errors.New("SST reset requires a newer authorized epoch")
		}
		prior := s
		prior.Galera = &myspec.Galera{ClusterName: old.ClusterName, FormationEpoch: old.FormationEpoch}
		if _, err = guardInitialization(prior, p); err != nil {
			return err
		}
		intent.ClusterName, intent.Member = s.Galera.ClusterName, s.Member
		if err = durableJSON(name, intent); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if err = json.Unmarshal(raw, &intent); err != nil {
		return err
	}
	if intent.ClusterName != s.Galera.ClusterName || intent.Member != s.Member {
		return errors.New("foreign SST reset receipt")
	}
	if intent.Done {
		return nil
	}
	old := fmt.Sprintf("%s.galera-%d", p.data, s.Galera.FormationEpoch)
	if _, err = os.Lstat(old); os.IsNotExist(err) {
		if err = os.Rename(p.data, old); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if err = privateDir(p.data); err != nil {
		return err
	}
	parent, err := os.Open(filepath.Dir(p.data))
	if err != nil {
		return err
	}
	syncErr := parent.Sync()
	closeErr := parent.Close()
	if syncErr != nil {
		return syncErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err = clearGaleraStartState(p); err != nil {
		return err
	}
	if err = durableJSON(filepath.Join(p.root, "initialized.json"), myspec.DataIdentity{}); err != nil {
		return err
	}
	if err = durableJSON(filepath.Join(p.root, "sst-authorized.json"), galeraIdentity{ClusterName: s.Galera.ClusterName, Member: s.Member, FormationEpoch: s.Galera.FormationEpoch, Pending: true}); err != nil {
		return err
	}
	intent.Done = true
	if err = durableJSON(name, intent); err != nil {
		return err
	}
	return os.RemoveAll(old)
}

func clearGaleraStartState(p paths) error {
	for _, file := range []string{"galera-state-uuid", "galera-sst-start"} {
		if err := os.Remove(filepath.Join(p.root, file)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
