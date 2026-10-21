// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// This agent-owned receipt is outside the SST-controlled datadir. Pending is
// consumed only after a Synced member proves the same Galera state identity.
// SST is allowed to delete auto.cnf; upstream's scripts and keep-lists are intact.
type galeraIdentity struct {
	ClusterName    string
	Member         string
	FormationEpoch uint64
	StateUUID      string
	Pending        bool
}

func readGaleraIdentity(s myspec.Spec, p paths) (galeraIdentity, error) {
	var r galeraIdentity
	raw, err := os.ReadFile(filepath.Join(p.root, "sst-authorized.json"))
	if err != nil {
		return r, err
	}
	if err = json.Unmarshal(raw, &r); err != nil {
		return r, err
	}
	if r.ClusterName != s.Galera.ClusterName || r.Member != s.Member || r.FormationEpoch > s.Galera.FormationEpoch {
		return r, errors.New("foreign Galera identity or stale formation epoch")
	}
	if info, err := os.Lstat(filepath.Join(p.root, "galera-sst-start")); err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return r, errors.New("invalid supervisor SST authorization")
		}
		r.Pending = true
	} else if !os.IsNotExist(err) {
		return r, err
	}
	return r, nil
}

const galeraTransferRequiredUUID = "00000000-0000-0000-0000-000000000000"

// galeraDiskUUID returns the on-disk state UUID and, for upstream's
// transfer-required reset, the bootstrap flag it currently carries.
func galeraDiskUUID(p paths) (string, string, error) {
	name := filepath.Join(p.data, "grastate.dat")
	info, err := os.Lstat(name)
	if err != nil {
		return "", "", err
	}
	if !info.Mode().IsRegular() || info.Size() > 4096 {
		return "", "", errors.New("invalid Galera state identity")
	}
	raw, err := os.ReadFile(name)
	if err != nil {
		return "", "", err
	}
	var uuid string
	seqno, safe := "", ""
	seenSeqno, seenSafe := false, false
	for _, line := range strings.Split(string(raw), "\n") {
		k, v, ok := strings.Cut(line, ":")
		if ok && strings.TrimSpace(k) == "uuid" {
			if uuid != "" {
				return "", "", errors.New("ambiguous Galera state identity")
			}
			uuid = strings.TrimSpace(v)
		}
		if ok && strings.TrimSpace(k) == "seqno" {
			if seenSeqno {
				return "", "", errors.New("ambiguous Galera state sequence")
			}
			seenSeqno = true
			seqno = strings.TrimSpace(v)
		}
		if ok && strings.TrimSpace(k) == "safe_to_bootstrap" {
			if seenSafe {
				return "", "", errors.New("ambiguous Galera bootstrap state")
			}
			seenSafe = true
			safe = strings.TrimSpace(v)
		}
	}
	// Upstream resets exactly this tuple after an interrupted IST/SST. It
	// requests state transfer, not a new or unidentified cluster identity.
	// Bootstrap preparation of a recovered seed may only flip its flag to 1.
	if uuid == galeraTransferRequiredUUID && seqno == "-1" && (safe == "0" || safe == "1") {
		return uuid, safe, nil
	}
	if !myspec.ValidUUID(uuid) {
		return "", "", errors.New("missing Galera state identity")
	}
	return uuid, "", nil
}
func guardGaleraIdentity(s myspec.Spec, p paths) (galeraIdentity, error) {
	r, err := readGaleraIdentity(s, p)
	if err != nil {
		return r, err
	}
	if r.StateUUID != "" {
		if !myspec.ValidUUID(r.StateUUID) {
			return r, errors.New("invalid durable Galera state identity")
		}
		uuid, safe, err := galeraDiskUUID(p)
		if err != nil {
			return r, err
		}
		if uuid == galeraTransferRequiredUUID {
			// Only this member's own receipt for this cluster may retry the
			// upstream transfer-required state, and only when its last launch
			// never reached Synced. readGaleraIdentity already rejects a foreign
			// member/cluster or a receipt newer than the configured epoch; a
			// newer configured epoch is a CP reformation of the same pinned
			// history (new histories get a new cluster name and prepare-join).
			// Galera writes the same tuple when it evicts a Synced member for
			// inconsistency; that must stay an explicit recovery. Keep the
			// pinned UUID authoritative: recovery redo and the eventual Synced
			// SQL state must match it. Server-identity changes stay governed by
			// the supervisor's own launch authorization.
			if info, err := os.Lstat(filepath.Join(p.root, galeraUnsyncedMarker)); err != nil || !info.Mode().IsRegular() {
				return r, errors.New("galera state reset outside an unfinished join")
			}
			// Only an unconsumed, recovered-position-bound bootstrap of this
			// epoch may have set the flag; the launcher still verifies redo
			// against that exact position before forming the component.
			if safe == "1" {
				if !s.Galera.Bootstrap || s.Galera.StartPosition == "" {
					return r, errors.New("galera reset state carries unauthorized bootstrap permission")
				}
				if _, err := os.Stat(galeraClaimPath(s, p)); !os.IsNotExist(err) {
					return r, errors.New("galera reset state carries unauthorized bootstrap permission")
				}
			}
			return r, nil
		}
		if uuid != r.StateUUID {
			return r, errors.New("foreign Galera datadir state UUID")
		}
	}
	return r, nil
}
func authorizeGaleraStart(s myspec.Spec, p paths) error {
	r, err := readGaleraIdentity(s, p)
	if os.IsNotExist(err) {
		id, e := readIdentity(p)
		if e != nil {
			return e
		}
		if !s.Galera.Bootstrap || !id.Initialized {
			return errors.New("galera start lacks own identity receipt")
		}
		r = galeraIdentity{ClusterName: s.Galera.ClusterName, Member: s.Member, FormationEpoch: s.Galera.FormationEpoch}
	} else if err != nil {
		return err
	}
	r.Pending = true
	r.FormationEpoch = s.Galera.FormationEpoch
	return durableJSON(filepath.Join(p.root, "sst-authorized.json"), r)
}
func refreshGaleraIdentity(ctx context.Context, db *sql.DB, s myspec.Spec, p paths, status myspec.GaleraStatusResponse) error {
	r, err := readGaleraIdentity(s, p)
	if err != nil {
		return err
	}
	if !status.Eligible() || !myspec.ValidUUID(status.StateUUID) {
		return errors.New("galera identity requires Synced Primary")
	}
	if r.StateUUID != "" && r.StateUUID != status.StateUUID {
		return errors.New("galera cluster state identity changed")
	}
	id, err := readIdentity(p)
	if err != nil {
		return err
	}
	var uuid string
	if err = db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&uuid); err != nil {
		return err
	}
	if !myspec.ValidUUID(uuid) || id.ServerUUID != "" && id.ServerUUID != uuid && !r.Pending {
		return errors.New("unauthorized Galera server UUID change")
	}
	// Persist the actual-launch recovery guard before admitting formed data.
	statePath := filepath.Join(p.root, "galera-state-uuid")
	if raw, err := os.ReadFile(statePath); os.IsNotExist(err) {
		if err = durableText(statePath, status.StateUUID+"\n"); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if string(raw) != status.StateUUID+"\n" {
		return errors.New("supervisor recovery state identity mismatch")
	}
	// This launch is Synced in its pinned history: it finished joining.
	if err = os.Remove(filepath.Join(p.root, galeraUnsyncedMarker)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if !r.Pending && r.StateUUID == status.StateUUID && id.Initialized && id.ServerUUID == uuid && id.FormationEpoch == s.Galera.FormationEpoch {
		return nil
	}
	id.Initialized = true
	id.ServerUUID = uuid
	id.FormationEpoch = s.Galera.FormationEpoch
	if err = durableJSON(filepath.Join(p.root, "initialized.json"), id); err != nil {
		return err
	}
	r.StateUUID = status.StateUUID
	// Upstream SST may remove auto.cnf after mysqld selected its in-memory UUID.
	// Keep authorization until the next start materializes a stable disk UUID.
	_, autoErr := os.Lstat(filepath.Join(p.data, "auto.cnf"))
	if autoErr != nil && !os.IsNotExist(autoErr) {
		return autoErr
	}
	r.Pending = os.IsNotExist(autoErr)
	if err = durableJSON(filepath.Join(p.root, "sst-authorized.json"), r); err != nil {
		return err
	}
	if !r.Pending {
		if err = os.Remove(filepath.Join(p.root, "galera-sst-start")); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return durableText(filepath.Join(p.root, "galera-state-uuid"), status.StateUUID+"\n")
}
