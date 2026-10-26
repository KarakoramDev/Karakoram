// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

const missingDataMessage = "PostgreSQL data is missing from a previously initialized member; restore a validated backup into this cluster; automatic empty initialization is disabled"

func initializationRecorded(p paths) (bool, error) {
	_, err := os.Stat(filepath.Join(p.root, "initialized.json"))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// The receipt lives outside PGDATA. It is also written before running initdb:
// an interrupted first initialization must not authorize a second identity.
func recordInitialization(p paths) error {
	recorded, err := initializationRecorded(p)
	if err != nil || recorded {
		return err
	}
	return durableJSON(filepath.Join(p.root, "initialized.json"), true)
}

// Patroni can restart using cached YAML without another Runner.Apply. Consume
// authorization at its native initdb execution boundary, not at post_bootstrap.
// Keep this attempt receipt separate from initialized.json: that completion
// receipt removes the bootstrap stanza on later pulls, including while initdb
// may still be running. A failed or interrupted attempt is never retried empty.
func renderInitDBGuard(s pgspec.Spec, p paths) string {
	const header = "#!/bin/sh\nset -eu\numask 077\n"
	refuse := "printf '%s\\n' " + shellQuote(missingDataMessage) + " >&2\nexit 1\n"
	if s.Bootstrap == nil || s.Bootstrap.Method != "initdb" || !s.Bootstrap.AllowInitDB || s.Bootstrap.SeedMember != "" && s.Bootstrap.SeedMember != s.Member {
		return header + refuse
	}
	initialized := shellQuote(filepath.Join(p.root, "initialized.json"))
	attempt := shellQuote(filepath.Join(p.root, "initdb-attempt.json"))
	return header +
		"if [ -e " + initialized + " ] || [ -L " + initialized + " ]; then\n" + refuse + "fi\n" +
		// Noclobber creates the receipt exclusively, including across competing
		// native invocations. Never remove it on command or fsync failure.
		"if ! (set -C; printf 'true\\n' > " + attempt + "); then\n" + refuse + "fi\n" +
		"sync " + attempt + "\nsync " + shellQuote(p.root) + "\n" +
		"exec " + shellQuote(filepath.Join(s.BinDir, "initdb")) + " \"$@\"\n"
}

func dataRecoveryRequired(s pgspec.Spec, p paths) bool {
	if s.Role != pgspec.RoleData || s.Topology != pgspec.TopologySingle {
		return false
	}
	if _, err := os.Stat(filepath.Join(p.pgdata, "PG_VERSION")); !os.IsNotExist(err) {
		return false
	}
	if s.Bootstrap != nil && s.Bootstrap.Method == "restore" {
		raw, err := os.ReadFile(filepath.Join(p.root, "bootstrap-generation.json"))
		var generation *int
		return err == nil && json.Unmarshal(raw, &generation) == nil && generation != nil && *generation == s.Bootstrap.Generation
	}
	recorded, err := initializationRecorded(p)
	return err != nil || recorded || s.Bootstrap == nil || !s.Bootstrap.AllowInitDB
}

func guardInitialization(s pgspec.Spec, p paths) error {
	recorded, err := initializationRecorded(p)
	if err != nil {
		return err
	}
	if recorded || s.Bootstrap == nil || !s.Bootstrap.AllowInitDB {
		return errors.New(missingDataMessage)
	}
	return recordInitialization(p)
}

func dataMissing(s pgspec.Spec, p paths) bool {
	if s.Role != pgspec.RoleData {
		return false
	}
	_, err := os.Stat(filepath.Join(p.pgdata, "PG_VERSION"))
	return os.IsNotExist(err)
}
