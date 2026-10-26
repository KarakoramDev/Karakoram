// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"os"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

// Return only fixed vocabulary, never native command output, paths or secrets.
func upgradeFailureDiagnostic(p paths, request pgspec.UpgradeRequest) string {
	phase := "unknown"
	targetOwned := false
	name, err := receiptPath(p, "upgrade", request.Receipt)
	if err == nil {
		data, err := os.ReadFile(name)
		if os.IsNotExist(err) {
			phase = "planned"
		} else if err == nil {
			var receipt upgradeReceipt
			if json.Unmarshal(data, &receipt) == nil && receipt.Request == request {
				targetOwned = receipt.TargetCreated
				switch receipt.Phase {
				case "planned", "initialized", "checked", "linking", "linked", "old-retired", "done":
					phase = receipt.Phase
				}
			}
		}
	}
	if phase == "planned" && !targetOwned {
		if _, err := os.Lstat(pathsForUpgrade(p, request).target); err == nil {
			return "PostgreSQL major upgrade recovery required at durable checkpoint planned: the target directory exists without a durable ownership receipt. Preserve both directories and the receipt; do not erase or adopt the unverified target. Inspect the offline state, or restore a validated pre-upgrade backup into a separate cluster before an explicit cutover."
		}
	}
	if phase == "linking" {
		return "PostgreSQL major upgrade recovery required at durable checkpoint linking: the link command may have modified both data directories. Keep PostgreSQL and Patroni stopped; preserve both directories and the upgrade receipt. Restore a validated pre-upgrade backup into a separate cluster, validate it, and cut over explicitly. Do not clear the receipt or retry the link."
	}
	if phase == "unknown" {
		return "PostgreSQL major upgrade recovery required: the durable receipt is unreadable or does not match. Keep both data directories stopped and preserve the receipt; inspect the offline upgrade state before recovery."
	}
	return "PostgreSQL major upgrade action failed at durable checkpoint " + phase + ". Preserve both data directories and the receipt; repair the member dependency and retry the same upgrade. Check native pg_upgrade logs in the protected upgrade work directory; do not start retired data."
}
