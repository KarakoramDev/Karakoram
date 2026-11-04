//go:build !linux

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import "errors"

// Existing non-Linux launches remain supported. The Linux native cleanup proof
// requires WNOWAIT and /proc; do not substitute a numeric-PGID kill after reap.
const ownedGroupAnchors = false

func observeChildExit(int) error {
	return errors.New("owned process-group exit observation requires Linux")
}

func awaitOwnedGroupExit(int) error {
	return errors.New("owned process-group death confirmation requires Linux")
}
