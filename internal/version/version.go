// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package version exposes build metadata shared by the server and CLI.
package version

import "fmt"

// Version is set by the release build's linker flags.
var Version = "dev"

// Commit is the source revision set by the release build's linker flags.
var Commit = "unknown"

// BuildDate is the UTC build timestamp set by the release build's linker flags.
var BuildDate = "unknown"

const unknownMetadata = "unknown (built without git metadata)"

// VersionLabel makes an unversioned source build explicit to API/UI consumers.
func VersionLabel() string {
	if Version == "" || Version == "unknown" || (Version == "dev" && (Commit == "" || Commit == "unknown")) {
		return unknownMetadata
	}
	return Version
}

// CommitLabel distinguishes absent provenance from a deployed source revision.
func CommitLabel() string {
	if Commit == "" || Commit == "unknown" {
		return unknownMetadata
	}
	return Commit
}

// String returns a human-readable version with source and build provenance.
func String() string {
	return fmt.Sprintf("%s (commit %s, built %s)", VersionLabel(), CommitLabel(), BuildDate)
}
