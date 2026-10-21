// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"crypto/rand"
	"encoding/hex"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

var pxcCatalog = []domain.VersionInfo{
	{Version: "8.4.11-11.1", Major: "8.4", Default: true, Status: "supported"},
	{Version: "8.0.46-38.1", Major: "8.0", Status: "supported"},
}

func (p *Core) versions() []domain.VersionInfo {
	if p.Name() == "pxc" {
		return pxcCatalog
	}
	return catalog
}
func (p *Core) artifactPins(version string) (string, string, error) {
	if p.Name() != "pxc" {
		return ArtifactPins(version)
	}
	for _, v := range pxcCatalog {
		if v.Version == version {
			backup := "8.4.0-7"
			if v.Major == "8.0" {
				backup = "8.0.35-36"
			}
			return "percona-xtradb-cluster-" + version, "percona-xtrabackup-" + backup, nil
		}
	}
	return "", "", engine.Invalid("unsupported PXC version")
}
func clustered(topology string) bool { return topology == "group" || topology == "cluster" }

// Galera's gmcast handshake stores cluster names in String<32>. Its name is
// independent of the UUID supplied by wsrep recovered-position observations.
func (p *Core) newReplicationIdentity() (string, error) {
	if p.Name() != "pxc" {
		return newUUID()
	}
	var identity [16]byte
	if _, err := rand.Read(identity[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(identity[:]), nil
}
