// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

var pxcCatalog = []domain.VersionInfo{
	{Version: "8.4.11-12.1", Major: "8.4", Default: true, Status: "supported"},
	{Version: "8.4.11-11.1", Major: "8.4", Status: "deprecated"},
	{Version: "8.0.46-38.1", Major: "8.0", Status: "deprecated"},
}

// Withdrawn entries stay in the catalog and artifact lock for existing member
// identity. Only fresh deployments substitute the available upstream release.
var withdrawnPXC = map[string]string{"8.4.11-11.1": "8.4.11-12.1"}

func deployVersion(engineName, version string) string {
	if engineName == "pxc" {
		if replacement, ok := withdrawnPXC[version]; ok {
			return replacement
		}
	}
	return version
}

func versionSatisfies(engineName, memberVersion, specVersion string) bool {
	return memberVersion == specVersion || memberVersion == deployVersion(engineName, specVersion)
}

func (p *Core) setDeployVersion(ctx context.Context, rc *engine.Context, m *member, version string) error {
	if m.Role != "data" {
		m.Version = version
		return nil
	}
	target := deployVersion(p.Name(), version)
	withdrawn := m.Version
	if deployVersion(p.Name(), withdrawn) == withdrawn {
		withdrawn = version
	}
	if m.Version != target && deployVersion(p.Name(), withdrawn) != withdrawn && rc.Events != nil {
		if err := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Type: "Normal", Reason: "WithdrawnVersionReplaced", Message: fmt.Sprintf("Member %s moves from %s to %s because Percona withdrew the %s artifact", m.Name, withdrawn, target, withdrawn)}); err != nil {
			return err
		}
	}
	m.Version = target
	return nil
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
