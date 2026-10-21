// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package myspec

import (
	"encoding/pem"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const (
	EnginePXC                = "pxc"
	TopologyCluster          = "cluster"
	PortGalera               = 4567
	PortGaleraIST            = 4568
	PortGaleraSST            = 4444
	ActionGaleraStatus       = "galera-status"
	ActionWSRepRecover       = "wsrep-recover"
	ActionGaleraBootstrap    = "galera-bootstrap"
	ActionGaleraForcePrimary = "galera-force-primary"
	ActionGaleraPrepareJoin  = "galera-prepare-join"
	// This token acknowledges that all absent members have been fenced. The
	// control plane must expose this operation only to an explicit operator.
	GaleraForcePrimaryConfirmation = "all-absent-members-fenced"
)

var galeraVersion = regexp.MustCompile(`^8\.(0|4)\.[0-9]+-[0-9]+\.[0-9]+$`)
var galeraClusterName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,31}$`)

// ValidGaleraClusterName enforces gmcast's fixed String<32> handshake field.
// This cluster identity is distinct from the recovered Galera state UUID.
func ValidGaleraClusterName(name string) bool {
	return galeraClusterName.MatchString(name)
}

type Galera struct {
	ClusterName    string `json:"clusterName"`
	FormationEpoch uint64 `json:"formationEpoch"`
	Peers          []Peer `json:"peers"`
	GaleraPort     int    `json:"galeraPort"`
	ISTPort        int    `json:"istPort"`
	SSTPort        int    `json:"sstPort"`
	Bootstrap      bool   `json:"bootstrap"`
	StartPosition  string `json:"startPosition,omitempty"`
	GCacheSize     string `json:"gcacheSize"`
	// TrafficCert is identical on all members and separate from client SQL TLS.
	TrafficCert  *Certificate `json:"trafficCert"`
	TrafficCAPEM string       `json:"trafficCAPEM"`
}

type GaleraStatusResponse struct {
	ClusterStatus     string `json:"clusterStatus"`
	LocalStateComment string `json:"localStateComment"`
	Ready             bool   `json:"ready"`
	Connected         bool   `json:"connected"`
	ClusterSize       int    `json:"clusterSize"`
	StateUUID         string `json:"stateUUID"`
	LastCommitted     int64  `json:"lastCommitted"`
	GTIDExecuted      string `json:"gtidExecuted"`
}

func (s GaleraStatusResponse) Eligible() bool {
	return s.ClusterStatus == "Primary" && s.LocalStateComment == "Synced" && s.Ready && s.Connected
}

type WSRepRecoverResponse struct {
	UUID    string `json:"uuid"`
	Seqno   int64  `json:"seqno"`
	Stopped bool   `json:"stopped"`
}
type GaleraBootstrapRequest struct {
	ClusterName    string `json:"clusterName"`
	FormationEpoch uint64 `json:"formationEpoch"`
	StartPosition  string `json:"startPosition"`
}
type GaleraForcePrimaryRequest struct {
	ClusterName    string `json:"clusterName"`
	FormationEpoch uint64 `json:"formationEpoch"`
	Confirmation   string `json:"confirmation"`
}

func ParseGaleraPosition(p string) (string, int64, error) {
	u, n, ok := strings.Cut(p, ":")
	seq, err := strconv.ParseInt(n, 10, 64)
	if !ok || !ValidUUID(u) || err != nil || seq < 0 {
		return "", 0, fmt.Errorf("invalid recovered Galera position")
	}
	return u, seq, nil
}
func (g Galera) Validate() error {
	if !ValidGaleraClusterName(g.ClusterName) || g.FormationEpoch == 0 || len(g.Peers) == 0 {
		return fmt.Errorf("invalid Galera cluster identity")
	}
	ca, trailing := pem.Decode([]byte(g.TrafficCAPEM))
	if ca == nil || ca.Type != "CERTIFICATE" || strings.TrimSpace(string(trailing)) != "" || strings.TrimSpace(g.TrafficCAPEM) != strings.TrimSpace(string(pem.EncodeToMemory(ca))) {
		return fmt.Errorf("exactly one dedicated Galera traffic CA certificate required")
	}
	if g.TrafficCert == nil {
		return fmt.Errorf("shared encrypted Galera traffic certificate required")
	}
	if err := g.TrafficCert.Validate(); err != nil {
		return fmt.Errorf("invalid Galera traffic certificate: %w", err)
	}
	ports := map[int]bool{}
	for _, p := range []int{g.GaleraPort, g.ISTPort, g.SSTPort} {
		if p < 1 || p > 65535 || ports[p] {
			return fmt.Errorf("invalid Galera ports")
		}
		ports[p] = true
	}
	seen := map[string]bool{}
	for _, p := range g.Peers {
		if err := p.Validate(); err != nil {
			return err
		}
		if seen[p.Name] {
			return fmt.Errorf("duplicate Galera peer")
		}
		seen[p.Name] = true
	}
	if g.StartPosition != "" {
		if _, _, err := ParseGaleraPosition(g.StartPosition); err != nil {
			return err
		}
	}
	if !galeraCacheSize(g.GCacheSize) {
		return fmt.Errorf("invalid Galera cache size")
	}
	return nil
}
func galeraCacheSize(s string) bool {
	if len(s) < 2 || !strings.ContainsRune("KMG", rune(s[len(s)-1])) {
		return false
	}
	n, err := strconv.ParseUint(s[:len(s)-1], 10, 64)
	return err == nil && n > 0
}
