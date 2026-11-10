// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

const maxConfigPullCertificates = 4096

// Server request-local copies share the complete synchronization domain.
// Never copy a mutex separately from the maps and gates it protects.
type configPullState struct {
	configPullMu      sync.Mutex
	configPulls       map[configPullKey]configPullCertificate
	configRenderGates map[configPullKey]*configRenderGate
}

type configPullKey struct {
	cluster      domain.ClusterRef
	member       string
	capabilities [32]byte
}

type configPullCertificate struct {
	inputs, revision, changeAck string
}

func memberConfigKey(rc *engine.Context, member string) configPullKey {
	return configPullKey{rc.Cluster.Ref(), member, sha256.Sum256([]byte(strings.Join(rc.AgentConfigCapabilities, "\x00")))}
}

func (s *Server) conditionalConfigCertificate(key configPullKey, inputs string, eligible bool) configPullCertificate {
	if !eligible || inputs == "" {
		return configPullCertificate{}
	}
	s.configPullMu.Lock()
	defer s.configPullMu.Unlock()
	certificate, ok := s.configPulls[key]
	if !ok || certificate.inputs != inputs {
		return configPullCertificate{}
	}
	return certificate
}

// issueConfigRevision binds the actual freshly rendered payload, not just its
// input fingerprint. Empty inputs deliberately issue no conditional certificate:
// the agent still sends its payload token, but uncertain evidence stays fresh.
// The caller holds this key's render gate through rendering and publication.
func (s *Server) issueConfigRevision(key configPullKey, inputs, epoch, changeAck string, cfg *agentapi.MemberConfig) error {
	cfg.PullRevision, cfg.ChangeRevision, cfg.ContentRevision = "", "", ""
	digest := sha256.New()
	if err := json.NewEncoder(digest).Encode(struct {
		Epoch, Inputs string
		Cluster       domain.ClusterRef
		Member        string
		Capabilities  [32]byte
		Config        *agentapi.MemberConfig
	}{epoch, inputs, key.cluster, key.member, key.capabilities, cfg}); err != nil {
		return err
	}
	var sum [32]byte
	digest.Sum(sum[:0])
	revision := hex.EncodeToString(sum[:])
	s.configPullMu.Lock()
	if s.configPulls == nil {
		s.configPulls = make(map[configPullKey]configPullCertificate)
	}
	if _, exists := s.configPulls[key]; !exists && len(s.configPulls) >= maxConfigPullCertificates {
		// Cache eviction only causes a fresh render; it never extends a proof.
		for previous := range s.configPulls {
			delete(s.configPulls, previous)
			break
		}
	}
	s.configPulls[key] = configPullCertificate{inputs: inputs, revision: revision, changeAck: changeAck}
	s.configPullMu.Unlock()
	cfg.PullRevision = revision
	return nil
}
