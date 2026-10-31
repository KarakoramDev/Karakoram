// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

// pxcJoinAllowed admits at most one not-yet-Synced data member. Joined is the
// controller's current-formation Synced launch receipt, not GTID certification.
// Formation/reformation must invalidate followers' old joined receipts after
// confirming them stopped, and record fresh Synced observations before moving
// this barrier. No agent RPC belongs in MemberConfig: an agent may be waiting
// synchronously for the configuration being rendered.
func pxcJoinAllowed(s *state, m *member) bool {
	if s.Topology != "cluster" || m.Role != "data" {
		return true
	}
	if m.Offline {
		return false
	}
	// FencesOwned is an acquisition intent, not fence publication. Empty
	// joiners need both the published owner and the pinned seed donor. A new
	// bootstrap also needs that fence, even when its seed was joined before.
	certification := s.Formation.GTIDCertification
	if certification != nil && !certification.Legacy &&
		(m.Stage != "joined" && certification.EmptyMembers[m.Name] ||
			s.Formation.State == "bootstrapping" && m.Name == s.Formation.Seed) {
		proof := certification.Checkpoint.GTIDCertification
		if certification.Owner == "" || proof == nil || !proof.FencesOwned {
			return false
		}
		if certification.JoinOnly {
			// Scale-up does not stop existing writers. Fence only the new
			// member; birth/bootstrap can never use this narrower authority.
			if s.Formation.State == "bootstrapping" || !m.GaleraJoinFence {
				return false
			}
		} else if s.RestoreGTIDFence != certification.Owner {
			return false
		}
		if m.Name != s.Formation.Seed && m.GaleraSSTDonor != s.Formation.Seed {
			return false
		}
	}
	switch s.Formation.State {
	case "unformed":
		return false
	case "bootstrapping":
		return m.Name == s.Formation.Seed
	}
	// Existing members must keep running during ordinary restarts and graceful
	// retirement; their explicit Offline hold, not a join queue, stops them.
	if m.Stage == "joined" {
		return true
	}
	if s.Formation.State != "formed" || m.Stage != "deployed" || m.Removing {
		return false
	}
	if m.Name == s.Formation.Seed {
		return true
	}
	for _, peer := range s.Members {
		if peer.Role != "data" || peer.Name == s.Formation.Seed || peer.Stage == "joined" || peer.Offline || peer.Removing {
			continue
		}
		// Earlier identities reserve their place even while deployment is still
		// pending, preventing out-of-order platform completion from releasing
		// two SST joiners or pre-empting one already being admitted.
		if peer.Index < m.Index || peer.Index == m.Index && peer.Name < m.Name {
			return false
		}
	}
	return true
}

// Desired fence removal precedes native acknowledgement. Do not publish a
// joining member to clients until its durable release proof also completes.
func pxcJoinRoutingFenced(s *state, m *member) bool {
	if m.GaleraJoinFence {
		return true
	}
	certification := s.Formation.GTIDCertification
	if certification == nil || certification.Legacy || !certification.JoinOnly || !certification.FencedMembers[m.Name] {
		return false
	}
	proof := certification.Checkpoint.GTIDCertification
	return certification.Blocked != "" || proof == nil || !proof.Certified || proof.FencesOwned
}
