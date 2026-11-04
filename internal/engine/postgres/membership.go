// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

// etcdVoter requires a successful membership RPC, not just a constructed agent
// client. A replacement's predecessor may be unreachable but still be joined.
func etcdVoter(ctx context.Context, rc *engine.Context, s *state, exclude string) (*agentapi.Client, pgspec.EtcdMemberResponse, error) {
	lastErr := engine.ErrUnavailable
	for _, m := range s.group(0).data() {
		if m.Name == exclude || m.Removing || m.EtcdLearner || m.Stage != "joined" || s.Replacement != nil && m.Name == s.Replacement.Old {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, pgspec.EtcdMemberResponse{}, err
		}
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			lastErr = err
			continue
		}
		var response pgspec.EtcdMemberResponse
		if err := a.Engine(ctx, "postgres", "etcd-member", pgspec.EtcdMemberRequest{Op: "list"}, &response); err != nil {
			lastErr = err
			continue
		}
		return a, response, nil
	}
	return nil, pgspec.EtcdMemberResponse{}, lastErr
}
func (p *Core) addLearner(ctx context.Context, rc *engine.Context, s *state, m *member) error {
	if m.Group != 0 {
		return nil
	}
	a, response, err := etcdVoter(ctx, rc, s, m.Name)
	if err != nil {
		return err
	}
	peerURL := "https://" + m.Name + ":2380"
	found := false
	for _, peer := range response.Members {
		for _, u := range peer.PeerURL {
			if u == peerURL {
				m.EtcdID = peer.ID
				found = true
			}
		}
	}
	if !found {
		if err = a.Engine(ctx, "postgres", "etcd-member", pgspec.EtcdMemberRequest{Op: "add-learner", Name: m.Name, PeerURL: peerURL}, &response); err != nil {
			return err
		}
		for _, peer := range response.Members {
			for _, u := range peer.PeerURL {
				if u == peerURL {
					m.EtcdID = peer.ID
				}
			}
		}
	}
	if m.EtcdID == "" {
		return engine.ErrUnavailable
	}
	m.EtcdLearner = true
	m.EtcdState = "existing"
	m.InitialCluster = response.InitialCluster
	s.ConfigVersion++
	return nil
}
func (p *Core) promoteLearner(ctx context.Context, rc *engine.Context, s *state, m *member) error {
	if m.Group != 0 {
		return nil
	}
	a, response, err := etcdVoter(ctx, rc, s, m.Name)
	if err != nil {
		return err
	}
	found := false
	for _, peer := range response.Members {
		match := peer.ID == m.EtcdID && m.EtcdID != ""
		for _, peerURL := range peer.PeerURL {
			match = match || peerURL == "https://"+m.Name+":2380"
		}
		if !match {
			continue
		}
		// Recover the authoritative ID from a voter even if a learner's local
		// status could not report it before this checkpoint was persisted.
		m.EtcdID = peer.ID
		found = true
		if peer.Learner {
			if err = a.Engine(ctx, "postgres", "etcd-member", pgspec.EtcdMemberRequest{Op: "promote", MemberID: m.EtcdID}, nil); err != nil {
				return err
			}
		}
	}
	if !found {
		return engine.ErrUnavailable
	}
	m.EtcdLearner = false
	s.ConfigVersion++
	return nil
}

// removeEtcdMember observes membership first so a crash after the removal RPC
// does not turn its retry into an unrecoverable member-not-found response.
func removeEtcdMember(ctx context.Context, rc *engine.Context, s *state, m *member) error {
	if m.Group != 0 {
		return nil
	}
	a, response, err := etcdVoter(ctx, rc, s, m.Name)
	if err != nil {
		return err
	}
	for _, peer := range response.Members {
		match := peer.ID == m.EtcdID && m.EtcdID != ""
		for _, peerURL := range peer.PeerURL {
			match = match || peerURL == "https://"+m.Name+":2380"
		}
		if match {
			if err := a.Engine(ctx, "postgres", "etcd-member", pgspec.EtcdMemberRequest{Op: "remove", MemberID: peer.ID}, nil); err != nil {
				return err
			}
			break
		}
	}
	m.EtcdID = ""
	return nil
}
