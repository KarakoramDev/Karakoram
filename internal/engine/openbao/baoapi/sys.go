// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package baoapi

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// SealStatus is GET sys/seal-status (unauthenticated).
type SealStatus struct {
	Type         string `json:"type"`
	Initialized  bool   `json:"initialized"`
	Sealed       bool   `json:"sealed"`
	T            int    `json:"t"`
	N            int    `json:"n"`
	Progress     int    `json:"progress"`
	Nonce        string `json:"nonce"`
	Version      string `json:"version"`
	ClusterName  string `json:"cluster_name"`
	ClusterID    string `json:"cluster_id"`
	RecoverySeal bool   `json:"recovery_seal"`
	StorageType  string `json:"storage_type"`
}

// SealStatus reads the seal state.
func (c *Client) SealStatus(ctx context.Context) (SealStatus, error) {
	var s SealStatus
	err := c.Do(ctx, http.MethodGet, "sys/seal-status", nil, &s)
	return s, err
}

// Health is GET sys/health, requested so that every state answers 200.
type Health struct {
	Initialized bool   `json:"initialized"`
	Sealed      bool   `json:"sealed"`
	Standby     bool   `json:"standby"`
	Version     string `json:"version"`
	ClusterName string `json:"cluster_name"`
	ClusterID   string `json:"cluster_id"`
}

// Health reads the health summary without status-code signalling.
func (c *Client) Health(ctx context.Context) (Health, error) {
	var h Health
	err := c.Do(ctx, http.MethodGet, "sys/health?standbycode=200&sealedcode=200&uninitcode=200&perfstandbyok=true", nil, &h)
	return h, err
}

// Leader is GET sys/leader (unauthenticated).
type Leader struct {
	HAEnabled            bool   `json:"ha_enabled"`
	IsSelf               bool   `json:"is_self"`
	LeaderAddress        string `json:"leader_address"`
	LeaderClusterAddress string `json:"leader_cluster_address"`
	RaftCommittedIndex   uint64 `json:"raft_committed_index"`
	RaftAppliedIndex     uint64 `json:"raft_applied_index"`
}

// Leader reads the HA leader view of this server.
func (c *Client) Leader(ctx context.Context) (Leader, error) {
	var l Leader
	err := c.Do(ctx, http.MethodGet, "sys/leader", nil, &l)
	return l, err
}

// InitRequest is PUT sys/init.
type InitRequest struct {
	SecretShares      int `json:"secret_shares,omitempty"`
	SecretThreshold   int `json:"secret_threshold,omitempty"`
	RecoveryShares    int `json:"recovery_shares,omitempty"`
	RecoveryThreshold int `json:"recovery_threshold,omitempty"`
}

// Init initializes the server and returns the raw sys/init response body,
// which callers must treat as secret.
func (c *Client) Init(ctx context.Context, req InitRequest) ([]byte, error) {
	body, err := c.Stream(ctx, http.MethodPut, "sys/init", jsonReader(req), "application/json")
	if err != nil {
		return nil, err
	}
	defer drain(body)
	data, err := io.ReadAll(io.LimitReader(body, 64<<10+1))
	if err != nil {
		clear(data)
		return nil, fmt.Errorf("openbao PUT /v1/sys/init: read response: %w", err)
	}
	if len(data) > 64<<10 {
		clear(data)
		return nil, errors.New("openbao PUT /v1/sys/init: response too large")
	}
	return data, nil
}

// Unseal submits one unseal (or, never for auto-unseal, recovery) share.
func (c *Client) Unseal(ctx context.Context, key string) (SealStatus, error) {
	var s SealStatus
	err := c.Do(ctx, http.MethodPut, "sys/unseal", map[string]any{"key": key}, &s)
	return s, err
}

// ErrInsufficientShares means the native threshold exceeds supplied material
// and the attempt did not complete. The incomplete attempt has been reset.
var ErrInsufficientShares = errors.New("openbao native threshold exceeds supplied shares")

// UnsealWith submits shares until the server reports unsealed or the shares
// are exhausted. It resets a partial foreign unseal attempt first.
func (c *Client) UnsealWith(ctx context.Context, keys []string) (s SealStatus, err error) {
	s, err = c.SealStatus(ctx)
	if err != nil || !s.Sealed {
		return s, err
	}
	if s.Progress > 0 {
		if err := c.Do(ctx, http.MethodPut, "sys/unseal", map[string]any{"reset": true}, &s); err != nil {
			// A reset failure must not be classified as a rejected candidate.
			return s, fmt.Errorf("openbao reset partial unseal attempt: %v", err)
		}
	}
	defer func() {
		if err != nil {
			resetCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			if resetErr := c.Do(resetCtx, http.MethodPut, "sys/unseal", map[string]any{"reset": true}, nil); resetErr != nil {
				// Do not expose a cleanup HTTP 400 as a share rejection.
				err = fmt.Errorf("openbao reset failed unseal attempt: %v", resetErr)
			}
		}
	}()
	for _, k := range keys {
		if s, err = c.Unseal(ctx, k); err != nil || !s.Sealed {
			return s, err
		}
	}
	// After force-restore, seal-status may still advertise the target's old T
	// even though restored source shares can already unseal the barrier.
	// Classify insufficiency only after submission fails to finish unsealing.
	if s.T > len(keys) {
		return s, ErrInsufficientShares
	}
	return s, errors.New("openbao remained sealed after all unseal shares")
}

// RaftServer is one entry of the Raft configuration.
type RaftServer struct {
	NodeID          string `json:"node_id"`
	Address         string `json:"address"`
	Leader          bool   `json:"leader"`
	ProtocolVersion string `json:"protocol_version"`
	Voter           bool   `json:"voter"`
}

// RaftConfiguration reads sys/storage/raft/configuration (authenticated).
func (c *Client) RaftConfiguration(ctx context.Context) ([]RaftServer, error) {
	var out struct {
		Data struct {
			Config struct {
				Servers []RaftServer `json:"servers"`
			} `json:"config"`
		} `json:"data"`
	}
	if err := c.Do(ctx, http.MethodGet, "sys/storage/raft/configuration", nil, &out); err != nil {
		return nil, err
	}
	return out.Data.Config.Servers, nil
}

// RemovePeer removes a Raft server by node id (authenticated, active node).
func (c *Client) RemovePeer(ctx context.Context, nodeID string) error {
	return c.Do(ctx, http.MethodPost, "sys/storage/raft/remove-peer", map[string]any{"server_id": nodeID}, nil)
}

// AutopilotServer is one server in the autopilot state.
type AutopilotServer struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Address     string `json:"address"`
	NodeStatus  string `json:"node_status"`
	LastContact string `json:"last_contact"`
	LastTerm    uint64 `json:"last_term"`
	LastIndex   uint64 `json:"last_index"`
	Healthy     bool   `json:"healthy"`
	StableSince string `json:"stable_since"`
	// Status is leader, voter or non-voter.
	Status  string `json:"status"`
	Version string `json:"version"`
}

// AutopilotState is sys/storage/raft/autopilot/state.
type AutopilotState struct {
	Healthy          bool                       `json:"healthy"`
	FailureTolerance int                        `json:"failure_tolerance"`
	Leader           string                     `json:"leader"`
	Voters           []string                   `json:"voters"`
	Servers          map[string]AutopilotServer `json:"servers"`
}

// AutopilotState reads the autopilot view (authenticated).
func (c *Client) AutopilotState(ctx context.Context) (AutopilotState, error) {
	var out struct {
		Data AutopilotState `json:"data"`
	}
	err := c.Do(ctx, http.MethodGet, "sys/storage/raft/autopilot/state", nil, &out)
	return out.Data, err
}

// StepDown asks the active node to give up leadership (authenticated).
func (c *Client) StepDown(ctx context.Context) error {
	return c.Do(ctx, http.MethodPut, "sys/step-down", nil, nil)
}

// Snapshot streams a Raft snapshot (application/gzip). The caller closes it.
func (c *Client) Snapshot(ctx context.Context) (io.ReadCloser, error) {
	return c.Stream(ctx, http.MethodGet, "sys/storage/raft/snapshot", nil, "")
}

// RestoreSnapshot posts a snapshot to the active node; force restores a
// snapshot taken from another cluster (snapshot-force).
func (c *Client) RestoreSnapshot(ctx context.Context, snapshot io.Reader, force bool) error {
	path := "sys/storage/raft/snapshot"
	if force {
		path = "sys/storage/raft/snapshot-force"
	}
	body, err := c.Stream(ctx, http.MethodPost, path, snapshot, "application/octet-stream")
	if err != nil {
		return err
	}
	drain(body)
	return nil
}

// LookupSelfTTL returns the remaining TTL of the client token; a 403 means the
// token is invalid or revoked.
func (c *Client) LookupSelfTTL(ctx context.Context) (time.Duration, error) {
	var out struct {
		Data struct {
			TTL int64 `json:"ttl"`
		} `json:"data"`
	}
	if err := c.Do(ctx, http.MethodGet, "auth/token/lookup-self", nil, &out); err != nil {
		return 0, err
	}
	return time.Duration(out.Data.TTL) * time.Second, nil
}

// GenerateRoot runs a complete sys/generate-root attempt with the given unseal
// (or recovery) shares and returns the new root token. Any in-progress attempt
// is cancelled first; on failure the attempt is cancelled.
func (c *Client) GenerateRoot(ctx context.Context, keys []string) (token string, err error) {
	if err := c.Do(ctx, http.MethodDelete, "sys/generate-root/attempt", nil, nil); err != nil {
		return "", err
	}
	var attempt struct {
		Nonce    string `json:"nonce"`
		OTP      string `json:"otp"`
		Required int    `json:"required"`
	}
	if err := c.Do(ctx, http.MethodPut, "sys/generate-root/attempt", map[string]any{}, &attempt); err != nil {
		return "", err
	}
	defer func() {
		if err != nil {
			cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			if cancelErr := c.Do(cancelCtx, http.MethodDelete, "sys/generate-root/attempt", nil, nil); cancelErr != nil {
				// Do not expose a cleanup HTTP 400 as a share rejection.
				err = fmt.Errorf("openbao cancel failed generate-root attempt: %v", cancelErr)
			}
		}
	}()
	if attempt.OTP == "" || attempt.Nonce == "" {
		return "", errors.New("openbao generate-root returned no OTP")
	}
	if attempt.Required > len(keys) {
		return "", ErrInsufficientShares
	}
	for _, k := range keys {
		var update struct {
			Complete     bool   `json:"complete"`
			EncodedToken string `json:"encoded_token"`
		}
		if err := c.Do(ctx, http.MethodPut, "sys/generate-root/update", map[string]any{"key": k, "nonce": attempt.Nonce}, &update); err != nil {
			return "", err
		}
		if update.Complete {
			return decodeRootToken(update.EncodedToken, attempt.OTP)
		}
	}
	return "", errors.New("openbao generate-root did not complete with the stored shares")
}

func decodeRootToken(encoded, otp string) (string, error) {
	raw, err := base64.RawStdEncoding.DecodeString(encoded)
	if err != nil {
		if raw, err = base64.StdEncoding.DecodeString(encoded); err != nil {
			return "", errors.New("openbao generate-root returned an undecodable token")
		}
	}
	if len(raw) != len(otp) {
		return "", errors.New("openbao generate-root token and OTP lengths differ")
	}
	out := make([]byte, len(raw))
	for i := range raw {
		out[i] = raw[i] ^ otp[i]
	}
	return string(out), nil
}
