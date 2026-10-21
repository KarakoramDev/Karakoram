// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baoapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
)

type storedUnseal struct {
	Shares        []string  `json:"shares"`
	Threshold     int       `json:"threshold"`
	SealType      string    `json:"sealType"`
	Recovery      bool      `json:"recovery"`
	RootToken     string    `json:"rootToken"`
	InitializedAt time.Time `json:"initializedAt"`
}
type initKey struct {
	ReceiptID  string `json:"receiptId"`
	PrivateKey []byte `json:"privateKey"`
}

func readUnseal(ctx context.Context, rc *engine.Context) (storedUnseal, error) {
	var v storedUnseal
	b, err := secret(ctx, rc, SecretUnseal)
	if err != nil {
		return v, err
	}
	if json.Unmarshal(b, &v) != nil || v.Threshold < 1 || len(v.Shares) < v.Threshold {
		return storedUnseal{}, fmt.Errorf("invalid stored OpenBao initialization material")
	}
	return v, nil
}
func (p *Provider) initializationLost(s *internalState) error {
	s.InitializationOutcomeLost = "OpenBao was initialized without recoverable initialization material; recover the original control-plane secrets and encrypted receipt before proceeding"
	s.Phase = "initialization-outcome-lost"
	return fmt.Errorf("InitializationOutcomeLost: %s", s.InitializationOutcomeLost)
}

// initialize returns after each persistence boundary. The owner and private key
// are durable before the first RPC; a replay always uses the same recipient.
func (p *Provider) initialize(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	if s.InitializationOutcomeLost != "" {
		return true, p.initializationLost(s)
	}
	_, err := readUnseal(ctx, rc)
	if err == nil {
		s.Initialized = true
		return false, nil
	}
	if !missing(err) {
		return true, err
	}
	if s.Initialized {
		return true, p.initializationLost(s)
	}
	if s.InitOwner == "" {
		members := ordered(s)
		if len(members) == 0 {
			return true, nil
		}
		s.InitOwner = members[0].Name
		return true, nil
	}
	owner := s.find(s.InitOwner)
	if owner == nil {
		return true, p.initializationLost(s)
	}
	if owner.Stage != "configured" && owner.Stage != "ready" {
		return true, nil
	}
	var key initKey
	raw, err := secret(ctx, rc, SecretInitKey)
	if missing(err) {
		private, _, err := baospec.NewRecipientKey(rand.Reader)
		if err != nil {
			return true, err
		}
		id := make([]byte, 24)
		if _, err = rand.Read(id); err != nil {
			return true, err
		}
		key = initKey{ReceiptID: hex.EncodeToString(id), PrivateKey: private}
		raw, err = json.Marshal(key)
		if err != nil {
			return true, err
		}
		return true, putSecret(ctx, rc, SecretInitKey, raw)
	}
	if err != nil {
		return true, err
	}
	if json.Unmarshal(raw, &key) != nil || !baospec.ValidReceiptID(key.ReceiptID) {
		return true, fmt.Errorf("invalid stored OpenBao receipt key")
	}
	private, err := ecdh.X25519().NewPrivateKey(key.PrivateKey)
	if err != nil {
		return true, fmt.Errorf("invalid stored OpenBao receipt key")
	}
	o, err := parseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return true, err
	}
	var response baospec.InitResponse
	request := baospec.InitRequest{ReceiptID: key.ReceiptID, RecipientPublicKey: private.PublicKey().Bytes()}
	if p.autoUnseal(rc.Cluster) {
		request.RecoveryShares, request.RecoveryThreshold = o.Unseal.Shares, o.Unseal.Threshold
	} else {
		request.SecretShares, request.SecretThreshold = o.Unseal.Shares, o.Unseal.Threshold
	}
	err = p.call(ctx, rc, owner.Name, baospec.ActionInit, request, &response)
	if err != nil {
		var h *agentapi.HTTPError
		if errors.As(err, &h) && h.StatusCode == http.StatusConflict {
			return true, p.initializationLost(s)
		}
		return true, fmt.Errorf("OpenBao initialization receipt is not yet available")
	}
	if response.Envelope.ReceiptID != key.ReceiptID {
		return true, p.initializationLost(s)
	}
	plain, err := baospec.OpenReceipt(key.PrivateKey, rc.Cluster.Name, owner.Name, response.Envelope)
	if err != nil {
		return true, fmt.Errorf("cannot decrypt OpenBao initialization receipt")
	}
	var init baospec.InitMaterial
	if json.Unmarshal(plain, &init) != nil {
		return true, fmt.Errorf("invalid OpenBao initialization receipt")
	}
	shares := init.KeysBase64
	if p.autoUnseal(rc.Cluster) {
		shares = init.RecoveryKeysBase64
		init.Keys = init.RecoveryKeys
	}
	if len(shares) == 0 {
		for _, v := range init.Keys {
			b, e := hex.DecodeString(v)
			if e != nil {
				return true, fmt.Errorf("invalid OpenBao initialization shares")
			}
			shares = append(shares, base64.StdEncoding.EncodeToString(b))
		}
	}
	if len(shares) != o.Unseal.Shares || init.RootToken == "" {
		return true, fmt.Errorf("incomplete OpenBao initialization receipt")
	}
	for _, v := range shares {
		if b, e := base64.StdEncoding.DecodeString(v); e != nil || len(b) == 0 {
			return true, fmt.Errorf("invalid OpenBao initialization shares")
		}
	}
	material := storedUnseal{Shares: shares, Threshold: o.Unseal.Threshold, SealType: o.Seal.Type, Recovery: p.autoUnseal(rc.Cluster), RootToken: init.RootToken, InitializedAt: now(rc)}
	raw, err = json.Marshal(material)
	if err != nil {
		return true, err
	}
	if err = putSecret(ctx, rc, SecretUnseal, raw); err != nil {
		return true, err
	}
	s.Initialized = true
	return true, nil
}

func (p *Provider) ackInitialization(ctx context.Context, rc *engine.Context, s *internalState) error {
	raw, err := secret(ctx, rc, SecretInitKey)
	if missing(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var key initKey
	if json.Unmarshal(raw, &key) != nil {
		return fmt.Errorf("invalid stored OpenBao receipt key")
	}
	var out struct {
		OK bool `json:"ok"`
	}
	if err = p.call(ctx, rc, s.InitOwner, baospec.ActionInitAck, baospec.InitAckRequest{ReceiptID: key.ReceiptID}, &out); err != nil {
		return fmt.Errorf("OpenBao initialization acknowledgement failed")
	}
	return rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), SecretInitKey)
}

// ensureManagementToken recovers access without passing any share to an agent.
func (p *Provider) ensureManagementToken(ctx context.Context, rc *engine.Context, s *internalState) (string, error) {
	return p.ensureManagementTokenWithMaterials(ctx, rc, s, nil)
}

// Restore may straddle replacement of the barrier keys. A native threshold
// mismatch or key rejection permits trying retained pre-restore material.
func (p *Provider) ensureManagementTokenWithMaterials(ctx context.Context, rc *engine.Context, s *internalState, previous []storedUnseal) (string, error) {
	token, err := secret(ctx, rc, SecretManagementToken)
	if err != nil && !missing(err) {
		return "", err
	}
	c, active, err := p.activeClient(ctx, rc, s, string(token))
	if err != nil {
		return "", err
	}
	if len(token) > 0 {
		ttl, e := c.LookupSelfTTL(ctx)
		if e == nil && ttl > 0 {
			if ttl < 12*time.Hour {
				if e = c.Do(ctx, http.MethodPost, "auth/token/renew-self", map[string]any{"increment": "24h"}, nil); e != nil {
					return "", e
				}
			}
			return string(token), nil
		}
		if e != nil && !baoapi.IsStatus(e, http.StatusForbidden) {
			return "", e
		}
	}
	material, err := readUnseal(ctx, rc)
	if err != nil {
		return "", err
	}
	root := ""
	generated := false
	if _, e := secret(ctx, rc, SecretInitKey); e == nil && material.RootToken != "" && len(previous) == 0 {
		// A pending receipt belongs to the target bootstrap, not necessarily to
		// restored material. Supplied source shares carry no bootstrap token.
		root = material.RootToken
	} else if e != nil && !missing(e) {
		return "", e
	} else {
		recovery, err := p.recoveryClient(ctx, rc, active.Name)
		if err != nil {
			return "", err
		}
		for i := 0; i <= len(previous); i++ {
			candidate := material
			if i > 0 {
				candidate = previous[i-1]
			}
			root, err = recovery.GenerateRoot(ctx, candidate.Shares[:candidate.Threshold])
			if err == nil {
				break
			}
			if !errors.Is(err, baoapi.ErrInsufficientShares) && !baoapi.IsStatus(err, http.StatusBadRequest) {
				return "", err
			}
		}
		if err != nil {
			return "", err
		}
		generated = true
	}
	admin := c.WithToken(root)
	if generated {
		defer func() {
			revokeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			_ = admin.Do(revokeCtx, http.MethodPost, "auth/token/revoke-self", nil, nil)
		}()
	}
	if err = admin.Do(ctx, http.MethodPut, "sys/policies/acl/karakoram-manager", map[string]any{"policy": managerPolicy}, nil); err != nil {
		return "", err
	}
	// Explicitly keep automatic peer deletion off: the provider owns Raft membership.
	if !p.postgresStorage(rc.Cluster) {
		if err = admin.Do(ctx, http.MethodPost, "sys/storage/raft/autopilot/configuration", map[string]any{"cleanup_dead_servers": false}, nil); err != nil {
			return "", err
		}
	}
	var out struct {
		Auth struct {
			ClientToken string `json:"client_token"`
		} `json:"auth"`
	}
	if err = admin.Do(ctx, http.MethodPost, "auth/token/create-orphan", map[string]any{"policies": []string{"karakoram-manager"}, "period": "24h", "no_default_policy": true, "display_name": "karakoram-manager"}, &out); err != nil {
		return "", err
	}
	if out.Auth.ClientToken == "" {
		return "", fmt.Errorf("OpenBao returned no management token")
	}
	if err = putSecret(ctx, rc, SecretManagementToken, []byte(out.Auth.ClientToken)); err != nil {
		return "", err
	}
	return out.Auth.ClientToken, nil
}
