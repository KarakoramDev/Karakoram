// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
)

func (p *Provider) autoUnseal(c *domain.Cluster) bool {
	o, e := parseOptions(c.Spec.EngineOptions)
	return e == nil && o.Seal.Type != "shamir"
}
func (p *Provider) postgresStorage(c *domain.Cluster) bool {
	o, e := parseOptions(c.Spec.EngineOptions)
	return e == nil && o.Storage.Type == "postgresql"
}

func (p *Provider) ExtractWriteOnlyOptions(old *domain.ClusterSpec, spec *domain.ClusterSpec) (map[string][]byte, error) {
	o, e := parseOptions(spec.EngineOptions)
	if e != nil {
		return nil, e
	}
	var prior *Options
	if old != nil {
		v, e := parseOptions(old.EngineOptions)
		if e != nil {
			return nil, e
		}
		prior = &v
	}
	if e = validateSealAndStorage(prior, &o); e != nil {
		return nil, e
	}
	if o.Storage.Type == "postgresql" && p.options.Bindings == nil {
		return nil, invalid("PostgreSQL storage requires the PostgreSQL binding service")
	}
	values := map[string][]byte{}
	if t := o.Seal.Transit; t != nil {
		if old == nil && t.Token == "" {
			return nil, invalid("transit token is required on create")
		}
		if t.Token != "" {
			values[SecretTransitToken] = []byte(t.Token)
			t.Token = ""
		}
	}
	if a := o.Seal.AWSKMS; a != nil {
		if (a.AccessKeyID == "") != (a.SecretAccessKey == "") || old == nil && a.AccessKeyID == "" {
			return nil, invalid("both AWS credentials are required")
		}
		if a.AccessKeyID != "" {
			values[SecretAWSAccessKeyID] = []byte(a.AccessKeyID)
			values[SecretAWSSecretAccessKey] = []byte(a.SecretAccessKey)
			a.AccessKeyID = ""
			a.SecretAccessKey = ""
		}
	}
	spec.EngineOptions, e = json.Marshal(o)
	return values, e
}

func (p *Provider) memberSeal(ctx context.Context, rc *engine.Context, o Options) (baospec.Seal, error) {
	s := baospec.Seal{Type: o.Seal.Type}
	if s.Type == "" {
		s.Type = "shamir"
	}
	if t := o.Seal.Transit; t != nil {
		token, e := secret(ctx, rc, SecretTransitToken)
		if e != nil {
			return s, e
		}
		mount := t.MountPath
		if mount == "" {
			mount = "transit/"
		}
		s.Transit = &baospec.TransitSeal{Address: t.Address, MountPath: mount, KeyName: t.KeyName, Token: string(token), CAPEM: t.CAPEM, TLSServerName: t.TLSServerName}
	}
	if a := o.Seal.AWSKMS; a != nil {
		key, e := secret(ctx, rc, SecretAWSAccessKeyID)
		if e != nil {
			return s, e
		}
		password, e := secret(ctx, rc, SecretAWSSecretAccessKey)
		if e != nil {
			return s, e
		}
		s.AWSKMS = &baospec.AWSKMSSeal{Region: a.Region, KMSKeyID: a.KMSKeyID, Endpoint: a.Endpoint, AccessKey: string(key), SecretKey: string(password)}
	}
	return s, nil
}

var _ engine.WriteOnlyOptions = (*Provider)(nil)

// Only the digest participates in durable restart state, never credentials.
func (p *Provider) sealCredentialFingerprint(ctx context.Context, rc *engine.Context) (string, error) {
	o, err := parseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return "", err
	}
	var keys []string
	switch o.Seal.Type {
	case "transit":
		keys = []string{SecretTransitToken}
	case "awskms":
		keys = []string{SecretAWSAccessKeyID, SecretAWSSecretAccessKey}
	default:
		return "", nil
	}
	h := sha256.New()
	for _, key := range keys {
		value, err := secret(ctx, rc, key)
		if err != nil {
			return "", err
		}
		h.Write([]byte(key))
		h.Write([]byte{0})
		h.Write(value)
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
