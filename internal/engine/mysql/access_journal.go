// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

const accessPendingKey = "mysql/access/pending"
const accessUsersKey = "mysql/access/proxy-users"
const accessRevisionKey = "mysql/access/revision"
const accessCredentialsKey = "mysql/access/credentials"

type accessCredential struct {
	Current   string `json:"current"`
	Secondary string `json:"secondary,omitempty"`
}

// accessIntent exists only in the sealed secret store. The exact request and
// hash include the operation identity; replay never generates another password.
// Revision is published last, after both recoverable credential indexes.
type accessIntent struct {
	Request     myspec.AccessRequest        `json:"request"`
	RequestHash string                      `json:"requestHash"`
	Revision    int64                       `json:"revision"`
	Generated   bool                        `json:"generated,omitempty"`
	Previous    map[string][]byte           `json:"previous"`
	Users       []myspec.ProxyUser          `json:"users"`
	Credentials map[string]accessCredential `json:"credentials"`
	Applied     bool                        `json:"applied,omitempty"`
}

func accessRequestHash(req myspec.AccessRequest) string {
	raw, _ := json.Marshal(req)
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:])
}
func loadAccessIntent(ctx context.Context, rc *engine.Context) (*accessIntent, error) {
	raw, err := secret(ctx, rc, accessPendingKey)
	if missing(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var intent accessIntent
	if err = json.Unmarshal(raw, &intent); err != nil {
		return nil, err
	}
	if intent.Request.RequestID == "" || intent.Revision < 1 || intent.RequestHash != accessRequestHash(intent.Request) {
		return nil, engine.Invalid("invalid sealed MySQL access intent")
	}
	return &intent, nil
}
func sealAccessIntent(ctx context.Context, rc *engine.Context, intent *accessIntent) error {
	raw, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	return putSecret(ctx, rc, accessPendingKey, raw)
}
func accessRevision(ctx context.Context, rc *engine.Context) (int64, error) {
	raw, err := secret(ctx, rc, accessRevisionKey)
	if missing(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var revision int64
	err = json.Unmarshal(raw, &revision)
	return revision, err
}
func clearAccessIntent(ctx context.Context, rc *engine.Context) error {
	err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), accessPendingKey)
	if missing(err) {
		return nil
	}
	return err
}
func accessResultError(result myspec.AccessResult) error {
	switch result.Status {
	case http.StatusNotFound:
		return engine.ErrNotFound
	case http.StatusConflict:
		return engine.ErrConflict
	case http.StatusServiceUnavailable:
		return engine.ErrUnavailable
	default:
		return engine.Invalid("MySQL rejected the account operation")
	}
}
func (p *Core) completeAccessIntent(ctx context.Context, rc *engine.Context, intent *accessIntent) error {
	if err := accessFence(ctx, rc); err != nil {
		return err
	}
	if !intent.Applied {
		var result myspec.AccessResult
		if err := p.accessAction(ctx, rc, myspec.ActionAccessUsers, intent.Request, &result); err != nil {
			return err
		}
		switch result.Outcome {
		case "rejected":
			for key, raw := range intent.Previous {
				var err error
				if raw == nil {
					err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key)
					if missing(err) {
						err = nil
					}
				} else {
					err = putSecret(ctx, rc, key, raw)
				}
				if err != nil {
					return err
				}
			}
			if err := clearAccessIntent(ctx, rc); err != nil {
				return err
			}
			return accessResultError(result)
		case "applied":
			intent.Applied = true
			if err := sealAccessIntent(ctx, rc, intent); err != nil {
				return err
			}
		default:
			return fmt.Errorf("MySQL account outcome unknown; sealed intent retained: %w", engine.ErrUnavailable)
		}
	}
	for _, item := range []struct {
		key   string
		value any
	}{{accessCredentialsKey, intent.Credentials}, {accessUsersKey, intent.Users}, {accessRevisionKey, intent.Revision}} {
		raw, err := json.Marshal(item.value)
		if err != nil {
			return err
		}
		if err = putSecret(ctx, rc, item.key, raw); err != nil {
			return err
		}
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return err
	}
	if s.ConfigVersion < intent.Revision {
		s.ConfigVersion = intent.Revision
	}
	for _, m := range s.Members {
		if m.ConfigVersion < intent.Revision {
			m.ConfigVersion = intent.Revision
		}
	}
	if err = save(rc.Cluster, s); err != nil {
		return err
	}
	// MemberConfig also reads the sealed revision: config pulls do not depend on
	// an API handler persisting its local Cluster copy before acknowledgement.
	wait, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		all := true
		for _, m := range s.Members {
			if m.Role != "proxy" || m.Replicas == 0 {
				continue
			}
			ok, err := proxyApplied(wait, rc, m)
			if err != nil || !ok {
				all = false
			}
		}
		if all {
			return clearAccessIntent(ctx, rc)
		}
		select {
		case <-wait.Done():
			return fmt.Errorf("MySQL proxy credential acknowledgement pending: %w", engine.ErrUnavailable)
		case <-ticker.C:
		}
	}
}
func (p *Core) finishPendingAccess(ctx context.Context, rc *engine.Context) error {
	intent, err := loadAccessIntent(ctx, rc)
	if err != nil || intent == nil {
		return err
	}
	// A newly claimed lifecycle operation must finish the older sealed access
	// intent before freezing its backup snapshot or starting destructive work.
	// Ignore only its queue ownership here, never durable native/restore fences.
	recovery := *rc
	cluster := *rc.Cluster
	cluster.OperationID = ""
	recovery.Cluster = &cluster
	err = p.completeAccessIntent(ctx, &recovery, intent)
	rc.Cluster.Internal = cluster.Internal
	return err
}
func (p *Core) mutateUser(ctx context.Context, rc *engine.Context, req myspec.AccessRequest) (string, error) {
	p.accessMu.Lock()
	defer p.accessMu.Unlock()
	if err := accessFence(ctx, rc); err != nil {
		return "", err
	}
	// A retried identical API operation resumes the original generated password.
	// Different operations first finish the previous publication, never supersede it.
	pending, err := loadAccessIntent(ctx, rc)
	if err != nil {
		return "", err
	}
	if pending != nil {
		original := pending.Request
		original.RequestID = ""
		if original.Operation == "password" {
			original.Password = nil
		}
		if original.Operation == "create" && pending.Generated {
			u := *original.User
			u.Password = nil
			original.User = &u
		}
		if accessRequestHash(original) == accessRequestHash(req) {
			if err = p.completeAccessIntent(ctx, rc, pending); err != nil {
				return "", err
			}
			return generatedAccessPassword(pending), nil
		}
		if err = p.completeAccessIntent(ctx, rc, pending); err != nil {
			return "", err
		}
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return "", err
	}
	if req.RetainCurrent {
		if rc.Cluster.Spec.Components.Extra["proxy"].Replicas > 0 {
			return "", engine.Invalid("retainCurrent is unsupported with ProxySQL: both passwords must remain usable")
		}
		for _, m := range s.Members {
			if m.Role == "proxy" && m.Replicas > 0 {
				return "", engine.Invalid("retainCurrent is unsupported with ProxySQL")
			}
		}
	}
	users, err := managedProxyUsers(ctx, rc)
	if err != nil {
		return "", err
	}
	credentials := map[string]accessCredential{}
	previous := map[string][]byte{}
	for _, key := range []string{accessUsersKey, accessCredentialsKey, accessRevisionKey} {
		raw, e := secret(ctx, rc, key)
		if e != nil && !missing(e) {
			return "", e
		}
		previous[key] = raw
		if key == accessCredentialsKey && len(raw) > 0 {
			if e = json.Unmarshal(raw, &credentials); e != nil {
				return "", e
			}
		}
	}
	revision, err := accessRevision(ctx, rc)
	if err != nil {
		return "", err
	}
	revision = max(revision, s.ConfigVersion) + 1
	intent := &accessIntent{Request: req, Previous: previous, Users: users, Credentials: credentials, Revision: revision}
	name := req.Name
	if req.User != nil {
		name = req.User.Name
	}
	if req.Operation == "create" && req.User.Password == nil || req.Operation == "password" {
		password, err := secrets.Password()
		if err != nil {
			return "", err
		}
		intent.Generated = true
		if req.Operation == "create" {
			user := *req.User
			user.Password = &password
			intent.Request.User = &user
		} else {
			intent.Request.Password = &password
		}
	}
	credential := credentials[name]
	if credential.Current == "" {
		for _, user := range users {
			if user.Username == name {
				credential.Current = user.Password
				credentials[name] = credential
				break
			}
		}
	}
	if req.RetainCurrent && credential.Current == "" {
		return "", engine.Invalid("retainCurrent requires a sealed managed current password")
	}
	switch req.Operation {
	case "create":
		credential = accessCredential{Current: *intent.Request.User.Password}
		credentials[name] = credential
	case "password":
		if req.RetainCurrent {
			credential.Secondary = credential.Current
		} else {
			credential.Secondary = ""
		}
		credential.Current = *intent.Request.Password
		credentials[name] = credential
	case "discard-old":
		credential.Secondary = ""
		if credential.Current != "" {
			credentials[name] = credential
		}
	case "delete":
		delete(credentials, name)
	}
	intent.Users = slices.DeleteFunc(intent.Users, func(user myspec.ProxyUser) bool { return user.Username == name })
	if req.Operation != "delete" && credential.Current != "" {
		intent.Users = append(intent.Users, myspec.ProxyUser{Username: name, Password: credential.Current, RequireTLS: true})
	}
	slices.SortFunc(intent.Users, func(a, b myspec.ProxyUser) int {
		if a.Username < b.Username {
			return -1
		}
		if a.Username > b.Username {
			return 1
		}
		return 0
	})
	intent.Request.RequestID, err = secrets.Token()
	if err != nil {
		return "", err
	}
	intent.RequestHash = accessRequestHash(intent.Request)
	if err = sealAccessIntent(ctx, rc, intent); err != nil {
		return "", err
	}
	if err = p.completeAccessIntent(ctx, rc, intent); err != nil {
		return "", err
	}
	return generatedAccessPassword(intent), nil
}
func generatedAccessPassword(intent *accessIntent) string {
	if !intent.Generated {
		return ""
	}
	if intent.Request.User != nil && intent.Request.User.Password != nil {
		return *intent.Request.User.Password
	}
	if intent.Request.Password != nil {
		return *intent.Request.Password
	}
	return ""
}
