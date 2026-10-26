// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package users

import (
	"context"
	"crypto/md5" // MongoDB's SCRAM-SHA-1 password digest requires MD5.
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/xdg-go/scram"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Credential material is confined to reconciliation, never mapped to domain users.
type systemUser struct {
	Roles       []wireRoleRef              `bson:"roles"`
	Credentials map[string]scramCredential `bson:"credentials"`
	Mechanisms  []string                   `bson:"mechanisms"`
}

type scramCredential struct {
	IterationCount int    `bson:"iterationCount"`
	Salt           string `bson:"salt"`
	StoredKey      string `bson:"storedKey"`
	ServerKey      string `bson:"serverKey"`
}

func getSystemUser(ctx context.Context, c *mongo.Client, name string) (*systemUser, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	selector := bson.D{{Key: "user", Value: name}, {Key: "db", Value: "admin"}}
	var response struct {
		Users []systemUser `bson:"users"`
	}
	visible := true
	err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "usersInfo", Value: selector}, {Key: "showCredentials", Value: true}}).Decode(&response)
	var ce mongo.CommandError
	if errors.As(err, &ce) && ce.Code == 13 { // Unauthorized, not authentication or transport failure.
		visible = false
		response.Users = nil
		err = c.Database("admin").RunCommand(ctx, bson.D{{Key: "usersInfo", Value: selector}, {Key: "showCredentials", Value: false}}).Decode(&response)
	}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			// Preserve the deadline without exposing credential-bearing server data.
			return nil, false, fmt.Errorf("PSMDB usersInfo read exceeded 4s budget: inspect member responsiveness/connectivity: %w", context.DeadlineExceeded)
		}
		// Decode and server errors may contain credential fields. Do not wrap them.
		if errors.As(err, &ce) {
			return nil, false, commandError(ce)
		}
		return nil, false, errors.New("MongoDB system-user lookup failed")
	}
	if len(response.Users) == 0 {
		return nil, visible, fmt.Errorf("system user not found: %w", engine.ErrNotFound)
	}
	return &response.Users[0], visible, nil
}

func (u *systemUser) requiredMechanisms() []string {
	mechanisms := []string{"SCRAM-SHA-256"}
	_, sha1 := u.Credentials["SCRAM-SHA-1"]
	for _, mechanism := range u.Mechanisms {
		sha1 = sha1 || mechanism == "SCRAM-SHA-1"
	}
	if sha1 {
		mechanisms = append(mechanisms, "SCRAM-SHA-1")
	}
	return mechanisms
}

func (u *systemUser) passwordMatches(user, password string) bool {
	matches := true
	for _, mechanism := range u.requiredMechanisms() {
		credential, ok := u.Credentials[mechanism]
		if !ok || !credential.matches(user, password, mechanism) {
			matches = false
		}
	}
	return matches
}

func (c scramCredential) matches(user, password, mechanism string) bool {
	if c.IterationCount <= 0 {
		return false
	}
	salt, err := base64.StdEncoding.DecodeString(c.Salt)
	if err != nil || len(salt) == 0 {
		return false
	}
	storedKey, err := base64.StdEncoding.DecodeString(c.StoredKey)
	if err != nil {
		return false
	}
	serverKey, err := base64.StdEncoding.DecodeString(c.ServerKey)
	if err != nil {
		return false
	}
	var client *scram.Client
	if mechanism == "SCRAM-SHA-1" {
		digest := md5.Sum([]byte(user + ":mongo:" + password))
		client, err = scram.SHA1.NewClientUnprepped(user, hex.EncodeToString(digest[:]), "")
	} else {
		client, err = scram.SHA256.NewClient(user, password, "")
	}
	if err != nil {
		return false
	} // SASLprep errors include passwords; never propagate.
	derived := client.GetStoredCredentials(scram.KeyFactors{Salt: string(salt), Iters: c.IterationCount})
	storedMatches := subtle.ConstantTimeCompare(storedKey, derived.StoredKey)
	serverMatches := subtle.ConstantTimeCompare(serverKey, derived.ServerKey)
	return storedMatches&serverMatches == 1
}
