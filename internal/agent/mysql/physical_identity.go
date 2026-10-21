// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

var physicalIdentityMu sync.Mutex

// The agent token lives outside the datadir and is never restored or cloned.
// The data token disappears on initialization/restore; clone/SST may copy it,
// but the recipient's agent token and server UUID still make a distinct source.
func physicalIdentity(p paths, uuid string) (string, error) {
	physicalIdentityMu.Lock()
	defer physicalIdentityMu.Unlock()
	if !myspec.ValidUUID(uuid) {
		return "", errors.New("physical_identity_invalid")
	}
	agentToken, err := physicalToken(filepath.Join(p.root, "physical-agent.json"))
	if err != nil {
		return "", err
	}
	dataToken, err := physicalToken(filepath.Join(p.data, "karakoram-physical.json"))
	if err != nil {
		return "", err
	}
	h := sha256.Sum256([]byte(agentToken + ":" + dataToken + ":" + uuid))
	return hex.EncodeToString(h[:]), nil
}

func physicalToken(name string) (string, error) {
	var token string
	b, err := os.ReadFile(name)
	if err == nil {
		if json.Unmarshal(b, &token) != nil {
			return "", errors.New("physical_identity_invalid")
		}
		decoded, decodeErr := hex.DecodeString(token)
		if decodeErr != nil || len(decoded) != 32 {
			return "", errors.New("physical_identity_invalid")
		}
		return token, nil
	}
	if !os.IsNotExist(err) {
		return "", err
	}
	var random [32]byte
	if _, err = rand.Read(random[:]); err != nil {
		return "", err
	}
	token = hex.EncodeToString(random[:])
	// durableJSON supplies atomic publication plus file/directory fsync.
	if err = durableJSON(name, token); err != nil {
		return "", err
	}
	return token, nil
}
