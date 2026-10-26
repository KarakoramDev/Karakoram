// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const restartActiveLimit = 128
const restartRetiredBytes = 64 << 10
const restartRetiredLimit = 32768

func loadRestartRetirement(dir string) ([]byte, error) {
	filter, err := os.ReadFile(dir + ".retired")
	if os.IsNotExist(err) {
		if _, markerErr := os.Stat(dir + ".initialized"); !os.IsNotExist(markerErr) {
			return nil, errors.New("restart retirement history unavailable; recover trusted receipt metadata before restarting")
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(filter) != 4+restartRetiredBytes || binary.LittleEndian.Uint32(filter[:4]) > restartRetiredLimit {
		return nil, errors.New("restart retirement history corrupt; recover trusted receipt metadata")
	}
	return filter, nil
}

// Retired arbitrary identities cannot be dropped safely. A fixed-size monotone
// filter remembers them conservatively: false positives refuse an action, never
// repeat one. The finite retirement allowance fails closed before saturation;
// exhausted members require a new member incarnation, not deletion of receipts.
// Pending/unknown receipts are never retired. At most 128 active receipts plus
// 64 KiB of retirement metadata are retained, independent of request body size.
func reserveRestartReceipt(dir string, digest [sha256.Size]byte) error {
	filterPath := dir + ".retired"
	markerPath := dir + ".initialized"
	filter, err := loadRestartRetirement(dir)
	if err != nil {
		return err
	}
	if filter == nil {
		filter = make([]byte, 4+restartRetiredBytes)
		if err := atomicWrite(filterPath, filter, 0600); err != nil {
			return err
		}
	}
	if err := atomicWrite(markerPath, []byte("initialized\n"), 0600); err != nil {
		return err
	}
	if retiredRestart(filter, digest, false) {
		return errors.New("restart identity retired (or conservatively fenced by retirement history); stale retries must not execute again")
	}
	if binary.LittleEndian.Uint32(filter[:4]) == restartRetiredLimit {
		return errors.New("restart receipt retirement allowance exhausted; replace this member through the control plane with a new incarnation; do not delete replay history")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	entries, err := d.ReadDir(restartActiveLimit + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if len(entries) > restartActiveLimit {
		return errors.New("restart receipt quota exceeded; reconcile existing receipt storage before admitting another restart")
	}
	if len(entries) < restartActiveLimit {
		return nil
	}
	var victim string
	var oldest time.Time
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".json") {
			return errors.New("unexpected restart receipt storage; reconcile before admitting another restart")
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Size() > 8192 {
			return errors.New("restart receipt exceeds storage allowance; reconcile before retrying")
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return err
		}
		var receipt restartReceipt
		if json.Unmarshal(data, &receipt) != nil {
			return errors.New("restart receipt corrupt; unresolved receipts cannot be retired")
		}
		if receipt.Complete && (victim == "" || info.ModTime().Before(oldest)) {
			victim, oldest = entry.Name(), info.ModTime()
		}
	}
	if victim == "" {
		return errors.New("restart receipt quota occupied by unresolved outcomes; reconcile pending process generations before another restart")
	}
	decoded, err := hex.DecodeString(strings.TrimSuffix(victim, ".json"))
	if err != nil || len(decoded) != sha256.Size {
		return errors.New("invalid restart receipt identity; refusing unsafe retirement")
	}
	var retired [sha256.Size]byte
	copy(retired[:], decoded)
	retiredRestart(filter, retired, true)
	binary.LittleEndian.PutUint32(filter[:4], binary.LittleEndian.Uint32(filter[:4])+1)
	// Publish replay refusal before deleting the old receipt. A crash may leave
	// both, which is safe: the active completed receipt takes precedence.
	if err := atomicWrite(filterPath, filter, 0600); err != nil {
		return fmt.Errorf("persist restart retirement: %w", err)
	}
	if err := os.Remove(filepath.Join(dir, victim)); err != nil {
		return err
	}
	return d.Sync()
}

func retiredRestart(filter []byte, digest [sha256.Size]byte, add bool) bool {
	present := true
	for i := range 4 {
		bit := binary.LittleEndian.Uint32(digest[i*4:]) % (restartRetiredBytes * 8)
		index, mask := 4+bit/8, byte(1<<(bit%8))
		present = present && filter[index]&mask != 0
		if add {
			filter[index] |= mask
		}
	}
	return present
}
