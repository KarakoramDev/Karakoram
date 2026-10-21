// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

const volumeSentinel = "karakoram/volume-identity.json"

func (a *nodeAgent) volumeObservation() *agentapi.VolumeStatus {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.volume == nil {
		return nil
	}
	v := *a.volume
	return &v
}

func sameVolume(a, b agentapi.VolumeIdentity) bool {
	return a.ClusterUID == b.ClusterUID && a.Member == b.Member && a.Volume == b.Volume && a.VolumeID == b.VolumeID
}

// volumeSafety runs before Apply and every supervised process start. It never
// calls the supervisor: restart callbacks invoke it with the supervisor locked.
func (a *nodeAgent) volumeSafety(ctx context.Context, c agentapi.MemberConfig) (err error) {
	if c.Volume == nil {
		return nil
	}
	want := *c.Volume
	if previous := a.volumeObservation(); previous != nil && sameVolume(previous.Identity, want) && (previous.Verified || previous.Identity.Initialized) {
		want.Initialized = true
	}
	state := &agentapi.VolumeStatus{Identity: want}
	defer func() {
		if err != nil {
			state.Message = err.Error()
		}
		a.mu.Lock()
		a.volume = state
		a.mu.Unlock()
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if want.ClusterUID == "" || want.Member != a.o.Member || want.Volume == "" || want.VolumeID == "" {
		return errors.New("VolumeIdentityUnavailable: incomplete durable volume identity; reconcile the member's original volume before starting its engine")
	}
	root, err := os.OpenRoot(a.o.DataDir)
	if err != nil {
		return fmt.Errorf("VolumeIdentityUnavailable: cannot open durable data directory: %w", err)
	}
	defer root.Close()
	data, err := readVolumeFile(root, volumeSentinel, 8192)
	if err == nil {
		var got agentapi.VolumeIdentity
		if json.Unmarshal(data, &got) != nil || !sameVolume(got, want) {
			return errors.New("VolumeIdentityUnavailable: durable volume identity sentinel is corrupt or belongs to another data incarnation; adoption requires verification of the original volume")
		}
		state.Verified = true
		return nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("VolumeIdentityUnavailable: cannot verify durable volume identity: %w", err)
	}
	retained, err := legacyVolumeData(root, c)
	if err != nil {
		return fmt.Errorf("VolumeIdentityUnavailable: cannot inspect retained native metadata; repair storage access before recovery: %w", err)
	}
	if !retained {
		empty, err := emptyNativeVolume(root)
		if err != nil {
			return fmt.Errorf("VolumeIdentityUnavailable: cannot establish whether native storage is empty: %w", err)
		}
		if !empty {
			return errors.New("VolumeIdentityUnavailable: retained data or recovery journals have an unrecognized or partial layout; original-volume adoption requires native metadata repair, not member replacement")
		}
		if want.Initialized {
			state.Lost = true
			return errors.New("VolumeLost: initialized member has no sentinel and positively empty native storage; refusing fresh engine initialization; recover the original volume or replace from healthy replicas")
		}
	}
	// Fresh empty volumes are authorized by the CP. Retained native layouts
	// require positive recognition, irrespective of the CP initialization bit.
	want.Initialized = false
	data, err = json.Marshal(want)
	if err != nil {
		return err
	}
	if err = (&Node{a: a}).WriteFile("volume-identity.json", data, 0600); err != nil {
		return fmt.Errorf("persist durable volume identity: %w", err)
	}
	state.Verified = true
	return nil
}

// Only known agent configuration and backend/filesystem scaffolding are excluded.
// Any other file, symlink, or unbounded/inaccessible subtree (including
// upgrade/restore journals and retired data directories) makes absence of native
// metadata inconclusive.
func emptyNativeVolume(root *os.Root) (bool, error) {
	remaining := 512
	var visit func(string, int) (bool, error)
	visit = func(name string, depth int) (bool, error) {
		if depth > 8 || remaining <= 0 {
			return false, errors.New("native emptiness scan limit reached")
		}
		f, err := root.Open(name)
		if err != nil {
			return false, err
		}
		entries, err := f.ReadDir(remaining + 1)
		f.Close()
		if err != nil && !errors.Is(err, io.EOF) {
			return false, err
		}
		for _, entry := range entries {
			remaining--
			if remaining < 0 {
				return false, errors.New("native emptiness scan limit reached")
			}
			path := filepath.Join(name, entry.Name())
			if entry.IsDir() {
				// ext4 creates this root-owned recovery namespace on a fresh
				// filesystem. It may not be readable by the member's UID and is
				// not an engine data directory. Never follow a same-name symlink.
				if path == "lost+found" {
					continue
				}
				empty, err := visit(path, depth+1)
				if err != nil || !empty {
					return false, err
				}
				continue
			}
			if entry.Type().IsRegular() {
				switch path {
				case ".karakoram-writer.lock", volumeSentinel,
					"karakoram/config.json", "karakoram/controls.json", "karakoram/controls.initialized",
					"karakoram/mongod.conf", "karakoram/mongos.conf", "karakoram/keyfile",
					"karakoram/mongodb.pem", "karakoram/ca.pem":
					continue
				}
			}
			return false, nil
		}
		return true, nil
	}
	return visit(".", 0)
}

func readVolumeFile(root *os.Root, name string, limit int64) ([]byte, error) {
	f, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Size() > limit {
		return nil, errors.New("invalid durable metadata file")
	}
	return io.ReadAll(io.LimitReader(f, limit))
}

func nonemptyVolumeFile(root *os.Root, name string, min int64) (bool, error) {
	st, err := root.Stat(name)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return st.Mode().IsRegular() && st.Size() >= min, nil
}

func nativeMetadata(root *os.Root, name string, limit int64) ([]byte, error) {
	data, err := readVolumeFile(root, name, limit)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	return data, err
}

// These checks establish retained native metadata, not database integrity.
// Unknown I/O and unbounded layouts are not evidence of data loss.
func legacyVolumeData(root *os.Root, c agentapi.MemberConfig) (bool, error) {
	switch engineName(c.Engine) {
	case "openbao":
		// Both Raft log and barrier databases are bbolt files. Require the
		// native meta-page magic, not merely nonempty files or a config cache.
		for _, name := range []string{"openbao/raft/raft.db", "openbao/raft/vault.db"} {
			f, err := root.Open(name)
			if errors.Is(err, fs.ErrNotExist) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			st, statErr := f.Stat()
			var header [24]byte
			_, readErr := io.ReadFull(f, header[:])
			f.Close()
			if statErr != nil {
				return false, statErr
			}
			if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
				return false, readErr
			}
			if readErr != nil || !st.Mode().IsRegular() || st.Size() < 8192 ||
				!bytes.Equal(header[16:24], []byte{0xed, 0xda, 0x0c, 0xed, 2, 0, 0, 0}) {
				return false, nil
			}
		}
		return true, nil
	case "psmdb":
		header, err := nativeMetadata(root, "db/WiredTiger", 8192)
		if err != nil {
			return false, err
		}
		if !bytes.Contains(header, []byte("WiredTiger")) {
			return false, nil
		}
		return nonemptyVolumeFile(root, "db/WiredTiger.wt", 4096)
	case "postgres":
		version, err := nativeMetadata(root, "postgres/PG_VERSION", 16)
		if err != nil {
			return false, err
		}
		v := strings.TrimSpace(string(version))
		if v == "" {
			return false, nil
		}
		for _, ch := range v {
			if ch != '.' && (ch < '0' || ch > '9') {
				return false, nil
			}
		}
		return nonemptyVolumeFile(root, "postgres/global/pg_control", 8192)
	case "mysql":
		auto, err := nativeMetadata(root, "mysql/auto.cnf", 4096)
		if err != nil {
			return false, err
		}
		serverUUID := ""
		for _, line := range strings.Split(string(auto), "\n") {
			key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
			if ok && strings.TrimSpace(key) == "server-uuid" {
				serverUUID = strings.TrimSpace(value)
			}
		}
		if len(serverUUID) != 36 || strings.Trim(serverUUID, "0-") == "" {
			return false, nil
		}
		for i, ch := range serverUUID {
			if i == 8 || i == 13 || i == 18 || i == 23 {
				if ch != '-' {
					return false, nil
				}
			} else if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f' || ch >= 'A' && ch <= 'F') {
				return false, nil
			}
		}
		return nonemptyVolumeFile(root, "mysql/mysql.ibd", 16384)
	case "elasticsearch":
		for _, dir := range []string{"karakoram/data/_state", "karakoram/data/nodes/0/_state"} {
			f, err := root.Open(dir)
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return false, err
			}
			entries, readErr := f.ReadDir(128)
			f.Close()
			if readErr != nil && !errors.Is(readErr, io.EOF) {
				return false, readErr
			}
			for _, entry := range entries {
				if !strings.HasPrefix(entry.Name(), "node-") || !strings.HasSuffix(entry.Name(), ".st") {
					continue
				}
				file, err := root.Open(filepath.Join(dir, entry.Name()))
				if err != nil {
					return false, err
				}
				var magic [4]byte
				_, err = io.ReadFull(file, magic[:])
				file.Close()
				if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
					return false, err
				}
				// Lucene CodecUtil.CODEC_MAGIC used by persisted node metadata.
				if err == nil && magic == [4]byte{0x3f, 0xd7, 0x6c, 0x17} {
					return true, nil
				}
			}
			if len(entries) == 128 && readErr == nil {
				return false, errors.New("native metadata scan limit reached")
			}
		}
	case "rabbitmq":
		// Only metadata-store directories are scanned, not message/queue data.
		// Config binds Khepri to the node; RAWA identifies retained Ra logs.
		config, wal := false, false
		remaining := 256
		var visit func(string, int) error
		visit = func(dir string, depth int) error {
			if config && wal {
				return nil
			}
			if depth > 5 || remaining <= 0 {
				return errors.New("native metadata scan limit reached")
			}
			f, err := root.Open(dir)
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			if err != nil {
				return err
			}
			entries, readErr := f.ReadDir(remaining)
			f.Close()
			if readErr != nil && !errors.Is(readErr, io.EOF) {
				return readErr
			}
			for _, entry := range entries {
				remaining--
				if remaining < 0 {
					return errors.New("native metadata scan limit reached")
				}
				name := filepath.Join(dir, entry.Name())
				if entry.IsDir() {
					if err := visit(name, depth+1); err != nil {
						return err
					}
					continue
				}
				if entry.Name() == "config" {
					data, err := nativeMetadata(root, name, 65536)
					if err != nil {
						return err
					}
					if bytes.Contains(data, []byte("rabbit@"+c.Member)) && bytes.Contains(data, []byte("rabbitmq_metadata")) {
						config = true
					}
				}
				if strings.HasSuffix(entry.Name(), ".wal") {
					file, err := root.Open(name)
					if err != nil {
						return err
					}
					var magic [5]byte
					_, err = io.ReadFull(file, magic[:])
					file.Close()
					if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
						return err
					}
					if err == nil && string(magic[:4]) == "RAWA" && magic[4] == 1 {
						wal = true
					}
				}
				if config && wal {
					return nil
				}
			}
			if remaining == 0 && readErr == nil {
				return errors.New("native metadata scan limit reached")
			}
			return nil
		}
		for _, name := range []string{"rabbitmq_metadata", "coordination"} {
			if err := visit(filepath.Join("rabbitmq", "rabbit@"+c.Member, name), 0); err != nil {
				return false, err
			}
		}
		return config && wal, nil
	}
	return false, nil
}
