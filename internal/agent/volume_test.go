// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package agent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func volumeFixture(t *testing.T, engine string, initialized bool) (*nodeAgent, agentapi.MemberConfig) {
	t.Helper()
	a := &nodeAgent{o: Options{Member: "member", DataDir: t.TempDir()}}
	c := agentapi.MemberConfig{Cluster: "demo", Member: "member", Engine: engine, Volume: &agentapi.VolumeIdentity{ClusterUID: "incarnation", Member: "member", Volume: "volume", VolumeID: "original-volume", Initialized: initialized}}
	return a, c
}
func volumeFile(t *testing.T, a *nodeAgent, name string, data []byte) {
	t.Helper()
	path := filepath.Join(a.o.DataDir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}
func legacyNativeVolume(t *testing.T, a *nodeAgent, engine string) {
	t.Helper()
	switch engine {
	case "":
		volumeFile(t, a, "db/WiredTiger", []byte("WiredTiger 11.3\n"))
		volumeFile(t, a, "db/WiredTiger.wt", make([]byte, 4096))
	case "weaviate":
		volumeFile(t, a, "weaviate/raft/raft.db", []byte{1})
		volumeFile(t, a, "weaviate/schema.db", []byte{1})
	case "postgres":
		volumeFile(t, a, "postgres/PG_VERSION", []byte("18\n"))
		volumeFile(t, a, "postgres/global/pg_control", make([]byte, 8192))
	case "mysql":
		volumeFile(t, a, "mysql/auto.cnf", []byte("[auto]\nserver-uuid=11111111-2222-3333-4444-555555555555\n"))
		volumeFile(t, a, "mysql/mysql.ibd", make([]byte, 16384))
	case "neo4j":
		metadata := make([]byte, 8192)
		copy(metadata[65:], []byte("record\x01\x07aligned"))
		volumeFile(t, a, "karakoram/neo4j/data/databases/system/neostore", metadata)
		volumeFile(t, a, "karakoram/neo4j/data/databases/system/neostore.nodestore.db", make([]byte, 8192))
		volumeFile(t, a, "karakoram/neo4j/data/databases/system/database_lock", nil)
	case "elasticsearch":
		volumeFile(t, a, "karakoram/data/_state/node-1.st", []byte{0x3f, 0xd7, 0x6c, 0x17, 1})
	case "openbao":
		header := make([]byte, 8192)
		copy(header[16:], []byte{0xed, 0xda, 0x0c, 0xed, 2, 0, 0, 0})
		volumeFile(t, a, "openbao/raft/raft.db", header)
		volumeFile(t, a, "openbao/raft/vault.db", header)
	case "rabbitmq":
		volumeFile(t, a, "rabbitmq/rabbit@member/rabbitmq_metadata/UID/config", []byte("#{id => {rabbitmq_metadata, 'rabbit@member'}}."))
		volumeFile(t, a, "rabbitmq/rabbit@member/rabbitmq_metadata/0000000000000001.wal", []byte{'R', 'A', 'W', 'A', 1, 1})
	}
}
func TestVolumeSafetyInitializedEmptyVolumeRefusesEveryFamily(t *testing.T) {
	for _, engine := range []string{"", "postgres", "mysql", "elasticsearch", "rabbitmq", "openbao", "neo4j", "weaviate"} {
		t.Run(engine, func(t *testing.T) {
			a, c := volumeFixture(t, engine, true)
			if err := a.volumeSafety(t.Context(), c); err == nil || !strings.Contains(err.Error(), "VolumeLost") {
				t.Fatal("empty initialized volume allowed", err)
			}
			if state := a.volumeObservation(); state == nil || !state.Lost || state.Verified {
				t.Fatal("loss not observable", state)
			}
			if _, err := os.Stat(filepath.Join(a.o.DataDir, volumeSentinel)); !os.IsNotExist(err) {
				t.Fatal("failed check created sentinel", err)
			}
		})
	}
}
func TestVolumeSafetyFreshLegacyAndChangedIdentity(t *testing.T) {
	for _, engine := range []string{"", "postgres", "mysql", "elasticsearch", "rabbitmq", "openbao", "neo4j", "weaviate"} {
		t.Run(engine, func(t *testing.T) {
			a, c := volumeFixture(t, engine, false)
			if err := a.volumeSafety(t.Context(), c); err != nil {
				t.Fatal("fresh authorized identity rejected", err)
			}
			if state := a.volumeObservation(); state == nil || !state.Verified || state.Lost {
				t.Fatal(state)
			}
			// Missing sentinel before the CP's next observation is still loss, not a
			// second chance to initialize an empty volume under the old identity.
			if err := os.Remove(filepath.Join(a.o.DataDir, volumeSentinel)); err != nil {
				t.Fatal(err)
			}
			if err := a.volumeSafety(t.Context(), c); err == nil {
				t.Fatal("same-process loss before CP ack reinitialized")
			}
			legacyNativeVolume(t, a, engine)
			if err := a.volumeSafety(t.Context(), c); err != nil {
				t.Fatal("retained native data not adopted", err)
			}
			restarted := &nodeAgent{o: a.o}
			c.Volume.Initialized = true
			if err := restarted.volumeSafety(t.Context(), c); err != nil {
				t.Fatal("restart rejected original identity", err)
			}
			c.Volume.VolumeID = "different-volume"
			if err := restarted.volumeSafety(t.Context(), c); err == nil {
				t.Fatal("different data incarnation accepted")
			}
		})
	}
}
func TestVolumeSafetyLegacyRequiresNativeMetadata(t *testing.T) {
	for _, engine := range []string{"", "postgres", "mysql", "elasticsearch", "rabbitmq", "openbao", "neo4j", "weaviate"} {
		t.Run(engine, func(t *testing.T) {
			a, c := volumeFixture(t, engine, true)
			volumeFile(t, a, "karakoram/config.json", []byte(`{"cluster":"demo"}`))
			volumeFile(t, a, "postgres/PG_VERSION", []byte("invalid"))
			volumeFile(t, a, "db/WiredTiger.wt", []byte("truncated"))
			volumeFile(t, a, "karakoram/data/_state/node-1.st", []byte("invalid"))
			volumeFile(t, a, "rabbitmq/rabbit@member/rabbitmq_metadata/config", []byte("wrong node"))
			volumeFile(t, a, "mysql/auto.cnf", []byte("[auto]\nserver-uuid=invalid\n"))
			volumeFile(t, a, "mysql/mysql.ibd", []byte("truncated"))
			volumeFile(t, a, "karakoram/neo4j/data/databases/system/neostore", []byte("invalid"))
			volumeFile(t, a, "karakoram/neo4j/data/databases/system/database_lock", nil)
			if err := a.volumeSafety(t.Context(), c); err == nil {
				t.Fatal("cache/directory/corrupt marker adopted")
			}
		})
	}
}

func TestOpenBaoVolumeRequiresBothNativeDatabases(t *testing.T) {
	for _, scenario := range []string{"missing-vault", "truncated-raft", "invalid-vault"} {
		t.Run(scenario, func(t *testing.T) {
			a, c := volumeFixture(t, "openbao", true)
			legacyNativeVolume(t, a, "openbao")
			switch scenario {
			case "missing-vault":
				if err := os.Remove(filepath.Join(a.o.DataDir, "openbao/raft/vault.db")); err != nil {
					t.Fatal(err)
				}
			case "truncated-raft":
				volumeFile(t, a, "openbao/raft/raft.db", []byte{0xed, 0xda, 0x0c, 0xed})
			case "invalid-vault":
				volumeFile(t, a, "openbao/raft/vault.db", make([]byte, 8192))
			}
			if err := a.volumeSafety(t.Context(), c); err == nil {
				t.Fatal("partial or invalid native data was adopted")
			}
		})
	}
}
func TestVolumeSafetySentinelCorruptionAndStateless(t *testing.T) {
	a, c := volumeFixture(t, "postgres", true)
	legacyNativeVolume(t, a, "postgres")
	volumeFile(t, a, volumeSentinel, []byte("corrupt"))
	if err := a.volumeSafety(t.Context(), c); err == nil {
		t.Fatal("corrupt sentinel silently adopted")
	}
	c.Volume = nil
	if err := a.volumeSafety(t.Context(), c); err != nil {
		t.Fatal("stateless runtime gated", err)
	}
}

func TestVolumeSafetyConsumesCachedFreshAuthorization(t *testing.T) {
	a, c, _ := fakeNode(t, "")
	c.Maintenance = true
	c.Volume = &agentapi.VolumeIdentity{ClusterUID: "uid", Member: c.Member, Volume: "volume", VolumeID: "original"}
	if _, err := a.apply(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	if c.Volume.Initialized {
		t.Fatal("apply mutated caller's shared identity pointer")
	}
	data, err := os.ReadFile(filepath.Join(a.o.DataDir, "karakoram", "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	var cached agentapi.MemberConfig
	if err := json.Unmarshal(data, &cached); err != nil {
		t.Fatal(err)
	}
	if cached.Volume == nil || !cached.Volume.Initialized {
		t.Fatal("cached fresh authorization can be replayed")
	}
	if err := os.Remove(filepath.Join(a.o.DataDir, volumeSentinel)); err != nil {
		t.Fatal(err)
	}
	restarted := &nodeAgent{o: a.o}
	if err := restarted.volumeSafety(t.Context(), cached); err == nil {
		t.Fatal("cached config authorized empty volume after restart")
	}
}

func TestVolumeSafetyUnknownStorageIsNotConfirmedLoss(t *testing.T) {
	for _, scenario := range []string{"cancelled", "incomplete_identity", "unreadable_sentinel", "unreadable_native_metadata", "oversized_sentinel"} {
		t.Run(scenario, func(t *testing.T) {
			a, c := volumeFixture(t, "postgres", true)
			ctx := t.Context()
			switch scenario {
			case "cancelled":
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
			case "incomplete_identity":
				c.Volume.VolumeID = ""
			case "unreadable_sentinel", "unreadable_native_metadata":
				name := volumeSentinel
				if scenario == "unreadable_native_metadata" {
					name = "postgres/PG_VERSION"
				}
				target := filepath.Join(t.TempDir(), "outside")
				if err := os.WriteFile(target, []byte("18"), 0600); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(a.o.DataDir, name)
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			case "oversized_sentinel":
				volumeFile(t, a, volumeSentinel, make([]byte, 8193))
			}
			if err := a.volumeSafety(ctx, c); err == nil {
				t.Fatal("unknown storage state started engine")
			}
			state := a.volumeObservation()
			if state == nil || state.Lost || state.Verified || state.Message == "" {
				t.Fatal("unknown I/O was classified as destructive replacement evidence", state)
			}
		})
	}
}
