// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func nativeXBFrame(kind byte, name string, payload []byte, offset uint64, sparse []byte) []byte {
	var frame bytes.Buffer
	frame.WriteString("XBSTCK01")
	frame.WriteByte(0)
	frame.WriteByte(kind)
	binary.Write(&frame, binary.LittleEndian, uint32(len(name)))
	frame.WriteString(name)
	if kind != 'E' {
		if kind == 'S' {
			binary.Write(&frame, binary.LittleEndian, uint32(len(sparse)/8))
		}
		binary.Write(&frame, binary.LittleEndian, uint64(len(payload)))
		binary.Write(&frame, binary.LittleEndian, offset)
		sum := crc32.NewIEEE()
		sum.Write(sparse)
		sum.Write(payload)
		binary.Write(&frame, binary.LittleEndian, sum.Sum32())
		frame.Write(sparse)
		frame.Write(payload)
	}
	return frame.Bytes()
}

type fragmentedBackupReader struct{ source io.Reader }

func (r fragmentedBackupReader) Read(p []byte) (int, error) {
	if len(p) > 7 {
		p = p[:7]
	}
	return r.source.Read(p)
}

func TestCaptureBackupStreamMatchesNativeFrameObjects(t *testing.T) {
	data := bytes.Repeat([]byte("physical bytes"), 24000)
	sparse := make([]byte, 8)
	binary.LittleEndian.PutUint32(sparse[:4], 4096)
	binary.LittleEndian.PutUint32(sparse[4:], 3)
	frames := []struct {
		name string
		raw  []byte
	}{
		{"ibdata1", nativeXBFrame('P', "ibdata1", data, 0, nil)},
		{"backup-my.cnf", nativeXBFrame('P', "backup-my.cnf", []byte("[mysqld]\nserver_uuid="+recoverySID+"\n"), 0, nil)},
		{"auto.cnf", nativeXBFrame('P', "auto.cnf", []byte("[auto]\nserver-uuid="+recoverySID+"\n"), 0, nil)},
		{"sparse.ibd", nativeXBFrame('S', "sparse.ibd", []byte("abc"), 0, sparse)},
		{"ibdata1", nativeXBFrame('P', "ibdata1", []byte("tail"), uint64(len(data)), nil)},
		{"backup-my.cnf", nativeXBFrame('E', "backup-my.cnf", nil, 0, nil)},
		{"auto.cnf", nativeXBFrame('E', "auto.cnf", nil, 0, nil)},
		{"ibdata1", nativeXBFrame('E', "ibdata1", nil, 0, nil)},
		{"sparse.ibd", nativeXBFrame('E', "sparse.ibd", nil, 0, nil)},
	}
	var input, expectedMetadata bytes.Buffer
	for _, frame := range frames {
		input.Write(frame.raw)
		if frame.name == "backup-my.cnf" {
			expectedMetadata.Write(frame.raw)
		}
	}
	var cloud, metadata bytes.Buffer
	manifest, err := captureBackupStream(fragmentedBackupReader{bytes.NewReader(input.Bytes())}, &cloud, &metadata, "repo/base/full")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(cloud.Bytes(), input.Bytes()) || !bytes.Equal(metadata.Bytes(), expectedMetadata.Bytes()) {
		t.Fatal("source stream or trusted scalar metadata changed")
	}
	if len(manifest.Objects) != len(frames) {
		t.Fatal("missing source frame hashes")
	}
	indices := map[string]uint64{}
	for _, frame := range frames {
		index := indices[frame.name]
		indices[frame.name]++
		keySuffix := ".00000000000000000000"
		if index == 1 {
			keySuffix = ".00000000000000000001"
		}
		if index == 2 {
			keySuffix = ".00000000000000000002"
		}
		key := "repo/base/full/" + frame.name + keySuffix
		sum := sha256.Sum256(frame.raw)
		found := false
		for _, object := range manifest.Objects {
			if object.Key == key {
				found = object.Size == int64(len(frame.raw)) && object.SHA256 == hex.EncodeToString(sum[:])
			}
		}
		if !found {
			t.Fatalf("native source frame was not bound: %s", key)
		}
	}
}

func TestCaptureBackupStreamRejectsUnboundedOrTruncatedFrames(t *testing.T) {
	valid := nativeXBFrame('P', "ibdata1", []byte("source"), 0, nil)
	badLength := append([]byte{}, valid...)
	binary.LittleEndian.PutUint64(badLength[14+len("ibdata1"):], ^uint64(0))
	badPath := append([]byte{}, valid...)
	binary.LittleEndian.PutUint32(badPath[10:14], 512)
	for _, source := range [][]byte{valid[:13], valid[:len(valid)-1], badLength, badPath, []byte(strings.Repeat("x", 14))} {
		if _, err := captureBackupStream(bytes.NewReader(source), io.Discard, io.Discard, "repo/base/full"); err == nil {
			t.Fatal("invalid source framing was accepted")
		}
	}
}

func TestRepositoryCannotBlessReplacementBeforeManifestSealing(t *testing.T) {
	frame := nativeXBFrame('P', "ibdata1", []byte("trusted native source"), 0, nil)
	manifest, err := captureBackupStream(bytes.NewReader(frame), io.Discard, io.Discard, "repo/mysql/cluster/base/full")
	if err != nil {
		t.Fatal(err)
	}
	objects := map[string][]byte{manifest.Objects[0].Key: nativeXBFrame('P', "ibdata1", bytes.Repeat([]byte("x"), len("trusted native source")), 0, nil)}
	repo := manifestS3Fixture(t, objects)
	client, err := s3Client(repo)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := createBackupManifest(t.Context(), client, repo, manifest); err == nil {
		t.Fatal("mutable repository bytes became their own trusted checksum")
	}
}

func TestBackupManifestPipelinePreservesSourceAndReapsSinkFailure(t *testing.T) {
	frame := nativeXBFrame('P', "backup-my.cnf", []byte("[mysqld]\nserver_uuid="+recoverySID+"\n"), 0, nil)
	name := filepath.Join(t.TempDir(), "source.xbstream")
	if err := os.WriteFile(name, frame, 0600); err != nil {
		t.Fatal(err)
	}
	for _, fail := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		source := exec.CommandContext(ctx, "cat", name)
		sink := exec.CommandContext(ctx, "cat")
		if fail {
			sink = exec.CommandContext(ctx, "false")
		}
		var output, metadata bytes.Buffer
		sink.Stdout = &output
		manifest, err := backupManifestPipeline(source, sink, &metadata, "repo/base/full")
		cancel()
		if fail {
			if err == nil {
				t.Fatal("failed upload consumer accepted")
			}
		} else if err != nil || len(manifest.Objects) != 1 || !bytes.Equal(output.Bytes(), frame) || !bytes.Equal(metadata.Bytes(), frame) {
			t.Fatalf("source pipeline changed: objects=%d err=%v", len(manifest.Objects), err)
		}
	}
}
