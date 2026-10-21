// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os/exec"
	"sort"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// xbcloud put stores each raw XBStream frame as path.<20-digit file index>.
// Observe those same source bytes before they enter S3; reading mutable S3 data
// to invent the expected checksum would let a repository writer bless tampering.
// Framing follows upstream xbstream_read.cc: 14-byte header, path, optional
// sparse-map count, payload length/offset/checksum, sparse map and payload.
func captureBackupStream(source io.Reader, cloud io.Writer, metadata io.Writer, prefix string) (myspec.BackupObjectManifest, error) {
	manifest := myspec.BackupObjectManifest{Version: 1, ObjectPrefix: prefix}
	indices := map[string]uint64{}
	var header [14 + 512 + 24]byte
	buffer := make([]byte, 64<<10)
	hasher := sha256.New()
	normalSink := io.MultiWriter(cloud, hasher)
	metadataSink := io.MultiWriter(cloud, hasher, metadata)
	limited := io.LimitedReader{R: source}
	var digest [sha256.Size]byte
	for {
		n, err := io.ReadFull(source, header[:14])
		if err == io.EOF && n == 0 {
			break
		}
		if err != nil {
			return manifest, err
		}
		if !bytes.Equal(header[:8], []byte("XBSTCK01")) {
			return manifest, errors.New("backup_stream_magic")
		}
		kind := header[9]
		if kind != 'P' && kind != 'S' && kind != 'E' && header[8]&1 == 0 {
			return manifest, errors.New("backup_stream_type")
		}
		pathLength := int(binary.LittleEndian.Uint32(header[10:14]))
		if pathLength >= 512 {
			return manifest, errors.New("backup_stream_path")
		}
		if _, err = io.ReadFull(source, header[14:14+pathLength]); err != nil {
			return manifest, err
		}
		name := string(header[14 : 14+pathLength])
		if strings.ContainsRune(name, 0) {
			return manifest, errors.New("backup_stream_path")
		}
		headerSize := 14 + pathLength
		var payload uint64
		if kind != 'E' {
			extra := 20
			if kind == 'S' {
				extra += 4
			}
			if _, err = io.ReadFull(source, header[headerSize:headerSize+extra]); err != nil {
				return manifest, err
			}
			position := headerSize
			var sparse uint64
			if kind == 'S' {
				sparse = uint64(binary.LittleEndian.Uint32(header[position:position+4])) * 8
				position += 4
			}
			payload = binary.LittleEndian.Uint64(header[position : position+8])
			if payload > math.MaxInt64-sparse-uint64(headerSize+extra) {
				return manifest, errors.New("backup_stream_length")
			}
			payload += sparse
			headerSize += extra
		}
		hasher.Reset()
		sink := normalSink
		if backupStreamMetadataFile(name) {
			sink = metadataSink
		}
		if _, err = sink.Write(header[:headerSize]); err != nil {
			return manifest, err
		}
		limited.N = int64(payload)
		copied, err := io.CopyBuffer(sink, &limited, buffer)
		if err != nil {
			return manifest, err
		}
		if copied != int64(payload) {
			return manifest, io.ErrUnexpectedEOF
		}
		index := indices[name]
		indices[name] = index + 1
		hasher.Sum(digest[:0])
		manifest.Objects = append(manifest.Objects, myspec.BackupObject{Key: fmt.Sprintf("%s/%s.%020d", prefix, name, index), Size: int64(headerSize) + copied, SHA256: hex.EncodeToString(digest[:])})
		if kind == 'E' {
			delete(indices, name)
		}
	}
	sort.Slice(manifest.Objects, func(i, j int) bool { return manifest.Objects[i].Key < manifest.Objects[j].Key })
	if err := manifest.Validate(); err != nil {
		return manifest, err
	}
	return manifest, nil
}

func backupStreamMetadataFile(name string) bool {
	switch strings.TrimPrefix(name, "./") {
	case "xtrabackup_checkpoints", "xtrabackup_binlog_info", "xtrabackup_info", "backup-my.cnf":
		return true
	}
	return false
}

func backupManifestPipeline(a, b *exec.Cmd, metadata io.Writer, prefix string) (myspec.BackupObjectManifest, error) {
	var empty myspec.BackupObjectManifest
	reader, err := a.StdoutPipe()
	if err != nil {
		return empty, errors.New("pipeline_start")
	}
	writer, err := b.StdinPipe()
	if err != nil {
		reader.Close()
		return empty, errors.New("pipeline_start")
	}
	if err = b.Start(); err != nil {
		reader.Close()
		writer.Close()
		return empty, errors.New("pipeline_start")
	}
	if err = a.Start(); err != nil {
		reader.Close()
		writer.Close()
		_ = b.Process.Kill()
		_ = b.Wait()
		return empty, errors.New("pipeline_start")
	}
	manifest, streamErr := captureBackupStream(reader, writer, metadata, prefix)
	reader.Close()
	writer.Close()
	if streamErr != nil {
		_ = a.Process.Kill()
		_ = b.Process.Kill()
	}
	sourceErr, cloudErr := a.Wait(), b.Wait()
	if streamErr != nil {
		return empty, errors.New("pipeline_stream_failed")
	}
	if sourceErr != nil {
		return empty, errors.New("pipeline_source_failed")
	}
	if cloudErr != nil {
		return empty, errors.New("pipeline_sink_failed")
	}
	return manifest, nil
}
