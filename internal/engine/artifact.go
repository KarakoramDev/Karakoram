// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"io"
	"os"

	"github.com/KarakoramDev/Karakoram/internal/version"
)

// ExecutableArtifact streams the running control plane binary as a tar.gz named
// karakoram, matching the runtime deployment command without temporary files.
type ExecutableArtifact struct{}

// ArtifactIdentity supplies a cheap, stable identity without opening or hashing
// a deployment archive. Custom artifact sources may implement it as well.
func (ExecutableArtifact) ArtifactIdentity() string {
	return version.BuildIdentity()
}

func artifactIdentity(source ArtifactSource) string {
	if source, ok := source.(interface{ ArtifactIdentity() string }); ok {
		return source.ArtifactIdentity()
	}
	return ""
}

// Open streams the current executable as a deployment tar.gz.
func (ExecutableArtifact) Open(ctx context.Context) (io.ReadCloser, error) {
	path, err := os.Executable()
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	reader, writer := io.Pipe()
	stop := context.AfterFunc(ctx, func() { reader.CloseWithError(ctx.Err()) })
	go func() {
		defer stop()
		defer file.Close()
		gz := gzip.NewWriter(writer)
		tw := tar.NewWriter(gz)
		err := tw.WriteHeader(&tar.Header{Name: "karakoram", Mode: 0755, Size: stat.Size(), ModTime: stat.ModTime()})
		if err == nil {
			_, err = io.Copy(tw, file)
		}
		if e := tw.Close(); err == nil {
			err = e
		}
		if e := gz.Close(); err == nil {
			err = e
		}
		writer.CloseWithError(err)
	}()
	return reader, nil
}
