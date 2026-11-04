// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"syscall"
)

// Each child has its own writer; inspect it only after Cmd.Wait joins its stderr
// copier. Retain a bounded tail even if a native tool emits unlimited output.
type backupStderrTail struct {
	data [4096]byte
	used int
}

func (b *backupStderrTail) Write(p []byte) (int, error) {
	n := len(p)
	if n >= len(b.data) {
		copy(b.data[:], p[n-len(b.data):])
		b.used = len(b.data)
	} else {
		if b.used+n > len(b.data) {
			keep := len(b.data) - n
			copy(b.data[:keep], b.data[b.used-keep:b.used])
			b.used = keep
		}
		copy(b.data[b.used:], p)
		b.used += n
	}
	return n, nil
}

// Never echo native text, including unknown/obsolete credentials or URL query
// keys. Substitution-based redaction cannot safely handle a truncated/multiline
// secret. Normalize the retained tail into fixed, allowlisted summaries instead.
func (b *backupStderrTail) diagnostic() string {
	text := strings.ToLower(string(b.data[:b.used]))
	var summaries []string
	for _, candidate := range []struct{ match, summary string }{
		{"lost connection to mysql server", "mysqld connection lost"},
		{"lost connection to server during query", "mysqld connection lost"},
		{"mysql server has gone away", "mysqld connection lost"},
		{"connection refused", "native connection refused"},
		{"access denied", "access denied"},
		{"out of memory", "native out of memory"},
		{"cannot allocate memory", "native out of memory"},
		{"no space left on device", "no space left on device"},
		{"permission denied", "permission denied"},
		{"failed to upload", "object upload failed"},
		{"upload failed", "object upload failed"},
		{"ssl certificate problem", "TLS certificate verification failed"},
		{"certificate verify failed", "TLS certificate verification failed"},
	} {
		if strings.Contains(text, candidate.match) {
			duplicate := false
			for _, summary := range summaries {
				duplicate = duplicate || summary == candidate.summary
			}
			if !duplicate {
				summaries = append(summaries, candidate.summary)
			}
		}
	}
	if len(summaries) == 0 {
		if b.used == 0 {
			return "stderr empty"
		}
		return "stderr tail: unclassified native output omitted"
	}
	return "stderr tail: " + strings.Join(summaries, ", ")
}

func backupProcessDiagnostic(name string, cmd *exec.Cmd, err error, cleanup bool, stderr *backupStderrTail) string {
	status := "completion unavailable"
	if cmd.ProcessState != nil {
		if native, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && native.Signaled() {
			signal := fmt.Sprintf("signal %d", native.Signal())
			switch native.Signal() {
			case syscall.SIGKILL:
				signal = "SIGKILL (possible OOM"
				if cleanup {
					signal += " or pipeline cleanup"
				}
				signal += "; not confirmed)"
			case syscall.SIGTERM:
				signal = "SIGTERM"
			case syscall.SIGABRT:
				signal = "SIGABRT"
			case syscall.SIGSEGV:
				signal = "SIGSEGV"
			}
			status = "killed by " + signal
		} else {
			status = fmt.Sprintf("exit status %d", cmd.ProcessState.ExitCode())
		}
	} else if err == nil {
		status = "completed"
	}
	return name + " " + status + "; " + stderr.diagnostic()
}

func backupStreamDiagnostic(err error) string {
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "truncated stream"
	case errors.Is(err, syscall.EPIPE):
		return "upload pipe closed"
	}
	// These are internal parser/manifest constants, never child-provided text.
	switch err.Error() {
	case "backup_stream_magic", "backup_stream_type", "backup_stream_path", "backup_stream_length", "invalid physical backup manifest", "invalid physical backup object":
		return err.Error()
	default:
		return "stream I/O failed"
	}
}
