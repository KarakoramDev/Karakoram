// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
)

func commandEnv(s neo4jspec.Spec, conf string) []string {
	return []string{"JAVA_HOME=" + s.Install.JavaHome, "NEO4J_CONF=" + conf, "NEO4J_HOME=" + s.Install.Neo4jHome, "PATH=" + filepath.Join(s.Install.JavaHome, "bin") + ":/usr/bin:/bin", "LANG=C.UTF-8"}
}
func nativeCommand(ctx context.Context, s neo4jspec.Spec, conf, binary string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, filepath.Join(s.Install.Neo4jHome, "bin", binary), args...)
	cmd.Env = commandEnv(s, conf)
	cmd.Dir = s.Install.Neo4jHome
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = 5 * time.Second
	return cmd
}
func adminCommand(ctx context.Context, s neo4jspec.Spec, conf string, args ...string) *exec.Cmd {
	return nativeCommand(ctx, s, conf, "neo4j-admin", args...)
}

type boundedOutput struct {
	bytes.Buffer
	overflow bool
}

func (b *boundedOutput) Write(p []byte) (int, error) {
	n := len(p)
	if b.Len()+n > 1<<20 {
		b.overflow = true
		return n, nil
	}
	_, _ = b.Buffer.Write(p)
	return n, nil
}
func serverCommand(ctx context.Context, s neo4jspec.Spec, conf string, ownedPID ...int) (agent.ProcessSpec, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := nativeCommand(ctx, s, conf, "neo4j", "console", "--dry-run")
	var out, stderr boundedOutput
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	if out.overflow || stderr.overflow {
		return agent.ProcessSpec{}, errors.New("Neo4j command derivation failed")
	}
	if runErr != nil {
		// The launcher emits the complete dry-run argv before checking its
		// PID file. An owned running JVM is expected during config rendering;
		// only that exact PID diagnostic permits its otherwise nonzero exit.
		if len(ownedPID) != 1 || cmd.ProcessState == nil || !ownedDryRunExit(cmd.ProcessState.ExitCode(), stderr.String(), ownedPID[0]) {
			return agent.ProcessSpec{}, errors.New("Neo4j command derivation failed")
		}
	}
	argv, err := splitCommand(strings.TrimSpace(out.String()))
	if err != nil || len(argv) < 2 || !slices.Contains(argv, "org.neo4j.server.Neo4jCommunity") {
		return agent.ProcessSpec{}, errors.New("invalid Neo4j server command")
	}
	// Refuse launcher diagnostics or a different executable, even if syntactically
	// valid shell text. No shell ever evaluates the resulting words.
	java := filepath.Join(s.Install.JavaHome, "bin", "java")
	if filepath.Clean(argv[0]) != filepath.Clean(java) {
		return agent.ProcessSpec{}, errors.New("unexpected Neo4j JVM executable")
	}
	return agent.ProcessSpec{Binary: java, Args: argv[1:], Env: commandEnv(s, conf), Dir: s.Install.Neo4jHome, StopSignal: syscall.SIGTERM, StopTimeout: 45 * time.Second}, nil
}

func ownedDryRunExit(exitCode int, stderr string, pid int) bool {
	if exitCode != 1 || pid <= 0 {
		return false
	}
	for _, line := range strings.Split(stderr, "\n") {
		text, ok := strings.CutPrefix(strings.TrimSpace(line), "Neo4j is already running (pid:")
		if ok && strings.HasSuffix(text, ").") && strings.TrimSpace(strings.TrimSuffix(text, ").")) == strconv.Itoa(pid) {
			return true
		}
	}
	return false
}

// splitCommand parses POSIX shell words, not shell programs: expansion, command
// substitution and operators are never evaluated. Neo4j emits shell-quoted argv.
func splitCommand(line string) ([]string, error) {
	var words []string
	var word strings.Builder
	var quote rune
	escaped, started := false, false
	for _, c := range line {
		if escaped {
			if quote == '"' && !strings.ContainsRune("$`\"\\\n", c) {
				word.WriteRune('\\')
			}
			if c != '\n' {
				word.WriteRune(c)
			}
			escaped = false
			started = true
			continue
		}
		if quote == '\'' {
			if c == '\'' {
				quote = 0
			} else {
				word.WriteRune(c)
			}
			continue
		}
		if quote == '"' {
			if c == '"' {
				quote = 0
				continue
			}
			if c == '\\' {
				escaped = true
				continue
			}
			word.WriteRune(c)
			continue
		}
		switch {
		case c == '\'' || c == '"':
			quote = c
			started = true
		case c == '\\':
			escaped = true
			started = true
		case unicode.IsSpace(c):
			if started {
				words = append(words, word.String())
				word.Reset()
				started = false
			}
		case strings.ContainsRune(";|&<>`", c):
			return nil, errors.New("shell operator in JVM command")
		default:
			word.WriteRune(c)
			started = true
		}
	}
	if escaped || quote != 0 {
		return nil, errors.New("unterminated JVM command quoting")
	}
	if started {
		words = append(words, word.String())
	}
	return words, nil
}
