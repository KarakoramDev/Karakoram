// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Command karakoram runs the control plane, member agent, readiness probe, and offline recovery.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/server"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/KarakoramDev/Karakoram/internal/version"
	"golang.org/x/term"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
)

const usage = "Usage: karakoram server | agent [readiness] | version | admin reset-password <account>\nServer recovery reads KARAKORAM_RECOVERY_PASSWORD and optional KARAKORAM_RECOVERY_ACCOUNT (default admin) once per changed value.\nOffline reset reads KARAKORAM_RESET_PASSWORD, or prompts securely on a terminal. Stop the server first.\n"

// errUsage reports invalid command-line arguments; main prints the usage text.
var errUsage = errors.New("invalid arguments")

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "karakoram:", err)
		if errors.Is(err, errUsage) {
			fmt.Fprint(os.Stderr, usage)
		}
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out, errOut io.Writer) error {
	if len(args) == 0 {
		return errUsage
	}
	switch args[0] {
	case "help", "--help", "-h":
		_, err := fmt.Fprint(out, usage)
		return err
	case "version":
		if len(args) != 1 {
			return errUsage
		}
		_, err := fmt.Fprintln(out, version.String())
		return err
	case "server":
		if len(args) != 1 {
			return errUsage
		}
		cfg, err := server.ConfigFromEnv()
		if err != nil {
			return err
		}
		return server.Run(ctx, cfg, slog.New(slog.NewJSONHandler(errOut, nil)))
	case "agent":
		if len(args) > 2 || (len(args) == 2 && args[1] != "readiness") {
			return errUsage
		}
		if len(args) == 2 {
			// Readiness only needs a local TCP probe, not member credentials.
			return agent.Readiness(ctx, agent.Options{})
		}
		o, err := agent.OptionsFromEnv()
		if err != nil {
			return err
		}
		return agent.Run(ctx, o)
	case "admin":
		if len(args) != 3 || args[1] != "reset-password" {
			return errUsage
		}
		return resetPassword(ctx, args[2], out, errOut)
	default:
		return errUsage
	}
}
func resetPassword(ctx context.Context, account string, out, errOut io.Writer) error {
	dir := os.Getenv("KARAKORAM_DATA_DIR")
	if dir == "" {
		dir = "/mnt/karakoramdata"
	}
	path := filepath.Join(dir, "karakoram.db")
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("open existing database: %w", err)
	}
	password := os.Getenv("KARAKORAM_RESET_PASSWORD")
	if password == "" {
		fmt.Fprint(errOut, "New password: ")
		if term.IsTerminal(int(os.Stdin.Fd())) {
			raw, err := term.ReadPassword(int(os.Stdin.Fd()))
			fmt.Fprintln(errOut)
			if err != nil {
				return err
			}
			password = string(raw)
		} else {
			line, err := bufio.NewReader(io.LimitReader(os.Stdin, 4096)).ReadString('\n')
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
			password = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		}
	}
	sealer, err := secrets.FromEnv()
	if err != nil {
		return err
	}
	st, err := store.Open(path, sealer)
	if err != nil {
		return err
	}
	defer st.Close()
	service, err := auth.New(auth.Options{Store: st, Sealer: sealer})
	if err != nil {
		return err
	}
	if err = service.ResetPassword(ctx, account, password); err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "Password reset for %s; a password change is required on next login.\n", account)
	return err
}
