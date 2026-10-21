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
	datamcpagent "github.com/KarakoramDev/Karakoram/internal/agent/datamcp"
	elasticagent "github.com/KarakoramDev/Karakoram/internal/agent/elastic"
	postgresagent "github.com/KarakoramDev/Karakoram/internal/agent/postgres"
	rabbitmqagent "github.com/KarakoramDev/Karakoram/internal/agent/rabbitmq"
	supabaseagent "github.com/KarakoramDev/Karakoram/internal/agent/supabase"
	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/relay"
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

const usage = "Usage: karakoram server | agent [readiness [args...]] | version | admin reset-password <account>\nServer recovery reads KARAKORAM_RECOVERY_PASSWORD and optional KARAKORAM_RECOVERY_ACCOUNT (default admin) once per changed value.\nOffline reset reads KARAKORAM_RESET_PASSWORD, or prompts securely on a terminal. Stop the server first.\n"

// errUsage reports invalid command-line arguments; main prints the usage text.
var errUsage = errors.New("invalid arguments")

func main() {
	if len(os.Args) == 2 && os.Args[1] == "agent" {
		if handled, code, err := agent.ReexecIfPID1(); handled {
			if err != nil {
				fmt.Fprintln(os.Stderr, "karakoram init:", err)
			}
			os.Exit(code)
		}
	}
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
		if len(args) > 1 && args[1] != "readiness" {
			return errUsage
		}
		if len(args) > 1 {
			opts, rest := readinessOptions(args[2:])
			return agent.Readiness(ctx, opts, rest, rabbitmqagent.New(), elasticagent.NewEngine(), postgresagent.New(), &supabaseagent.Runner{}, datamcpagent.New())
		}
		o, err := agent.OptionsFromEnv()
		if err != nil {
			return err
		}
		return agent.Run(ctx, o, rabbitmqagent.New(), elasticagent.NewEngine(), postgresagent.New(), &supabaseagent.Runner{}, datamcpagent.New())
	case "relay":
		if len(args) > 2 || (len(args) == 2 && args[1] != "readiness") {
			return errUsage
		}
		if len(args) == 2 {
			return relay.Readiness(ctx, relay.Options{})
		}
		o, err := relay.OptionsFromEnv()
		if err != nil {
			return err
		}
		o.Logger = slog.New(slog.NewJSONHandler(errOut, nil))
		return relay.Run(ctx, o)
	case "admin":
		if len(args) != 3 || args[1] != "reset-password" {
			return errUsage
		}
		return resetPassword(ctx, args[2], out, errOut)
	default:
		return errUsage
	}
}

// readinessOptions accepts explicit probe settings without member credentials.
// Zerops readiness exec checks do not inherit the service environment.
func readinessOptions(args []string) (agent.Options, []string) {
	opts := agent.Options{Engine: os.Getenv("KARAKORAM_ENGINE")}
	if len(args) > 0 && strings.HasPrefix(args[0], "--engine=") {
		opts.Engine, args = strings.TrimPrefix(args[0], "--engine="), args[1:]
	}
	if len(args) > 0 && strings.HasPrefix(args[0], "--data-dir=") {
		opts.DataDir, args = strings.TrimPrefix(args[0], "--data-dir="), args[1:]
	}
	return opts, args
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
