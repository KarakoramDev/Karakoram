// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// karactl provides the Karakoram REST API command-line interface.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/KarakoramDev/Karakoram/internal/cli"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	os.Exit(cli.Run(ctx, cli.Options{}, nil))
}
