// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

type commandError struct {
	code       int
	message    string
	httpStatus int
}

func (e *commandError) Error() string { return e.message }
func usageError(format string, args ...any) error {
	return &commandError{code: 2, message: fmt.Sprintf(format, args...)}
}

// ExitCode maps failures to stable shell exit statuses.
func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	var classified *commandError
	if errors.As(err, &classified) {
		return classified.code
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return 7
	}
	if strings.HasPrefix(err.Error(), "unknown command ") {
		return 2
	}
	return 1
}

// Run executes a fresh command and reports errors on stderr, including usage for
// command-line mistakes. A nil argument slice uses the process arguments.
func Run(ctx context.Context, o Options, args []string) int {
	cmd := NewCommand(o)
	if args == nil {
		args = os.Args[1:]
	}
	cmd.SetArgs(args)
	selected, err := cmd.ExecuteContextC(ctx)
	code := ExitCode(err)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), "karactl:", err)
		if code == 2 {
			if selected == nil {
				selected = cmd
			}
			fmt.Fprint(cmd.ErrOrStderr(), selected.UsageString())
		}
	}
	return code
}

func groupCommand(use, short string) *cobra.Command {
	return &cobra.Command{Use: use, Short: short, Annotations: map[string]string{"local": "true"}, RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return usageError("%s requires a subcommand", cmd.CommandPath())
		}
		return usageError("unknown subcommand %q for %s", args[0], cmd.CommandPath())
	}}
}

func apiFailure(status int, message string) error {
	code := 1
	switch status {
	case 400, 415, 422:
		code = 6
	case 401, 403:
		code = 5
	case 404:
		code = 3
	case 409:
		code = 4
	case 408, 504:
		code = 7
	}
	return &commandError{code: code, message: message, httpStatus: status}
}

func printPEM(w io.Writer, pem string) error {
	if pem == "" {
		return fmt.Errorf("CA certificate is unavailable")
	}
	_, err := fmt.Fprint(w, pem)
	if err == nil && !strings.HasSuffix(pem, "\n") {
		_, err = fmt.Fprintln(w)
	}
	return err
}
