// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"bufio"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"strings"
)

func destructive(op string) bool {
	return strings.HasPrefix(op, "delete") || op == "createRestore" || op == "replaceClusterMember" || op == "raiseClusterFCV" || op == "rotateSystemUser" || op == "rotateClusterTLS" || op == "rotateNamespaceToken" || op == "resetAccountPassword" || op == "cancelOperation"
}

// Payload input belongs entirely to the request, never to confirmation.
func (a *app) checkConfirmationInput(cmd *cobra.Command, op string) error {
	yes, _ := cmd.Flags().GetBool("yes")
	into, _ := cmd.Flags().GetString("into-new")
	if !destructive(op) || yes || op == "createRestore" && into != "" {
		return nil
	}
	for _, flag := range []string{"file", "token-file"} {
		if value, _ := cmd.Flags().GetString(flag); value == "-" {
			return usageError("pass --yes to confirm when --%s reads a payload from stdin; no request submitted", flag)
		}
	}
	return nil
}

func (a *app) confirm(cmd *cobra.Command, op string, args []string) error {
	yes, _ := cmd.Flags().GetBool("yes")
	into, _ := cmd.Flags().GetString("into-new")
	if !destructive(op) || yes || op == "createRestore" && into != "" {
		return nil
	}
	if !a.terminal() {
		line, err := bufio.NewReader(a.opts.In).ReadString('\n')
		if err != nil && err != io.EOF {
			return err
		}
		answer := strings.TrimSpace(line)
		if answer == "" {
			return usageError("confirmation required; pass --yes for non-interactive use; no request submitted")
		}
		if strings.EqualFold(answer, "y") || strings.EqualFold(answer, "yes") {
			return nil
		}
		return fmt.Errorf("confirmation declined; no request submitted")
	}
	name := ""
	if len(args) > 0 {
		name = args[len(args)-1]
	}
	if op == "createRestore" {
		name = args[0]
	}
	if name == "" {
		fmt.Fprintf(a.opts.Err, "Confirm %s? [y/N]: ", cmd.CommandPath())
	} else {
		fmt.Fprintf(a.opts.Err, "%s affects %s. Type the name to confirm: ", cmd.CommandPath(), name)
	}
	line, err := bufio.NewReader(a.opts.In).ReadString('\n')
	if err != nil && err != io.EOF {
		return err
	}
	answer := strings.TrimSpace(line)
	if name != "" && answer == name || name == "" && strings.EqualFold(answer, "y") {
		return nil
	}
	return fmt.Errorf("confirmation declined; no request submitted")
}
