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

func (a *app) confirm(cmd *cobra.Command, op string, args []string) error {
	yes, _ := cmd.Flags().GetBool("yes")
	into, _ := cmd.Flags().GetString("into-new")
	if !destructive(op) || yes || !a.terminal() || op == "createRestore" && into != "" {
		return nil
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
