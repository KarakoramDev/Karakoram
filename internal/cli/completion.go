// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import "github.com/spf13/cobra"

func (a *app) addCompletion(root *cobra.Command) {
	root.CompletionOptions.DisableDefaultCmd = true
	group := groupCommand("completion", "Generate a shell completion script (no server or credentials required)")
	for _, shell := range []string{"bash", "zsh", "fish", "powershell"} {
		name := shell
		cmd := &cobra.Command{Use: name, Short: "Generate " + name + " completion", Args: cobra.NoArgs}
		cmd.Flags().Bool("no-descriptions", false, "Disable completion descriptions")
		cmd.RunE = func(cmd *cobra.Command, _ []string) error {
			noDescriptions, _ := cmd.Flags().GetBool("no-descriptions")
			switch name {
			case "bash":
				return root.GenBashCompletionV2(a.opts.Out, !noDescriptions)
			case "zsh":
				if noDescriptions {
					return root.GenZshCompletionNoDesc(a.opts.Out)
				}
				return root.GenZshCompletion(a.opts.Out)
			case "fish":
				return root.GenFishCompletion(a.opts.Out, !noDescriptions)
			default:
				if noDescriptions {
					return root.GenPowerShellCompletion(a.opts.Out)
				}
				return root.GenPowerShellCompletionWithDesc(a.opts.Out)
			}
		}
		group.AddCommand(cmd)
	}
	root.AddCommand(group)
}

func wrapCommandArgs(cmd *cobra.Command) {
	if validate := cmd.Args; validate != nil {
		cmd.Args = func(c *cobra.Command, args []string) error {
			if err := validate(c, args); err != nil {
				return usageError("%s", err)
			}
			return nil
		}
	}
	for _, child := range cmd.Commands() {
		wrapCommandArgs(child)
	}
}
