// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/term"
)

func readInput(r io.Reader) ([]byte, error) { return io.ReadAll(r) }
func password(r io.Reader, w io.Writer) (string, error) {
	fmt.Fprint(w, "Password: ")
	if f, ok := r.(*os.File); ok && term.IsTerminal(int(f.Fd())) {
		b, err := term.ReadPassword(int(f.Fd()))
		fmt.Fprintln(w)
		return string(b), err
	}
	line, err := bufio.NewReader(r).ReadString('\n')
	if err != nil && err != io.EOF {
		return "", err
	}
	fmt.Fprintln(w)
	return strings.TrimRight(line, "\r\n"), nil
}
func (a *app) addAuth(root *cobra.Command) {
	login := &cobra.Command{Use: "login", Short: "Log in with a password, create an API token, and save the context", Args: cobra.NoArgs}
	login.Flags().String("username", "admin", "Local account name")
	annotate(login, "login", "createAPIToken", "logout")
	login.RunE = func(cmd *cobra.Command, args []string) error {
		username, _ := cmd.Flags().GetString("username")
		pw, err := password(a.opts.In, a.opts.Err)
		if err != nil {
			return err
		}
		if pw == "" {
			return fmt.Errorf("password cannot be empty")
		}
		// Existing bearer credentials must not override the newly authenticated session.
		a.token = ""
		session, err := a.request(cmd.Context(), "login", nil, nil, map[string]any{"username": username, "password": pw})
		if err != nil {
			return err
		}
		a.csrf, _ = nested(session, "csrfToken").(string)
		if a.csrf == "" {
			return fmt.Errorf("login response did not include a CSRF token")
		}
		host, err := os.Hostname()
		if err != nil {
			return err
		}
		created, err := a.request(cmd.Context(), "createAPIToken", nil, nil, map[string]any{"name": "karactl-" + host})
		if err != nil {
			return err
		}
		secret, _ := nested(created, "secret").(string)
		if secret == "" {
			return fmt.Errorf("token creation response did not include a secret")
		}
		id, _ := nested(created, "token", "id").(string)
		a.config.Contexts[a.selected] = Context{Server: a.server, Token: secret, TokenID: id}
		a.config.CurrentContext = a.selected
		if err = SaveConfig(a.opts.ConfigPath, a.config); err != nil {
			return fmt.Errorf("saving credentials: %w", err)
		}
		// End the temporary password session; subsequent commands use only the API token.
		if _, err = a.request(cmd.Context(), "logout", nil, nil, nil); err != nil {
			return fmt.Errorf("context saved, but closing login session failed: %w", err)
		}
		fmt.Fprintf(a.opts.Out, "Logged in as %s; context %s saved.\n", username, a.selected)
		return nil
	}
	logout := &cobra.Command{Use: "logout", Short: "Revoke the saved CLI token and remove credentials from the context", Args: cobra.NoArgs}
	logout.Flags().Bool("local", false, "Only remove local credentials (server token remains active)")
	annotate(logout, "logout", "deleteAPIToken")
	logout.RunE = func(cmd *cobra.Command, args []string) error {
		local, _ := cmd.Flags().GetBool("local")
		c := a.config.Contexts[a.selected]
		if !local {
			if _, err := a.request(cmd.Context(), "logout", nil, nil, nil); err != nil {
				return err
			}
			if c.TokenID != "" && a.token == c.Token {
				if _, err := a.request(cmd.Context(), "deleteAPIToken", []string{c.TokenID}, nil, nil); err != nil {
					return err
				}
			}
		}
		c.Token = ""
		c.TokenID = ""
		a.config.Contexts[a.selected] = c
		if err := SaveConfig(a.opts.ConfigPath, a.config); err != nil {
			return err
		}
		fmt.Fprintf(a.opts.Out, "Logged out of context %s.\n", a.selected)
		return nil
	}
	root.AddCommand(login, logout)
}
