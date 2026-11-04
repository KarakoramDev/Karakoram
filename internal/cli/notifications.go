// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

func notificationFlags(c *cobra.Command, op string) {
	if op != "createNotificationChannel" && op != "replaceNotificationChannel" {
		return
	}
	f := c.Flags()
	f.String("name", "", "Channel name (required on create)")
	f.String("type", "", "Channel type: webhook, slack or email")
	f.Bool("enabled", true, "Enable deliveries")
	f.StringArray("category", nil, "Category filter (repeatable; omitted means all)")
	// Override the global namespace selector: these are channel filters, not API scope.
	f.StringArray("namespace", nil, "Namespace filter (repeatable; omitted includes installation-wide events)")
	f.String("url", "", "Webhook destination URL (use a protected -f file if it contains credentials)")
	f.Bool("signing-secret-stdin", false, "Read webhook signing secret from stdin")
	f.Bool("webhook-url-stdin", false, "Read the secret Slack webhook URL from stdin")
	f.String("smtp-host", "", "SMTP host")
	f.Int("smtp-port", 587, "SMTP port")
	f.String("security", "starttls", "SMTP TLS mode: starttls or tls")
	f.String("username", "", "SMTP username")
	f.Bool("password-stdin", false, "Read SMTP password from stdin")
	f.String("from", "", "Email sender")
	f.StringArray("to", nil, "Email recipient (repeatable; 1–20)")
}

func (a *app) notificationBody(cmd *cobra.Command, args []string) (any, error) {
	f := cmd.Flags()
	str := func(name string) string { v, _ := f.GetString(name); return v }
	name := str("name")
	if len(args) != 0 {
		if name != "" && name != args[0] {
			return nil, usageError("--name must match the channel argument")
		}
		name = args[0]
	}
	if name == "" {
		return nil, usageError("--name is required")
	}
	typ := str("type")
	if typ != "webhook" && typ != "slack" && typ != "email" {
		return nil, usageError("--type must be webhook, slack or email")
	}
	enabled, _ := f.GetBool("enabled")
	categories, _ := f.GetStringArray("category")
	namespaces, _ := f.GetStringArray("namespace")
	body := map[string]any{"name": name, "type": typ, "enabled": enabled, "categories": categories, "namespaces": namespaces}
	secretFlags := []string{"signing-secret-stdin", "webhook-url-stdin", "password-stdin"}
	selected := ""
	for _, flag := range secretFlags {
		if on, _ := f.GetBool(flag); on {
			if selected != "" {
				return nil, usageError("only one secret can be read from stdin; use -f for multiple secrets")
			}
			selected = flag
		}
	}
	expected := map[string]string{"webhook": "signing-secret-stdin", "slack": "webhook-url-stdin", "email": "password-stdin"}[typ]
	if selected != "" && selected != expected {
		return nil, usageError("secret input flag does not match --type")
	}
	var config map[string]any
	switch typ {
	case "webhook":
		config = map[string]any{}
		if url := str("url"); url != "" {
			config["url"] = url
		}
	case "slack":
		config = map[string]any{}
	case "email":
		port, _ := f.GetInt("smtp-port")
		security := str("security")
		to, _ := f.GetStringArray("to")
		if port < 1 || port > 65535 {
			return nil, usageError("--smtp-port must be between 1 and 65535")
		}
		if security != "starttls" && security != "tls" {
			return nil, usageError("--security must be starttls or tls")
		}
		if str("smtp-host") == "" || str("from") == "" || len(to) < 1 || len(to) > 20 {
			return nil, usageError("email requires --smtp-host, --from and 1–20 --to recipients")
		}
		config = map[string]any{"host": str("smtp-host"), "port": port, "security": security, "username": str("username"), "from": str("from"), "to": to}
	}
	if selected != "" {
		data, err := io.ReadAll(a.opts.In)
		if err != nil {
			return nil, fmt.Errorf("read notification secret: %w", err)
		}
		secret := strings.TrimRight(string(data), "\r\n")
		if secret == "" {
			return nil, usageError("secret input must not be empty")
		}
		key := map[string]string{"webhook": "signingSecret", "slack": "webhookUrl", "email": "password"}[typ]
		config[key] = secret
	}
	body[typ] = config
	return body, nil
}

func (a *app) notificationTable(result any, op string) error {
	if op == "testNotificationChannel" {
		_, err := fmt.Fprintf(a.opts.Out, "Delivered: %s\nError: %s\n", cell(nested(result, "delivered")), cell(nested(result, "error")))
		return err
	}
	if op == "deleteNotificationChannel" {
		return a.print(result)
	}
	rows := []any{result}
	if op == "listNotificationChannels" {
		rows, _ = nested(result, "items").([]any)
	}
	w := tabwriter.NewWriter(a.opts.Out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tTYPE\tENABLED\tCATEGORIES\tHOST\tSTATUS")
	for _, row := range rows {
		host := nested(row, "webhook", "host")
		if nested(row, "type") == "email" {
			host = nested(row, "email", "host")
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\tpending=%s failed=%s error=%s\n", cell(nested(row, "name")), cell(nested(row, "type")), cell(nested(row, "enabled")), cell(nested(row, "categories")), cell(host), cell(nested(row, "status", "pending")), cell(nested(row, "status", "failed")), cell(nested(row, "status", "lastError")))
	}
	return w.Flush()
}
