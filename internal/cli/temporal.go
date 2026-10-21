// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/KarakoramDev/Karakoram/pkg/client"
	"github.com/spf13/cobra"
)

func (a *app) addTemporal(root *cobra.Command) {
	group := groupCommand("temporal", commandGroupDescriptions["temporal"])
	root.AddCommand(group)
	parents := map[string]*cobra.Command{}
	for _, name := range []string{"client-certificate", "namespace", "ui-oidc-secret"} {
		parents[name] = groupCommand(name, commandGroupDescriptions["temporal "+name])
		group.AddCommand(parents[name])
	}
	for _, spec := range temporalCommandSpecs {
		s := spec
		words := strings.Fields(s.path)
		cmd := &cobra.Command{Use: strings.Join(words[2:], " "), Short: commandDescription(s.path), Args: cobra.ExactArgs(s.args)}
		annotate(cmd, s.operation)
		cmd.ValidArgsFunction = cobra.FixedCompletions(nil, cobra.ShellCompDirectiveNoFileComp)
		switch s.operation {
		case "issueTemporalClientCertificate":
			cmd.Flags().String("common-name", "", "Client certificate common name (also the output filename)")
			cmd.Flags().Duration("ttl", 720*time.Hour, "Certificate lifetime, rounded to the nearest whole hour")
			cmd.Flags().String("out-dir", "", "Write certificate, key and CA files without overwriting")
			cmd.Flags().Bool("stdout", false, "Print certificate, private key and CA PEM bundle to stdout")
			cmd.MarkFlagsMutuallyExclusive("out-dir", "stdout")
			cmd.MarkFlagsOneRequired("out-dir", "stdout")
			_ = cmd.MarkFlagRequired("common-name")
		case "deleteTemporalNamespace":
			cmd.Flags().BoolP("yes", "y", false, "Confirm without reading stdin")
		case "setTemporalUIOIDCClientSecret":
			cmd.Flags().String("from-file", "", "Read the client secret from a file, never argv")
			cmd.Flags().Bool("from-stdin", false, "Read the client secret from stdin, never argv")
			cmd.MarkFlagsMutuallyExclusive("from-file", "from-stdin")
			cmd.MarkFlagsOneRequired("from-file", "from-stdin")
		}
		cmd.RunE = func(cmd *cobra.Command, args []string) error { return a.runTemporal(cmd, s.operation, args) }
		parents[words[1]].AddCommand(cmd)
	}
}

func (a *app) runTemporal(cmd *cobra.Command, op string, args []string) error {
	if err := a.confirm(cmd, op, args); err != nil {
		return err
	}
	var body any
	var paths []string
	if op == "issueTemporalClientCertificate" {
		cn, _ := cmd.Flags().GetString("common-name")
		if cn == "" || cn == "." || cn == ".." || strings.ContainsAny(cn, "/\\\x00") {
			return usageError("--common-name must be a nonempty filename without path separators")
		}
		ttl, _ := cmd.Flags().GetDuration("ttl")
		hours := int(ttl.Round(time.Hour) / time.Hour)
		if ttl <= 0 || hours <= 0 {
			return usageError("--ttl must round to at least one hour")
		}
		body = client.TemporalClientCertificateRequest{CommonName: cn, TtlHours: &hours}
		dir, _ := cmd.Flags().GetString("out-dir")
		stdout, _ := cmd.Flags().GetBool("stdout")
		if (dir == "") == !stdout {
			return usageError("exactly one of --out-dir or --stdout is required")
		}
		if dir != "" {
			paths = []string{filepath.Join(dir, cn+".crt"), filepath.Join(dir, cn+".key"), filepath.Join(dir, "ca.crt")}
			if paths[0] == paths[2] {
				return usageError("--common-name ca conflicts with ca.crt")
			}
			if err := temporalCertificatePathsAvailable(paths); err != nil {
				return err
			}
		}
	}
	if op == "setTemporalUIOIDCClientSecret" {
		file, _ := cmd.Flags().GetString("from-file")
		stdin, _ := cmd.Flags().GetBool("from-stdin")
		if (file == "") == !stdin {
			return usageError("exactly one of --from-file or --from-stdin is required")
		}
		var data []byte
		var err error
		if stdin {
			data, err = io.ReadAll(a.opts.In)
		} else {
			data, err = os.ReadFile(file)
		}
		if err != nil {
			return err
		}
		secret := strings.TrimSuffix(string(data), "\r\n")
		if secret == string(data) {
			secret = strings.TrimSuffix(secret, "\n")
		}
		if secret == "" {
			return usageError("client secret must not be empty")
		}
		body = client.TemporalUIOIDCClientSecret{ClientSecret: &secret}
	}
	result, err := a.request(cmd.Context(), op, args, nil, body)
	if err != nil {
		return err
	}
	if op == "issueTemporalClientCertificate" {
		fields, ok := result.(map[string]any)
		if !ok {
			return fmt.Errorf("unexpected Temporal certificate response")
		}
		values := make([]string, 3)
		for i, key := range []string{"certificatePem", "privateKeyPem", "caPem"} {
			values[i], ok = fields[key].(string)
			if !ok || values[i] == "" {
				return fmt.Errorf("temporal certificate response lacks %s", key)
			}
		}
		if len(paths) != 0 {
			err = writeTemporalCertificateFiles(paths, values)
		} else {
			for _, pem := range values {
				if err = printPEM(a.opts.Out, pem); err != nil {
					break
				}
			}
		}
		if err != nil {
			return err
		}
		for _, key := range []string{"serial", "notAfter", "frontendAddress", "serverName"} {
			fmt.Fprintf(a.opts.Err, "%s: %s\n", key, cell(fields[key]))
		}
		return nil
	}
	if op == "listTemporalClientCertificates" && a.format == "table" {
		fields, ok := result.(map[string]any)
		if !ok {
			return fmt.Errorf("unexpected Temporal certificate list response")
		}
		items, _ := fields["items"].([]any)
		if len(items) == 0 {
			return a.print(result)
		}
		w := tabwriter.NewWriter(a.opts.Out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "SERIAL\tCOMMONNAME\tNOTAFTER\tCAFINGERPRINT\tCURRENT")
		for _, item := range items {
			row, _ := item.(map[string]any)
			fingerprint := cell(row["caFingerprintSha256"])
			if len(fingerprint) > 12 {
				fingerprint = fingerprint[:12]
			}
			current := "no"
			if row["currentCa"] == true {
				current = "yes"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", cell(row["serial"]), cell(row["commonName"]), cell(row["notAfter"]), fingerprint, current)
		}
		return w.Flush()
	}
	return a.print(result)
}

func temporalCertificatePathsAvailable(paths []string) error {
	for _, path := range paths {
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("refusing to overwrite %s", path)
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func writeTemporalCertificateFiles(paths, values []string) (err error) {
	if err = temporalCertificatePathsAvailable(paths); err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(paths[0]), 0700); err != nil {
		return err
	}
	// Reserve every path before writing any material. O_EXCL also closes the
	// preflight race; on failure remove only files created by this invocation.
	files := make([]*os.File, 0, len(paths))
	defer func() {
		for i, file := range files {
			if file != nil {
				_ = file.Close()
			}
			if err != nil {
				_ = os.Remove(paths[i])
			}
		}
	}()
	for i, path := range paths {
		mode := os.FileMode(0600)
		if i == 2 {
			mode = 0644
		}
		file, openErr := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
		if openErr != nil {
			return openErr
		}
		files = append(files, file)
	}
	for i, file := range files {
		if _, err = io.WriteString(file, values[i]); err != nil {
			return err
		}
		err = file.Close()
		files[i] = nil
		if err != nil {
			return err
		}
	}
	return nil
}

func dispatchTemporal(ctx context.Context, c *client.Client, op string, args []string, query, body []byte) (*http.Response, error) {
	switch op {
	case "issueTemporalClientCertificate":
		var p client.IssueTemporalClientCertificateParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.IssueTemporalClientCertificateWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "listTemporalClientCertificates":
		var p client.ListTemporalClientCertificatesParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.ListTemporalClientCertificates(ctx, args[0], &p)
	case "listTemporalNamespaces":
		var p client.ListTemporalNamespacesParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.ListTemporalNamespaces(ctx, args[0], &p)
	case "deleteTemporalNamespace":
		var p client.DeleteTemporalNamespaceParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.DeleteTemporalNamespace(ctx, args[0], args[1], &p)
	case "setTemporalUIOIDCClientSecret":
		var p client.SetTemporalUIOIDCClientSecretParams
		if err := json.Unmarshal(query, &p); err != nil {
			return nil, err
		}
		return c.SetTemporalUIOIDCClientSecretWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	}
	return nil, fmt.Errorf("unknown Temporal operation %q", op)
}
