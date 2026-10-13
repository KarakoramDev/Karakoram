// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func backupURI(c agentapi.MemberConfig, dataDir string) (string, error) {
	cred, ok := c.SystemUsers["backup"]
	if !ok || cred.Password == "" {
		return "", errors.New("backup credentials unavailable")
	}
	u := url.URL{Scheme: "mongodb", Host: fmt.Sprintf("127.0.0.1:%d", memberPort(c)), Path: "/", User: url.UserPassword("backup", cred.Password), RawQuery: "authSource=admin"}
	if c.TLS.Mode != "" && c.TLS.Mode != "disabled" {
		query := u.Query()
		query.Set("tls", "true")
		query.Set("tlsCAFile", filepath.Join(dataDir, "karakoram", "ca.pem"))
		query.Set("tlsCertificateKeyFile", filepath.Join(dataDir, "karakoram", "mongodb.pem"))
		u.RawQuery = query.Encode()
	}
	return u.String(), nil
}
func memberPort(c agentapi.MemberConfig) int {
	if c.Port == 0 {
		return 27017
	}
	return c.Port
}

func pbmArgs(r agentapi.PBMRequest, configPath string) ([]string, error) {
	allowed := map[string]map[string]bool{
		"status": {"--sections": true}, "list": {"--restore": false, "--size": false, "--full": false}, "describe-backup": {}, "describe-restore": {},
		"backup":        {"--type": true, "--compression": true, "--compression-level": true, "--wait": false},
		"restore":       {"--time": true, "--base-snapshot": true, "--yes": false, "--wait": false, "--replset-remapping": true, "--ns": true},
		"delete-backup": {"--older-than": true, "--all": false, "--yes": false}, "delete-pitr": {"--older-than": true, "--all": false, "--yes": false},
		"config": {"--force-resync": false, "--set": true, "--wait": false}, "cancel-backup": {},
	}
	flags, ok := allowed[r.Command]
	if !ok {
		return nil, errors.New("PBM command not allowed")
	}
	args := []string{r.Command}
	positionals := 0
	for i := 0; i < len(r.Args); i++ {
		arg := r.Args[i]
		if strings.ContainsRune(arg, 0) {
			return nil, errors.New("invalid PBM argument")
		}
		if !strings.HasPrefix(arg, "-") {
			positionals++
			if positionals > 1 || !(r.Command == "describe-backup" || r.Command == "describe-restore" || r.Command == "restore" || r.Command == "delete-backup") {
				return nil, errors.New("unexpected PBM positional argument")
			}
			args = append(args, arg)
			continue
		}
		flag, value, hasValue := strings.Cut(arg, "=")
		if flag == "--out" {
			if !hasValue {
				i++
				if i >= len(r.Args) {
					return nil, errors.New("missing output format")
				}
				value = r.Args[i]
			}
			if value != "json" {
				return nil, errors.New("PBM output must be json")
			}
			continue
		}
		if flag == "--file" || flag == "--config" {
			if configPath == "" || hasValue {
				return nil, errors.New("PBM config path must be supplied via Config")
			}
			if flag == "--file" && r.Command != "config" || flag == "--config" && r.Command != "describe-restore" {
				return nil, errors.New("config flag not allowed for command")
			}
			args = append(args, flag, configPath)
			configPath = ""
			continue
		}
		needsValue, ok := flags[flag]
		if !ok {
			return nil, fmt.Errorf("PBM flag %s not allowed", flag)
		}
		if needsValue && !hasValue {
			i++
			if i >= len(r.Args) || strings.HasPrefix(r.Args[i], "-") {
				return nil, fmt.Errorf("missing value for %s", flag)
			}
			value = r.Args[i]
			hasValue = true
		}
		if !needsValue && hasValue {
			return nil, fmt.Errorf("unexpected value for %s", flag)
		}
		args = append(args, flag)
		if hasValue {
			args = append(args, value)
		}
	}
	if configPath != "" {
		flag := "--config"
		if r.Command == "config" {
			flag = "--file"
		}
		if r.Command != "config" && r.Command != "describe-restore" {
			return nil, errors.New("config is not supported for this command")
		}
		args = append(args, flag, configPath)
	}
	return append(args, "--out=json"), nil
}

func lastJSONObject(stdout []byte) json.RawMessage {
	var last json.RawMessage
	for i, b := range stdout {
		if b != '{' {
			continue
		}
		d := json.NewDecoder(bytes.NewReader(stdout[i:]))
		var raw json.RawMessage
		if d.Decode(&raw) == nil && len(raw) > 0 && raw[0] == '{' {
			end := i + int(d.InputOffset())
			if json.Valid(raw) {
				last = append(json.RawMessage(nil), raw...) // Skip nested candidates inside this object.
				for j := i + 1; j < end; j++ {
					stdout[j] = ' '
				}
			}
		}
	}
	return last
}

type pbmReceipt struct {
	Hash     string                `json:"hash"`
	Response *agentapi.PBMResponse `json:"response,omitempty"`
}

func (a *nodeAgent) pbm(ctx context.Context, r agentapi.PBMRequest) (agentapi.PBMResponse, error) {
	a.pbmMu.Lock()
	defer a.pbmMu.Unlock()
	var receiptPath, requestHash string
	if r.RequestID != "" {
		id := sha256.Sum256([]byte(r.RequestID))
		receiptPath = filepath.Join(a.o.DataDir, "karakoram", "pbm-receipts", hex.EncodeToString(id[:])+".json")
		raw, _ := json.Marshal(r)
		hash := sha256.Sum256(raw)
		requestHash = hex.EncodeToString(hash[:])
		data, err := os.ReadFile(receiptPath)
		if err == nil {
			var receipt pbmReceipt
			if err = json.Unmarshal(data, &receipt); err != nil {
				return agentapi.PBMResponse{}, err
			}
			if receipt.Hash != requestHash {
				return agentapi.PBMResponse{}, errors.New("PBM request ID reused with different command")
			}
			if receipt.Response == nil {
				return agentapi.PBMResponse{}, errors.New("PBM initiation outcome unknown after interrupted execution; reconcile before issuing another request")
			}
			return *receipt.Response, nil
		}
		if !os.IsNotExist(err) {
			return agentapi.PBMResponse{}, err
		}
	}
	c := a.config()
	uri, err := backupURI(c, a.o.DataDir)
	if err != nil {
		return agentapi.PBMResponse{}, err
	}
	configPath := ""
	if len(r.Config) > 0 {
		var yaml string
		if err = json.Unmarshal(r.Config, &yaml); err != nil {
			return agentapi.PBMResponse{}, errors.New("PBM Config must be a JSON-encoded YAML string")
		}
		f, err := os.CreateTemp(filepath.Join(a.o.DataDir, "karakoram"), "pbm-*.yaml")
		if err != nil {
			return agentapi.PBMResponse{}, err
		}
		configPath = f.Name()
		defer os.Remove(configPath)
		if _, err = f.WriteString(yaml); err != nil {
			f.Close()
			return agentapi.PBMResponse{}, err
		}
		if err = f.Close(); err != nil {
			return agentapi.PBMResponse{}, err
		}
	}
	args, err := pbmArgs(r, configPath)
	if err != nil {
		return agentapi.PBMResponse{}, err
	}
	if receiptPath != "" {
		data, _ := json.Marshal(pbmReceipt{Hash: requestHash})
		if err = atomicWrite(receiptPath, data, 0600); err != nil {
			return agentapi.PBMResponse{}, err
		}
	}
	cmd := exec.CommandContext(ctx, "pbm", args...)
	cmd.Env = append(os.Environ(), "PBM_MONGODB_URI="+uri)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	response := agentapi.PBMResponse{Output: lastJSONObject(stdout.Bytes()), Stderr: stderr.String()}
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			response.ExitCode = exit.ExitCode()
		} else {
			return response, err
		}
	}
	if receiptPath != "" {
		data, _ := json.Marshal(pbmReceipt{Hash: requestHash, Response: &response})
		if err = atomicWrite(receiptPath, data, 0600); err != nil {
			return response, err
		}
	}
	return response, nil
}
