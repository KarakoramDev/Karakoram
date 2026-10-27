// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

var backupActionMu sync.Mutex
var activeBackups sync.Map

type backupReceipt struct {
	RequestHash string                `json:"requestHash"`
	Response    myspec.BackupResponse `json:"response"`
}

func requestDigest(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func receiptPath(p paths, kind, id string) (string, error) {
	if id == "" || strings.ContainsAny(id, "\x00\r\n") {
		return "", errors.New("receipt_invalid")
	}
	if kind != "backup" && kind != "restore" {
		return "", errors.New("receipt_invalid")
	}
	h := sha256.Sum256([]byte(id))
	return filepath.Join(p.root, kind+"-"+hex.EncodeToString(h[:])+".json"), nil
}
func backupCommand(ctx context.Context, s myspec.Spec, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, filepath.Join(s.XtraBackupDir, "bin", name), args...)
	cmd.Env = append(nativeEnv(), "TZ=UTC")
	return cmd
}

// pipeline waits for both children, including when the consumer fails early.
// Native output is intentionally discarded: it can echo option-file passwords.
func pipeline(a, b *exec.Cmd) error {
	r, w, err := os.Pipe()
	if err != nil {
		return errors.New("pipeline_start")
	}
	a.Stdout = w
	b.Stdin = r
	if err = b.Start(); err != nil {
		r.Close()
		w.Close()
		return errors.New("pipeline_start")
	}
	if err = a.Start(); err != nil {
		r.Close()
		w.Close()
		_ = b.Process.Kill()
		_ = b.Wait()
		return errors.New("pipeline_start")
	}
	r.Close()
	w.Close()
	ea := a.Wait()
	eb := b.Wait()
	if ea != nil && eb != nil {
		return errors.New("pipeline_both_failed")
	}
	if ea != nil {
		return errors.New("pipeline_source_failed")
	}
	if eb != nil {
		return errors.New("pipeline_sink_failed")
	}
	return nil
}
func metadataValues(b []byte) map[string]string {
	m := map[string]string{}
	for _, line := range strings.Split(string(b), "\n") {
		k, v, ok := strings.Cut(line, "=")
		if ok {
			m[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return m
}
func parseBackupMetadata(dir string) (myspec.BackupResult, error) {
	var out myspec.BackupResult
	checkpoints, err := os.ReadFile(filepath.Join(dir, "xtrabackup_checkpoints"))
	if err != nil {
		return out, errors.New("metadata_missing")
	}
	m := metadataValues(checkpoints)
	out.BackupType = m["backup_type"]
	if out.BackupType != "full-backuped" && out.BackupType != "incremental" {
		return out, errors.New("metadata_invalid")
	}
	out.FromLSN, err = strconv.ParseUint(m["from_lsn"], 10, 64)
	if err != nil {
		return out, errors.New("metadata_invalid")
	}
	out.ToLSN, err = strconv.ParseUint(m["to_lsn"], 10, 64)
	if err != nil || out.ToLSN < out.FromLSN {
		return out, errors.New("metadata_invalid")
	}
	binlog, err := os.ReadFile(filepath.Join(dir, "xtrabackup_binlog_info"))
	if err != nil {
		return out, errors.New("metadata_missing")
	}
	fields := strings.Fields(string(binlog))
	if len(fields) < 3 {
		return out, errors.New("metadata_invalid")
	}
	out.BinlogFile = filepath.Base(fields[0])
	out.BinlogPosition, err = strconv.ParseUint(fields[1], 10, 64)
	if err != nil || !binlogName.MatchString(out.BinlogFile) || out.BinlogPosition < 4 {
		return out, errors.New("metadata_invalid")
	}
	out.GTIDSet = strings.Join(fields[2:], "")
	if _, err = parseRecoveryGTIDs(out.GTIDSet); err != nil {
		return out, errors.New("metadata_invalid")
	}
	info, err := os.ReadFile(filepath.Join(dir, "xtrabackup_info"))
	if err != nil {
		return out, errors.New("metadata_missing")
	}
	m = metadataValues(info)
	out.ServerVersion = m["server_version"]
	out.ToolVersion = m["tool_version"]
	if m["backup_size"] != "" {
		out.Size, err = strconv.ParseInt(m["backup_size"], 10, 64)
		if err != nil || out.Size < 0 {
			return out, errors.New("metadata_invalid")
		}
	}
	out.EndTime, err = time.ParseInLocation("2006-01-02 15:04:05", m["end_time"], time.UTC)
	if err != nil || out.ServerVersion == "" || out.ToolVersion == "" {
		return out, errors.New("metadata_invalid")
	}
	return out, nil
}

// XtraBackup copies only InnoDB and listed non-InnoDB extensions from the
// datadir, so auto.cnf never enters the stream. Its generated backup-my.cnf
// records the source's @@server_uuid inside the same authenticated stream.
func backupSourceUUID(dir string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "backup-my.cnf"))
	if err != nil {
		return "", err
	}
	uuid := metadataValues(raw)["server_uuid"]
	if !myspec.ValidUUID(uuid) {
		return "", errors.New("metadata_invalid")
	}
	return uuid, nil
}
func readBackupReceipt(name, digest string) (backupReceipt, error) {
	var v backupReceipt
	b, err := os.ReadFile(name)
	if err != nil {
		return v, err
	}
	if json.Unmarshal(b, &v) != nil {
		return v, errors.New("receipt_invalid")
	}
	if digest != "" && v.RequestHash != digest {
		return v, errors.New("receipt_conflict")
	}
	if v.Response.State == "running" {
		if _, ok := activeBackups.Load(name); !ok {
			v.Response.State = "failed"
			v.Response.Error = "backup_interrupted"
			if durableJSON(name, v) != nil {
				return v, errors.New("receipt_write")
			}
		}
	}
	return v, nil
}
func (e *Runner) backup(ctx context.Context, s myspec.Spec, p paths, r myspec.BackupRequest) (myspec.BackupResponse, error) {
	backupActionMu.Lock()
	defer backupActionMu.Unlock()
	if err := r.Validate(); err != nil {
		return myspec.BackupResponse{}, errors.New("request_invalid")
	}
	name, err := receiptPath(p, "backup", r.Receipt)
	if err != nil {
		return myspec.BackupResponse{}, err
	}
	digest := requestDigest(r)
	previous, err := readBackupReceipt(name, digest)
	if err == nil {
		return previous.Response, nil
	}
	if !os.IsNotExist(err) {
		return previous.Response, err
	}
	st, err := e.status(ctx)
	if err != nil || !st.Online(s.Galera != nil) {
		return myspec.BackupResponse{}, errors.New("member_unavailable")
	}
	if r.PhysicalIdentity != "" && r.PhysicalIdentity != st.PhysicalIdentity || r.Type == "incremental" && r.PhysicalIdentity == "" {
		return myspec.BackupResponse{}, errors.New("backup_physical_identity_mismatch")
	}
	r.PhysicalIdentity = st.PhysicalIdentity
	receipt := backupReceipt{RequestHash: digest, Response: myspec.BackupResponse{Receipt: r.Receipt, State: "running"}}
	if err = durableJSON(name, receipt); err != nil {
		return receipt.Response, errors.New("receipt_write")
	}
	finish, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	activeBackups.Store(name, cancel)
	go func() {
		defer cancel()
		result, runErr := runBackup(finish, s, p, r)
		if runErr == nil {
			after, statusErr := e.status(finish)
			if statusErr != nil || after.PhysicalIdentity != r.PhysicalIdentity {
				runErr = errors.New("backup_physical_identity_changed")
			}
		}
		backupActionMu.Lock()
		defer backupActionMu.Unlock()
		defer activeBackups.Delete(name)
		receipt.Response.State = "done"
		receipt.Response.Result = &result
		if runErr != nil {
			receipt.Response.State = "failed"
			receipt.Response.Result = nil
			receipt.Response.Error = runErr.Error()
		}
		_ = durableJSON(name, receipt)
	}()
	return receipt.Response, nil
}
func stopBackups(p paths) {
	activeBackups.Range(func(key, value any) bool {
		name, ok := key.(string)
		if ok && filepath.Dir(name) == p.root {
			if cancel, ok := value.(context.CancelFunc); ok {
				cancel()
			}
		}
		return true
	})
}
func runBackup(ctx context.Context, s myspec.Spec, p paths, r myspec.BackupRequest) (myspec.BackupResult, error) {
	var out myspec.BackupResult
	dir, err := os.MkdirTemp(p.root, "backup-stage-")
	if err != nil {
		return out, errors.New("staging_failed")
	}
	defer os.RemoveAll(dir)
	defaults := filepath.Join(dir, "backup.cnf")
	text := "[client]\nuser=" + optionQuote(s.BackupUser.Username) + "\npassword=" + optionQuote(s.BackupUser.Password) + "\nsocket=" + optionQuote(p.socket) + "\n"
	if err = os.WriteFile(defaults, []byte(text), 0600); err != nil {
		return out, errors.New("credentials_failed")
	}
	cloud, err := cloudConfig(r.Repository, dir)
	if err != nil {
		return out, errors.New("repository_invalid")
	}
	defer removeCloudConfig(cloud)
	args := []string{"--defaults-extra-file=" + defaults, "--backup", "--stream=xbstream", "--lock-ddl", "--extra-lsndir=" + dir, "--target-dir=" + filepath.Join(dir, "target"), "--datadir=" + p.data}
	if r.Type == "incremental" {
		args = append(args, "--incremental-lsn="+strconv.FormatUint(r.BaseLSN, 10))
	}
	// MySQL load_defaults consumes this option before xbcloud parses its verb;
	// placing it after "put" silently leaves the protected configuration unused.
	metadataFrames, err := os.CreateTemp(dir, "source-metadata-")
	if err != nil {
		return out, errors.New("staging_failed")
	}
	manifest, streamErr := backupManifestPipeline(backupCommand(ctx, s, "xtrabackup", args...), backupCommand(ctx, s, "xbcloud", "--defaults-file="+cloud, "put", "--storage=s3", r.ObjectPrefix), metadataFrames, r.ObjectPrefix)
	closeErr := metadataFrames.Close()
	if streamErr != nil {
		return out, streamErr
	}
	if closeErr != nil {
		return out, errors.New("metadata_write_failed")
	}
	// Scalar metadata and the UUID come from the authenticated source pipe,
	// never from mutable S3 objects fetched before the manifest is sealed.
	metadataDir := filepath.Join(dir, "metadata")
	if err = privateDir(metadataDir); err != nil {
		return out, errors.New("staging_failed")
	}
	input, err := os.Open(metadataFrames.Name())
	if err != nil {
		return out, errors.New("metadata_read_failed")
	}
	extract := backupCommand(ctx, s, "xbstream", "-x", "-C", metadataDir)
	extract.Stdin = input
	err = extract.Run()
	input.Close()
	if err != nil {
		return out, errors.New("metadata_extract_failed")
	}
	out, err = parseBackupMetadata(metadataDir)
	if err != nil {
		return out, err
	}
	out.SourceUUID, err = backupSourceUUID(metadataDir)
	if err != nil {
		return out, errors.New("metadata_invalid")
	}
	if out.FromLSN != r.BaseLSN {
		return out, errors.New("backup_lsn_mismatch")
	}
	client, err := s3Client(r.Repository)
	if err != nil {
		return out, errors.New("repository_invalid")
	}
	out.ManifestDigest, out.ManifestSize, err = createBackupManifest(ctx, client, r.Repository, manifest)
	if err != nil {
		return out, errors.New("physical_manifest_failed")
	}
	out.ObjectPrefix = r.ObjectPrefix
	out.PhysicalIdentity = r.PhysicalIdentity
	return out, nil
}
func (e *Runner) backupRoutes(mux *http.ServeMux, n *agent.Node) {
	for _, action := range []string{myspec.ActionBackup, myspec.ActionBackupInfo, myspec.ActionArchiveStatus, myspec.ActionPITRCoverage} {
		mux.HandleFunc("POST /v1/mysql/"+action, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
			defer cancel()
			var result any
			var err error
			s, p := e.state()
			switch action {
			case myspec.ActionBackup:
				var req myspec.BackupRequest
				err = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req)
				if err == nil {
					err = n.WithLock(ctx, func(_ *agent.Node) error { var inner error; result, inner = e.backup(ctx, s, p, req); return inner })
				}
			case myspec.ActionBackupInfo:
				var req struct {
					Receipt string `json:"receipt"`
				}
				err = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req)
				if err == nil {
					var name string
					name, err = receiptPath(p, "backup", req.Receipt)
					if err == nil {
						backupActionMu.Lock()
						var receipt backupReceipt
						receipt, err = readBackupReceipt(name, "")
						backupActionMu.Unlock()
						result = receipt.Response
					}
				}
			case myspec.ActionArchiveStatus:
				result, err = e.archiveStatus()
			case myspec.ActionPITRCoverage:
				var req myspec.PITRCoverageRequest
				err = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req)
				if err == nil {
					result, err = e.pitrCoverage(ctx, req)
				}
			}
			if err != nil {
				message := fmt.Sprintf("MySQL %s unavailable", action)
				// Only stable, credential-free codes may cross the HTTP boundary.
				switch err.Error() {
				case "request_invalid", "receipt_invalid", "receipt_conflict", "receipt_write", "member_unavailable", "backup_physical_identity_mismatch":
					message += ": " + err.Error()
				}
				http.Error(w, message, http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(result)
		})
	}
}
