// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"gopkg.in/yaml.v3"
)

const importReceiptKey = "KARAKORAM_IMPORT_REQUEST"

// Import recovery is available only with explicit readable ownership. Never
// adopt a foreign, failed or masked volume merely because its name matches.
func importReceipt(specs []infra.ServiceSpec) string {
	public := make([]infra.ServiceSpec, len(specs))
	for i, spec := range specs {
		if spec.Vars["KARAKORAM_OWNER"] == "" {
			return ""
		}
		public[i] = spec
		public[i].Env = nil // Do not publish a digest of secret values.
	}
	data, _ := json.Marshal(public)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func (b *Backend) recoverImport(ctx context.Context, specs []infra.ServiceSpec, receipt string) (infra.Op, bool, error) {
	if receipt == "" {
		return infra.Op{}, false, nil
	}
	services := make(map[string]string, len(specs))
	for _, spec := range specs {
		s, err := b.resolve(ctx, spec.Hostname)
		if err != nil {
			return infra.Op{}, false, err
		}
		if s == nil {
			return infra.Op{}, false, nil
		}
		if observed(*s).Kind != spec.Kind || s.Status == infra.StatusFailed {
			return infra.Op{}, false, &classifiedError{cause: errors.New("zerops: refusing import recovery for a failed or different-kind service; inspect original ownership and storage; automatic volume recreation is refused"), class: infra.ErrConflict}
		}
		items, err := b.client.Env(ctx, s.ID)
		if err != nil {
			return infra.Op{}, false, err
		}
		values := make(map[string]string, len(items))
		for _, item := range items {
			if !item.Sensitive && item.Content != maskedValue {
				values[item.Key] = item.Content
			}
		}
		if values[importReceiptKey] != receipt || values["KARAKORAM_OWNER"] != spec.Vars["KARAKORAM_OWNER"] {
			return infra.Op{}, false, &classifiedError{cause: errors.New("zerops: refusing import recovery for conflicting or unreadable ownership; inspect the original service identity without deleting its storage"), class: infra.ErrConflict}
		}
		services[spec.Hostname] = s.ID
	}
	encoded, _ := json.Marshal(services)
	return infra.Op{ID: "import-observe:" + string(encoded), Kind: "import", Services: services}, true, nil
}
func (b *Backend) importedStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	var services map[string]string
	if json.Unmarshal([]byte(strings.TrimPrefix(op.ID, "import-observe:")), &services) != nil || len(services) == 0 {
		return infra.OpState{}, &infra.RejectedError{Message: "zerops: invalid recovered import handle"}
	}
	state := infra.OpState{Done: true, Status: "FINISHED"}
	for name, id := range services {
		s, err := b.client.GetService(ctx, id)
		if err != nil {
			return infra.OpState{}, err
		}
		if s.ID != id || s.Name != name || s.protected() || (s.ProjectID != "" && s.ProjectID != b.client.project) {
			return infra.OpState{}, &infra.RejectedError{Message: "zerops: recovered import identity mismatch"}
		}
		state.ServiceIDs = append(state.ServiceIDs, id)
		switch s.Status {
		case infra.StatusActive, infra.StatusRunning, infra.StatusReadyToDeploy:
		case infra.StatusFailed:
			return infra.OpState{Done: true, Failed: true, Status: infra.StatusFailed, Message: "imported service failed; inspect platform process diagnostics without recreating its storage"}, nil
		default:
			state.Done = false
			state.Status = s.Status
		}
	}
	return state, nil
}

func receiptImportDocument(doc *importDocument, receipt string) (string, error) {
	for i := range doc.Services {
		vars := make(map[string]string, len(doc.Services[i].Vars)+1)
		for key, value := range doc.Services[i].Vars {
			vars[key] = value
		}
		vars[importReceiptKey] = receipt
		doc.Services[i].Vars = vars
	}
	data, err := yaml.Marshal(doc)
	return string(data), err
}
