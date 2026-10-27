// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// InternalStateKey journals component identities, binding ownership and ordered
// upgrades in Cluster.Internal. It never holds private keys or passwords.
const InternalStateKey = "temporal"

// UIOIDCClientSecretKey stores the write-only UI OIDC secret in SecretStore.
// Updating this key triggers configuration publication on the next Sync.
const UIOIDCClientSecretKey = "temporal/ui-oidc-client-secret"

// ConfigSecretPrefix stores full agent envelopes, including credentials.
// The cluster CA uses tls/ca.crt and tls/ca.key for the shared agent dialer.
// Leaf pairs are sealed at temporal/{agent,internode,frontend}-cert/<member>.
const ConfigSecretPrefix = "temporal/config/"

// InternodeServerName is the shared SAN for role-to-role mutual TLS.
const InternodeServerName = "internode.temporal.karakoram.internal"

// FrontendServerName is the stable serving identity verified by UI and SDK clients.
const FrontendServerName = "frontend.temporal.karakoram.internal"

type componentState struct {
	Members              map[string]*componentMember `json:"members"`
	Pending              *componentPending           `json:"pending,omitempty"`
	BindingID            string                      `json:"bindingId,omitempty"`
	CredentialGeneration int64                       `json:"credentialGeneration"`
	Binding              bindingStatus               `json:"binding"`
	Namespaces           []tspec.NamespaceStatus     `json:"namespaces,omitempty"`
	NamespaceHash        string                      `json:"namespaceHash,omitempty"`
	Version              string                      `json:"version,omitempty"`
	Upgrade              *upgradeState               `json:"upgrade,omitempty"`
	WaitingSource        bool                        `json:"waitingSource,omitempty"`
	Backups              map[string]backupManifest   `json:"backups,omitempty"`
	RestoreRecovery      *restoreRecoveryState       `json:"restoreRecovery,omitempty"`
}

type bindingStatus struct {
	SourceNamespace string `json:"sourceNamespace"`
	SourceCluster   string `json:"sourceCluster"`
	Shared          bool   `json:"shared"`
	ConsumerCount   int    `json:"consumerCount"`
}

type upgradeState struct {
	From        string    `json:"from"`
	To          string    `json:"to"`
	Component   string    `json:"component,omitempty"`
	SettleUntil time.Time `json:"settleUntil,omitempty"`
}

type componentMember struct {
	Name            string                `json:"name"`
	Component       string                `json:"component"`
	ServiceID       string                `json:"serviceId,omitempty"`
	ImportBatch     *engine.ImportPlan    `json:"importBatch,omitempty"`
	Stage           engine.ComponentStage `json:"stage"`
	Version         string                `json:"version,omitempty"`
	ConfigHash      string                `json:"configHash,omitempty"`
	ConfigVersion   int64                 `json:"configVersion"`
	Replicas        int                   `json:"replicas"`
	Resources       domain.Resources      `json:"resources"`
	Public          bool                  `json:"public"`
	RuntimeDeployed bool                  `json:"runtimeDeployed,omitempty"`
	Recovering      bool                  `json:"recovering,omitempty"`
	NativeReadiness *bool                 `json:"nativeReadiness,omitempty"`
	Observed        tspec.Status          `json:"observed"`
	RestartNonce    string                `json:"restartNonce,omitempty"`
}

type componentPending struct {
	Component string           `json:"component"`
	Action    string           `json:"action"`
	Op        infra.Op         `json:"op"`
	Replicas  int              `json:"replicas,omitempty"`
	Resources domain.Resources `json:"resources"`
	Public    bool             `json:"public,omitempty"`
}

func loadState(c *domain.Cluster) (*componentState, error) {
	s := &componentState{Members: map[string]*componentMember{}}
	if len(c.Internal) == 0 {
		return s, nil
	}
	var root map[string]json.RawMessage
	if e := json.Unmarshal(c.Internal, &root); e != nil {
		return nil, e
	}
	if raw := root[InternalStateKey]; len(raw) > 0 {
		if e := json.Unmarshal(raw, s); e != nil {
			return nil, e
		}
	}
	if s.Members == nil {
		s.Members = map[string]*componentMember{}
	}
	return s, nil
}

func saveState(c *domain.Cluster, s *componentState) error {
	root := map[string]json.RawMessage{}
	if len(c.Internal) > 0 {
		if e := json.Unmarshal(c.Internal, &root); e != nil {
			return e
		}
	}
	raw, e := json.Marshal(s)
	if e != nil {
		return e
	}
	root[InternalStateKey] = raw
	c.Internal, e = json.Marshal(root)
	return e
}

func missing(e error) bool {
	return errors.Is(e, engine.ErrNotFound) || errors.Is(e, store.ErrNotFound)
}

func clockNow(rc *engine.Context) time.Time {
	if rc.Clock != nil {
		return rc.Clock.Now()
	}
	return time.Now().UTC()
}

func frontendComponent(c *domain.Cluster) string {
	if c.Spec.Topology.Type == "split" {
		return tspec.Frontend
	}
	return tspec.Server
}

var componentCodes = map[string]string{
	tspec.History: "th", tspec.Matching: "tm", tspec.Frontend: "tf",
	tspec.Worker: "tw", tspec.Server: "ts", tspec.UI: "tu",
}

func memberName(c *domain.Cluster, component string) string {
	return c.Name + componentCodes[component]
}
