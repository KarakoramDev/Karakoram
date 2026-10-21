// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useEffect, useRef, useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap } from '@/api/client';
import { keys } from '@/api/keys';
import { idempotencyHeaders } from '@/api/idempotency';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { ChoiceGroup, Switch, TagListField } from '@/components/ui/Field';
import { Badge, Callout, Card, CodeLine, ErrorBlock, KV } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { OperationNotice } from '@/components/OperationNotice';
import { useClusterContext } from './ClusterDetail';
import { useSubmissionKey } from './use-submission-key';
import { buildDataMcpPatch, dataMcpConnectCommands, dataMcpDraft, dataMcpEndpoint, dataMcpView, scopeFields, validateDataMcpDraft, type DataMcpDraft } from './datamcp';

export function DataMcpSection() {
  const { cluster, namespace, name, can } = useClusterContext();
  const toast = useToast();
  const queryClient = useQueryClient();
  const provider = cluster.spec.provider;
  const persisted = cluster.spec.dataMcp;
  const view = dataMcpView(persisted, cluster.status.dataMcp);
  const fields = scopeFields(provider);
  const [draft, setDraft] = useState<DataMcpDraft>(() => dataMcpDraft(persisted, provider));
  const [confirmDisable, setConfirmDisable] = useState(false);
  const persistedKey = JSON.stringify(persisted ?? null);
  const [seed, setSeed] = useState(persistedKey);
  const submission = useSubmissionKey(JSON.stringify(draft));
  const saving = useRef(false);
  useEffect(() => {
    if (persistedKey !== seed) {
      setDraft(dataMcpDraft(persisted, provider));
      setSeed(persistedKey);
    }
  }, [persistedKey, seed, persisted, provider]);

  const patch = buildDataMcpPatch(persisted, draft, provider);
  const dirty = Object.keys(patch).length > 0;
  const errors = validateDataMcpDraft(draft, provider);
  const editable = can('update');
  const disabling = view.enabled && !draft.enabled;
  const origin = typeof window === 'undefined' ? '' : window.location.origin;

  const save = useMutation({
    mutationFn: () =>
      unwrap(
        api.PATCH('/clusters/{cluster}', {
          headers: { ...idempotencyHeaders(submission.key), 'Content-Type': 'application/merge-patch+json' },
          params: { path: { cluster: name }, query: { namespace } },
          body: { spec: { dataMcp: patch } },
        }),
      ),
    onSuccess: (c) => {
      submission.reset();
      setConfirmDisable(false);
      queryClient.setQueryData(keys.cluster(namespace, name), c);
      toast.success(<OperationNotice operationId={c.operationId}>Data MCP settings updated.</OperationNotice>);
    },
    onError: (e) => toast.error(errorMessage(e)),
  });
  const submit = () => {
    if (saving.current) return;
    saving.current = true;
    void save.mutateAsync().finally(() => { saving.current = false; }).catch(() => {});
  };
  const discard = () => {
    save.reset();
    submission.reset();
    setDraft(dataMcpDraft(persisted, provider));
  };

  return (
    <Card
      title={<span className="row" style={{ gap: 10 }}>Data MCP <Badge tone={view.tone} title={view.message}>{view.phase}</Badge></span>}
      actions={
        editable ? (
          <>
            {dirty ? (
              <Button size="sm" disabled={save.isPending} onClick={discard}>
                Discard
              </Button>
            ) : null}
            <Button size="sm" variant="primary" disabled={!dirty || errors.length > 0} loading={save.isPending} onClick={() => (disabling ? setConfirmDisable(true) : submit())}>
              Save settings
            </Button>
          </>
        ) : null
      }
    >
      <div className="stack" style={{ gap: 16 }}>
        <div className="grid-2">
          <div className="stack">
            <KV
              items={[
                ['Backend', view.kind ? <span>{view.kind === 'companion' ? 'Companion service' : 'Native toolset'}{view.upstream ? <> · <span className="mono small">{view.upstream}</span></> : null}</span> : null],
                ['Access', view.access ? (view.access === 'readWrite' ? <Badge tone="warn" plain>read and write</Badge> : <Badge plain>read-only</Badge>) : null],
                view.message ? ['Message', <span className="small">{view.message}</span>] : null,
              ]}
            />
            {!view.enabled ? <p className="small muted">Data access through MCP is off. Enable it below to let AI agents query this cluster with a restricted token.</p> : null}
          </div>
          <div className="stack">
            <strong className="small">Endpoint</strong>
            <CodeLine value={dataMcpEndpoint(origin, namespace, name)} />
            <strong className="small">Connect a client</strong>
            {dataMcpConnectCommands(namespace, name, view.access).map((command) => <CodeLine key={command} value={command} />)}
            <p className="small muted">
              Tokens are restricted and cluster-scoped; they need the <span className="mono">cluster-data</span> permission, and data writes additionally require the installation switch <span className="mono">KARAKORAM_MCP=readwrite</span>.
            </p>
          </div>
        </div>

        <div className="section-title">Settings</div>
        {!editable ? <Callout tone="info">Data MCP settings are read-only. You need the cluster update permission to change them.</Callout> : null}
        {save.error ? <ErrorBlock error={save.error} /> : null}
        <fieldset disabled={!editable || save.isPending} style={{ border: 0, padding: 0, margin: 0 }} className="stack">
          <Switch label={draft.enabled ? 'Data access enabled' : 'Data access disabled'} checked={draft.enabled} onChange={(e) => setDraft((d) => ({ ...d, enabled: e.target.checked }))} />
          <ChoiceGroup<DataMcpDraft['access']>
            name="datamcp-access"
            legend="Access"
            value={draft.access}
            onChange={(access) => setDraft((d) => ({ ...d, access }))}
            options={[
              { value: 'read', title: 'Read-only', description: 'A read-only database identity; write tools are never listed.' },
              { value: 'readWrite', title: 'Read and write', description: 'Adds a write identity; write tools appear only for tokens and installations that also allow writes.' },
            ]}
          />
          {draft.access === 'readWrite' ? (
            <Callout tone="warn" title="Agents can modify data">
              Write and destructive tools let an AI agent insert, update and delete production data within the scope below. Destructive tools always require confirmation; enable confirmation for write tools too unless the agent runs unattended by design.
            </Callout>
          ) : null}
          <div className={fields.length > 1 ? 'grid-2' : undefined}>
            {fields.map((field) => (
              <TagListField
                key={field.key}
                id={`datamcp-${field.key}`}
                label={field.label}
                placeholder={field.placeholder}
                hint={field.hint}
                value={draft.scope[field.key] ?? []}
                onChange={(value) => setDraft((d) => ({ ...d, scope: { ...d.scope, [field.key]: value } }))}
              />
            ))}
          </div>
          <TagListField
            id="datamcp-deny"
            label="Denied tools"
            placeholder="tool-name"
            hint="Tool names hidden from the agent and refused when called, in addition to the reviewed manifest."
            value={draft.deny}
            onChange={(deny) => setDraft((d) => ({ ...d, deny }))}
          />
          <Switch label="Require confirmation for write tools (destructive tools always need it)" checked={draft.confirmWrites} onChange={(e) => setDraft((d) => ({ ...d, confirmWrites: e.target.checked }))} />
          <Switch label="Record query text in audit events" checked={draft.auditQueryText} onChange={(e) => setDraft((d) => ({ ...d, auditQueryText: e.target.checked }))} />
          {draft.auditQueryText ? (
            <Callout tone="warn" title="Audit records will contain query text">
              Tool arguments, including filters and values an agent sends, are stored in audit events instead of only their hash. Anyone who can read audit events sees them.
            </Callout>
          ) : null}
          {errors.length && dirty ? <span className="field-error">{errors[0]}</span> : null}
        </fieldset>
      </div>

      <ConfirmDialog
        open={confirmDisable}
        onClose={() => { setConfirmDisable(false); save.reset(); }}
        onConfirm={() => save.mutateAsync()}
        title="Disable data MCP access"
        confirmLabel="Disable"
        danger
        busy={save.isPending}
        error={save.error}
      >
        <p className="small">
          The endpoint stops serving, existing tokens for this cluster are refused and the Karakoram-managed database identities are removed. Scope and tool settings are retained for re-enabling.
        </p>
      </ConfirmDialog>
    </Card>
  );
}
