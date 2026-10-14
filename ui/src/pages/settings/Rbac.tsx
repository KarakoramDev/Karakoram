// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useEffect, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap, type Schemas } from '@/api/client';
import { keys } from '@/api/keys';
import { useAuth } from '@/auth/session';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { hasAdminAssignment } from '@/auth/validation';
import { SelectField, TextAreaField, TextField } from '@/components/ui/Field';
import { Badge, Callout, Card, ErrorBlock, Loading } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';

const RESOURCES = ['clusters', 'cluster-credentials', 'database-users', 'backups', 'restores', 'backup-storages', 'operations', 'events', 'audit', 'monitoring', 'namespaces', 'accounts', 'api-tokens', 'rbac', 'settings', 'providers'];
const ACTIONS: Schemas['CanRequest']['action'][] = ['read', 'create', 'update', 'delete', 'export', '*'];
const OBJECT_EXAMPLES: Array<[string, string]> = [
  ['**', 'everything in every namespace (a bare * is normalized to this)'],
  ['dev', 'one namespace for the namespaces resource'],
  ['dev/**', 'the dev namespace and every cluster, storage or audit object in it'],
  ['prod/**', 'the prod namespace and everything in it'],
  ['prod/orders', 'one cluster (or storage) in prod'],
  ['prod/*', 'every cluster in prod, but not the namespace object itself'],
  ['orders', 'legacy form; normalized to default/orders for cluster-scoped resources and audit'],
];

/** Lightweight structural check so obvious typos never reach the server. */
function lintPolicy(csv: string): string[] {
  const problems: string[] = [];
  csv.split('\n').forEach((raw, i) => {
    const line = raw.trim();
    if (!line || line.startsWith('#')) return;
    const parts = line.split(',').map((p) => p.trim());
    if (parts[0] === 'p') {
      if (parts.length !== 6 && parts.length !== 5) problems.push(`Line ${i + 1}: p rules need "p, subject, resource, action, object[, allow|deny]".`);
      else if (parts.length === 6 && parts[5] !== 'allow' && parts[5] !== 'deny') problems.push(`Line ${i + 1}: effect must be allow or deny.`);
      else if (!RESOURCES.includes(parts[2] ?? '') && parts[2] !== '*') problems.push(`Line ${i + 1}: unknown resource "${parts[2]}".`);
    } else if (parts[0] === 'g') {
      if (parts.length !== 3) problems.push(`Line ${i + 1}: g rules need "g, subject, role".`);
    } else {
      problems.push(`Line ${i + 1}: rules start with p or g.`);
    }
  });
  return problems;
}

export function RbacPage() {
  const auth = useAuth();
  const toast = useToast();
  const queryClient = useQueryClient();
  const policy = useQuery({ queryKey: keys.rbacPolicy, queryFn: () => unwrap(api.GET('/rbac/policy')), enabled: auth.can('rbac', 'read') });
  const [text, setText] = useState('');
  const [seeded, setSeeded] = useState<string | null>(null);
  const [confirmLockout, setConfirmLockout] = useState(false);
  useEffect(() => {
    if (policy.data && policy.data.policy !== seeded) {
      setText(policy.data.policy);
      setSeeded(policy.data.policy);
    }
  }, [policy.data, seeded]);

  const save = useMutation({
    mutationFn: () => unwrap(api.PUT('/rbac/policy', { body: { policy: text } })),
    onSuccess: async (p) => {
      queryClient.setQueryData(keys.rbacPolicy, p);
      setSeeded(p.policy);
      setText(p.policy);
      toast.success('Policy saved and enforced.');
      await auth.refresh();
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  if (!auth.can('rbac', 'read')) return <Callout tone="info">You do not have permission to view the access policy.</Callout>;
  const dirty = text !== (policy.data?.policy ?? '');
  const problems = lintPolicy(text);
  const editable = auth.can('rbac', 'update');
  const removesAdminAssignments = !hasAdminAssignment(text);

  return (
    <>
      <Card
        title="Policy (Casbin CSV)"
        actions={
          editable ? (
            <>
              <Button size="sm" disabled={!dirty} onClick={() => setText(policy.data?.policy ?? '')}>
                Discard
              </Button>
              <Button size="sm" variant="primary" disabled={!dirty || problems.length > 0} loading={save.isPending} onClick={() => removesAdminAssignments ? setConfirmLockout(true) : save.mutate()}>
                Save policy
              </Button>
            </>
          ) : null
        }
      >
        {policy.isPending ? (
          <Loading />
        ) : policy.error ? (
          <ErrorBlock error={policy.error} retry={() => policy.refetch()} />
        ) : (
          <div className="stack">
            <p className="small muted">
              Rules: <code>p, subject, resource, action, object, allow|deny</code> and role bindings <code>g, subject, role</code>. Subjects are <code>user:&lt;name&gt;</code>, <code>group:&lt;oidc-group&gt;</code> or <code>role:&lt;name&gt;</code>. Objects
              are <code>&lt;namespace&gt;/&lt;cluster&gt;</code> globs for cluster-scoped resources and backup storages, <code>&lt;namespace&gt;</code> for the <code>namespaces</code> resource and <code>*</code> for global ones. Audit reads use the affected namespace/cluster, namespace or global object; <code>audit, read, dev/**</code> grants dev audit visibility only. Built-in roles{' '}
              <code>role:admin</code>, <code>role:operator</code>, <code>role:viewer</code> remain installed. Deny overrides allow.
            </p>
            <details className="small muted">
              <summary>Object pattern examples</summary>
              <dl className="kv" style={{ marginTop: 8 }}>
                {OBJECT_EXAMPLES.map(([pattern, meaning]) => (
                  <div key={pattern} style={{ display: 'contents' }}>
                    <dt className="mono">{pattern}</dt>
                    <dd>{meaning}</dd>
                  </div>
                ))}
              </dl>
              <p style={{ marginTop: 8 }}>
                <code>*</code> matches within one path segment, <code>**</code> spans segments and <code>x/**</code> also matches <code>x</code> itself. Restoring or cloning across namespaces additionally needs the <code>export</code> action on{' '}
                <code>backups</code> for the source cluster, e.g. <code>p, user:alice, backups, export, prod/orders</code>.
              </p>
            </details>
            <TextAreaField label="Policy" className="policy-editor" mono value={text} onChange={(e) => setText(e.target.value)} rows={18} readOnly={!editable} spellCheck={false} aria-describedby="policy-lint" />
            {dirty && removesAdminAssignments ? <Callout tone="warn" title="Administrator lockout risk">
              This policy has no user or group assignment to role:admin. Keep an enabled administrator with unrestricted rights. Saving requires confirmation; the server rejects policies without a verified administrator.
            </Callout> : null}
            <div id="policy-lint">
              {problems.length ? (
                <Callout tone="warn" title="Fix before saving">
                  <ul style={{ margin: 0, paddingLeft: 18 }}>
                    {problems.map((p) => (
                      <li key={p}>{p}</li>
                    ))}
                  </ul>
                </Callout>
              ) : dirty ? (
                <Badge plain tone="info">Unsaved changes — the server validates the full policy on save and keeps the previous one on error.</Badge>
              ) : null}
            </div>
          </div>
        )}
      </Card>

      <ConfirmDialog open={confirmLockout} onClose={() => setConfirmLockout(false)} title="Save without admin assignments?" confirmLabel="Confirm policy save" danger busy={save.isPending} onConfirm={() => { setConfirmLockout(false); return save.mutateAsync().catch(() => undefined); }}>
        <p>This removes every user/group assignment to role:admin. Confirm that an enabled account retains unrestricted administrative rights through other rules. The server keeps the previous policy if it cannot verify that.</p>
      </ConfirmDialog>
      <CanTester />

      <Card title="Your resolved permissions" flush>
        <div className="table-wrap">
          <table className="table">
            <thead>
              <tr>
                <th>Resource</th>
                <th>Action</th>
                <th>Object</th>
                <th>Effect</th>
              </tr>
            </thead>
            <tbody>
              {(auth.permissions?.permissions ?? []).map((p, i) => (
                <tr key={i}>
                  <td className="mono small">{p.resource}</td>
                  <td className="mono small">{p.action}</td>
                  <td className="mono small">{p.object}</td>
                  <td>
                    <Badge plain tone={p.effect === 'allow' ? 'ok' : 'danger'}>
                      {p.effect}
                    </Badge>
                  </td>
                </tr>
              ))}
              {(auth.permissions?.permissions ?? []).length === 0 ? (
                <tr>
                  <td colSpan={4} className="table-empty">
                    No permissions resolved for {auth.permissions?.subject ?? 'you'}.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </Card>
    </>
  );
}

function CanTester() {
  const auth = useAuth();
  const [subject, setSubject] = useState('');
  const [resource, setResource] = useState('clusters');
  const [action, setAction] = useState<Schemas['CanRequest']['action']>('read');
  const [object, setObject] = useState('*');
  const [result, setResult] = useState<{ allowed: boolean; query: string } | null>(null);
  const check = useMutation({
    mutationFn: () => unwrap(api.POST('/rbac/can', { body: { resource, action, object: object || '*', ...(subject.trim() ? { subject: subject.trim() } : {}) } })),
    onSuccess: (r) => setResult({ allowed: r.allowed, query: `${subject.trim() || auth.permissions?.subject || 'me'} → ${action} ${resource} ${object || '*'}` }),
    onError: (e) => setResult({ allowed: false, query: errorMessage(e) }),
  });
  return (
    <Card title="Can I…? (policy tester)">
      <form
        className="stack"
        onSubmit={(e) => {
          e.preventDefault();
          check.mutate();
        }}
      >
        <div className="grid-4">
          <TextField label="Subject (optional)" mono value={subject} onChange={(e) => setSubject(e.target.value)} placeholder={auth.permissions?.subject ?? 'user:alice'} hint="Checking other subjects needs rbac read." />
          <SelectField label="Action" value={action} onChange={(e) => setAction(e.target.value as Schemas['CanRequest']['action'])}>
            {ACTIONS.map((a) => (
              <option key={a} value={a}>
                {a}
              </option>
            ))}
          </SelectField>
          <SelectField label="Resource" value={resource} onChange={(e) => setResource(e.target.value)}>
            {RESOURCES.map((r) => (
              <option key={r} value={r}>
                {r}
              </option>
            ))}
          </SelectField>
          <TextField label="Object" mono value={object} onChange={(e) => setObject(e.target.value)} placeholder="*, <namespace> or <namespace>/<cluster>" />
        </div>
        <div className="row">
          <Button type="submit" loading={check.isPending}>
            Check
          </Button>
          {result ? (
            <span className="row">
              <Badge tone={result.allowed ? 'ok' : 'danger'}>{result.allowed ? 'allowed' : 'denied'}</Badge>
              <span className="small mono">{result.query}</span>
            </span>
          ) : null}
        </div>
      </form>
    </Card>
  );
}
