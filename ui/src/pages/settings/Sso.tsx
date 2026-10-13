// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useEffect, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, API_BASE, errorMessage, unwrap, type Schemas } from '@/api/client';
import { keys } from '@/api/keys';
import { useAuth } from '@/auth/session';
import { Button } from '@/components/ui/Button';
import { Switch, TextField } from '@/components/ui/Field';
import { Callout, Card, CodeLine, ErrorBlock, Loading } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';

type OIDC = Schemas['OIDCConfig'];

interface SsoForm {
  enabled: boolean;
  issuerURL: string;
  clientId: string;
  clientSecret: string;
  redirectURL: string;
  groupsClaim: string;
  scopes: string;
}

function fromSettings(s: Schemas['Settings']): SsoForm {
  return {
    enabled: s.oidc.enabled,
    issuerURL: s.oidc.issuerURL ?? '',
    clientId: s.oidc.clientId ?? '',
    clientSecret: '',
    redirectURL: s.oidc.redirectURL ?? '',
    groupsClaim: s.oidc.groupsClaim || 'groups',
    scopes: (s.oidc.scopes ?? ['openid', 'profile', 'email', 'groups']).join(' '),
  };
}

export function SsoPage() {
  const auth = useAuth();
  const toast = useToast();
  const queryClient = useQueryClient();
  const settings = useQuery({ queryKey: keys.settings, queryFn: () => unwrap(api.GET('/settings')), enabled: auth.can('settings', 'read') });
  const [form, setForm] = useState<SsoForm | null>(null);
  const [seed, setSeed] = useState<string | null>(null);
  useEffect(() => {
    if (settings.data) {
      const key = JSON.stringify(settings.data);
      if (key !== seed) {
        setForm(fromSettings(settings.data));
        setSeed(key);
      }
    }
  }, [settings.data, seed]);

  const save = useMutation({
    mutationFn: () => {
      if (!form) throw new Error('not loaded');
      const oidc: OIDC = {
        enabled: form.enabled,
        issuerURL: form.issuerURL.trim() || undefined,
        clientId: form.clientId.trim() || undefined,
        ...(form.clientSecret ? { clientSecret: form.clientSecret } : {}),
        redirectURL: form.redirectURL.trim() || undefined,
        groupsClaim: form.groupsClaim.trim() || 'groups',
        scopes: form.scopes.split(/\s+/).filter(Boolean),
      };
      return unwrap(api.PUT('/settings', { body: { oidc } }));
    },
    onSuccess: (s) => {
      queryClient.setQueryData(keys.settings, s);
      void queryClient.invalidateQueries({ queryKey: keys.version });
      toast.success('Single sign-on settings saved.');
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  if (!auth.can('settings', 'read')) return <Callout tone="info">You do not have permission to view settings.</Callout>;
  if (settings.isPending || !form) return settings.error ? <ErrorBlock error={settings.error} retry={() => settings.refetch()} /> : <Loading />;

  const editable = auth.can('settings', 'update');
  const set = (patch: Partial<SsoForm>) => setForm((f) => (f ? { ...f, ...patch } : f));
  const callback = `${window.location.origin}${API_BASE}/auth/oidc/callback`;
  const valid = !form.enabled || (/^https:\/\//.test(form.issuerURL) && form.clientId.trim() && (form.clientSecret || settings.data?.oidc.clientId));

  return (
    <Card
      title="OpenID Connect"
      actions={
        editable ? (
          <Button size="sm" variant="primary" loading={save.isPending} disabled={!valid} onClick={() => save.mutate()}>
            Save
          </Button>
        ) : null
      }
    >
      <fieldset disabled={!editable} style={{ border: 0, padding: 0, margin: 0 }} className="stack">
        <p className="small muted">
          Authorization-code flow with PKCE, handled server-side. Groups from the configured claim become <code>group:&lt;name&gt;</code> subjects for the RBAC policy. Register this redirect URI with your identity provider:
        </p>
        <CodeLine value={callback} />
        <Switch label="Enable Sign in with SSO" checked={form.enabled} onChange={(e) => set({ enabled: e.target.checked })} />
        <div className="grid-2">
          <TextField label="Issuer URL" mono required={form.enabled} value={form.issuerURL} onChange={(e) => set({ issuerURL: e.target.value })} placeholder="https://login.example.com/realms/main" hint="Discovery at <issuer>/.well-known/openid-configuration." />
          <TextField label="Client ID" mono required={form.enabled} value={form.clientId} onChange={(e) => set({ clientId: e.target.value })} />
          <TextField
            label={settings.data?.oidc.clientId ? 'Client secret (leave empty to keep)' : 'Client secret'}
            type="password"
            autoComplete="new-password"
            value={form.clientSecret}
            onChange={(e) => set({ clientSecret: e.target.value })}
            hint="Write-only; never shown again."
          />
          <TextField label="Redirect URL override (optional)" mono value={form.redirectURL} onChange={(e) => set({ redirectURL: e.target.value })} hint="Defaults to the callback above; set when behind a different public hostname." />
          <TextField label="Groups claim" mono value={form.groupsClaim} onChange={(e) => set({ groupsClaim: e.target.value })} />
          <TextField label="Scopes" mono value={form.scopes} onChange={(e) => set({ scopes: e.target.value })} hint="Space separated; openid is required." />
        </div>
      </fieldset>
    </Card>
  );
}
