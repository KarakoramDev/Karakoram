// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useBackupStorages } from '@/api/hooks';
import { SelectField, Switch, TextAreaField, TextField } from '@/components/ui/Field';
import { ErrorBlock } from '@/components/ui/Primitives';
import type { WizardSectionProps } from '../types';
import { OptionsSection } from '../postgres/WizardSections';
import { object } from '../postgres/spec';

export function SupabaseOptions(props: WizardSectionProps) {
  const { provider, spec, onChange, namespace } = props;
  const storages = useBackupStorages(namespace);
  const options = object(spec.engineOptions);
  const set = (path: string[], value: unknown) => {
    const next = structuredClone(options);
    let node = next;
    for (const key of path.slice(0, -1)) { node[key] = { ...object(node[key]) }; node = node[key] as Record<string, unknown>; }
    if (value === undefined) delete node[path.at(-1)!]; else node[path.at(-1)!] = value;
    onChange({ ...spec, engineOptions: next });
  };
  const text = (label: string, path: string[], type = 'text') => {
    let value: unknown = options;
    for (const key of path) value = object(value)[key];
    return <TextField key={path.join('.')} label={label} type={type} value={typeof value === 'string' || typeof value === 'number' ? value : ''} onChange={(e) => set(path, e.target.value === '' ? undefined : type === 'number' ? Number(e.target.value) : e.target.value)} />;
  };
  const flag = (label: string, path: string[], fallback = false) => {
    let value: unknown = options;
    for (const key of path) value = object(value)[key];
    return <Switch key={path.join('.')} label={label} checked={typeof value === 'boolean' ? value : fallback} onChange={(e) => set(path, e.target.checked)} />;
  };
  const extensions = object(provider.engineOptions?.extensions);
  const allowed = extensions.enum ?? extensions.allowlist ?? extensions.allowed ?? object(extensions.items).enum;
  const selected = Array.isArray(options.extensions) ? options.extensions as string[] : [];
  return <div className="stack">
    <OptionsSection {...props} />
    {text('Site URL', ['siteUrl'], 'url')}
    <TextAreaField label="Additional redirect URLs (one per line)" value={Array.isArray(options.additionalRedirectUrls) ? options.additionalRedirectUrls.join('\n') : ''} onChange={(e) => set(['additionalRedirectUrls'], e.target.value.split('\n').filter(Boolean))} />
    <strong>Authentication</strong>
    {flag('Disable signup', ['auth', 'disableSignup'])}{flag('Automatically confirm email', ['auth', 'autoconfirm'])}{text('JWT expiry (seconds)', ['auth', 'jwtExpiry'], 'number')}
    <strong>SMTP</strong>
    <p className="small muted">Set the SMTP password after creation in the Functions & SMTP tab. Passwords are never stored in engine options.</p>
    {text('SMTP host', ['auth', 'smtp', 'host'])}{text('SMTP port', ['auth', 'smtp', 'port'], 'number')}{text('SMTP user', ['auth', 'smtp', 'user'])}{text('Admin email', ['auth', 'smtp', 'adminEmail'], 'email')}{text('Sender name', ['auth', 'smtp', 'senderName'])}
    <strong>REST API</strong>
    <TextField label="Exposed schemas (comma separated)" value={Array.isArray(object(options.rest).schemas) ? (object(options.rest).schemas as string[]).join(', ') : ''} onChange={(e) => set(['rest', 'schemas'], e.target.value.split(',').map((v) => v.trim()).filter(Boolean))} />
    {text('Maximum rows', ['rest', 'maxRows'], 'number')}
    <strong>Object storage</strong>
    {storages.error ? <ErrorBlock error={storages.error} retry={() => storages.refetch()} /> : null}
    <SelectField label="Registered backend storage" value={String(object(options.storage).backendStorage ?? '')} onChange={(e) => set(['storage', 'backendStorage'], e.target.value || undefined)}><option value="">Choose storage</option>{storages.data?.items.map((s) => <option key={`${s.namespace}/${s.name}`} value={s.name}>{s.name}</option>)}</SelectField>
    {text('Bucket', ['storage', 'bucket'])}{text('File size limit (bytes)', ['storage', 'fileSizeLimit'], 'number')}{flag('Image transformation (requires imgproxy)', ['storage', 'imageTransformation'])}
    {flag('Public gateway HTTPS', ['gateway', 'publicHttps'])}
    <TextAreaField label="Gateway CORS origins (one per line; empty allows any origin)" value={Array.isArray(object(options.gateway).corsOrigins) ? (object(options.gateway).corsOrigins as string[]).join('\n') : ''} onChange={(e) => set(['gateway', 'corsOrigins'], e.target.value.split('\n').map((v) => v.trim()).filter(Boolean))} />
    {text('Studio organization', ['studio', 'organization'])}{text('Studio project', ['studio', 'project'])}
    {flag('Verify function JWTs', ['functions', 'verifyJwt'], true)}
    <strong>Extensions</strong>
    {Array.isArray(allowed) ? allowed.filter((v): v is string => typeof v === 'string').map((name) => <Switch key={name} label={name} checked={selected.includes(name)} onChange={(e) => set(['extensions'], e.target.checked ? [...selected, name] : selected.filter((v) => v !== name))} />) : <p className="small muted">The catalog does not advertise optional extensions for this release.</p>}
  </div>;
}
