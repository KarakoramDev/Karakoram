// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';

export type Channel = Schemas['NotificationChannel'];
export const notificationCategories = [
  'backup-failed', 'cluster-degraded', 'cluster-recovered', 'volume-deletion-refused',
  'certificate-expiring', 'deployment-retries-exhausted', 'operation-failed',
  'members-co-located', 'backup-drill-failed', 'control-plane-backup-failed', 'agents-outdated',
] as const satisfies readonly Schemas['NotificationCategory'][];

export interface ChannelForm {
  name: string;
  type: Channel['type'];
  enabled: boolean;
  allCategories: boolean;
  categories: Schemas['NotificationCategory'][];
  namespaces: string;
  url: string;
  signingSecret: string;
  slackURL: string;
  host: string;
  port: string;
  security: 'starttls' | 'tls';
  username: string;
  password: string;
  from: string;
  to: string;
}

export function channelForm(channel?: Channel): ChannelForm {
  return {
    name: channel?.name ?? '', type: channel?.type ?? 'webhook', enabled: channel?.enabled ?? true,
    allCategories: !channel?.categories?.length, categories: channel?.categories ?? [],
    namespaces: (channel?.namespaces ?? []).join(', '),
    // Explicitly never hydrate write-only values, even from a malformed response.
    url: '', signingSecret: '', slackURL: '', password: '',
    host: channel?.email?.host ?? '', port: String(channel?.email?.port ?? 587),
    security: channel?.email?.security ?? 'starttls', username: channel?.email?.username ?? '',
    from: channel?.email?.from ?? '', to: (channel?.email?.to ?? []).join(', '),
  };
}

function hostIdentity(url: string): string {
  // Go's URL.Host keeps explicit default ports; WHATWG URL.host drops them.
  const authority = url.trim().match(/^[a-z][a-z0-9+.-]*:\/\/([^/?#]*)/i)?.[1] ?? '';
  return authority.slice(authority.lastIndexOf('@') + 1).toLowerCase();
}

function webhookIdentity(form: ChannelForm, previous?: Channel): string {
  return form.url.trim() ? hostIdentity(form.url) : previous?.type === 'webhook' ? previous.webhook?.host?.toLowerCase() ?? '' : '';
}

function emailIdentityChanged(form: ChannelForm, host: string, port: number, username: string): boolean {
  return form.host.trim().toLowerCase() !== host.toLowerCase()
    || Number(form.port) !== port || form.username.trim() !== username;
}

export function updateChannelForm(current: ChannelForm, patch: Partial<ChannelForm>, previous?: Channel): ChannelForm {
  const next = { ...current, ...patch };
  if (next.type !== current.type) {
    return { ...next, url: '', signingSecret: '', slackURL: '', password: '' };
  }
  if (next.type === 'email' && emailIdentityChanged(next, current.host.trim(), Number(current.port), current.username.trim())) {
    next.password = '';
  }
  if (next.type === 'webhook' && webhookIdentity(next, previous) !== webhookIdentity(current, previous)) {
    next.signingSecret = '';
  }
  return next;
}

export function destinationChanged(form: ChannelForm, previous?: Channel): boolean {
  if (!previous || form.type !== previous.type) return true;
  if (form.type === 'webhook') return webhookIdentity(form, previous) !== (previous.webhook?.host?.toLowerCase() ?? '');
  if (form.type === 'email') return emailIdentityChanged(form, previous.email?.host ?? '', previous.email?.port ?? 0, previous.email?.username ?? '');
  return Boolean(form.slackURL);
}

function list(value: string): string[] { return [...new Set(value.split(/[\n,]+/).map((v) => v.trim()).filter(Boolean))]; }

export function channelFormError(form: ChannelForm, previous?: Channel): string | undefined {
  if (!/^[a-z][a-z0-9-]{0,39}$/.test(form.name)) return 'Use 1–40 lowercase letters, digits or hyphens, starting with a letter.';
  if (!form.allCategories && !form.categories.length) return 'Select at least one category or choose all categories.';
  const sameType = previous?.type === form.type;
  if (form.type === 'webhook') {
    if (!form.url && !(sameType && previous?.webhook?.urlConfigured)) return 'Enter a webhook URL.';
    if (form.url && !/^https?:\/\//.test(form.url.trim())) return 'Enter an HTTP(S) webhook URL.';
    if (sameType && previous?.webhook?.signingSecretConfigured && destinationChanged(form, previous) && !form.signingSecret) return 'Destination changed. Re-enter the signing secret for the new host.';
  } else if (form.type === 'slack') {
    if (!form.slackURL && !(sameType && previous?.slack?.webhookUrlConfigured)) return 'Enter a Slack incoming webhook URL.';
    if (form.slackURL && !/^https?:\/\//.test(form.slackURL.trim())) return 'Enter an HTTP(S) Slack webhook URL.';
  } else {
    if (!form.host.trim() || !Number.isInteger(Number(form.port)) || Number(form.port) < 1 || Number(form.port) > 65535) return 'Enter an SMTP host and port from 1 to 65535.';
    if (!form.from.trim() || list(form.to).length < 1 || list(form.to).length > 20) return 'Enter a sender and 1–20 recipients.';
    const changedPassword = sameType && previous?.email?.passwordConfigured && destinationChanged(form, previous);
    if (!form.password && (changedPassword || (form.username.trim() && !(sameType && previous?.email?.passwordConfigured)))) return 'Enter the SMTP password. A changed host, port or username requires a new password.';
  }
  return undefined;
}

export function channelBody(form: ChannelForm, previous?: Channel): Channel {
  const error = channelFormError(form, previous);
  if (error) throw new Error(error);
  const common = { name: form.name, type: form.type, enabled: form.enabled, categories: form.allCategories ? [] : form.categories, namespaces: list(form.namespaces) };
  if (form.type === 'webhook') return { ...common, webhook: {
    ...(form.url ? { url: form.url.trim() } : {}), ...(form.signingSecret ? { signingSecret: form.signingSecret } : {}),
  } };
  if (form.type === 'slack') return { ...common, slack: { ...(form.slackURL ? { webhookUrl: form.slackURL.trim() } : {}) } };
  return { ...common, email: { host: form.host.trim(), port: Number(form.port), security: form.security,
    username: form.username.trim(), from: form.from.trim(), to: list(form.to), ...(form.password ? { password: form.password } : {}),
  } };
}
