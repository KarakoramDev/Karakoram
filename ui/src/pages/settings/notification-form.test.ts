// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import test from 'node:test';
import { channelBody, channelForm, channelFormError, destinationChanged, notificationCategories, updateChannelForm, type Channel } from './notification-form.ts';

const webhook: Channel = { name: 'alerts', type: 'webhook', enabled: true, webhook: { host: 'hooks.example.com', urlConfigured: true, signingSecretConfigured: true } };
const email: Channel = { name: 'mail', type: 'email', enabled: true, email: { host: 'smtp.example.com', port: 587, username: 'alerts', passwordConfigured: true, security: 'starttls', from: 'alerts@example.com', to: ['ops@example.com'] } };

test('edit never hydrates a secret, even when response incorrectly includes write-only fields', () => {
  const form = channelForm({ ...webhook, webhook: { ...webhook.webhook, url: 'https://private.example.com/token', signingSecret: 'secret' }, slack: { webhookUrl: 'slack-secret' }, email: { ...email.email!, password: 'smtp-secret' } });
  assert.equal(form.url, '');
  assert.equal(form.signingSecret, '');
  assert.equal(form.slackURL, '');
  assert.equal(form.password, '');
  assert.deepEqual(channelBody(form, webhook), { name: 'alerts', type: 'webhook', enabled: true, categories: [], namespaces: [], webhook: {} });
});

test('unchanged destination omits empty secrets and all read-only metadata', () => {
  const source = { ...email, createdAt: '2026-01-01T00:00:00Z', status: { pending: 3 }, namespaces: ['production'], categories: ['backup-failed' as const] };
  assert.deepEqual(channelBody(channelForm(source), source), { name: 'mail', type: 'email', enabled: true, categories: ['backup-failed'], namespaces: ['production'], email: { host: 'smtp.example.com', port: 587, username: 'alerts', security: 'starttls', from: 'alerts@example.com', to: ['ops@example.com'] } });
  assert.equal(destinationChanged({ ...channelForm(email), host: 'SMTP.EXAMPLE.COM' }, email), false);
});

test('new webhook hosts and ports require re-entering an existing signing secret', () => {
  for (const url of ['https://other.example.com/path', 'https://hooks.example.com:8443/path']) {
    const form = { ...channelForm(webhook), url };
    assert.equal(destinationChanged(form, webhook), true);
    assert.match(channelFormError(form, webhook)!, /Re-enter the signing secret/);
    assert.throws(() => channelBody(form, webhook), /Re-enter the signing secret/);
    assert.equal(channelBody({ ...form, signingSecret: 'new-key' }, webhook).webhook?.signingSecret, 'new-key');
  }
  const sameHost = { ...channelForm(webhook), url: 'https://HOOKS.EXAMPLE.COM/new-path' };
  assert.equal(destinationChanged(sameHost, webhook), false);
  assert.deepEqual(channelBody(sameHost, webhook).webhook, { url: 'https://HOOKS.EXAMPLE.COM/new-path' });
});

test('SMTP identity changes cannot silently retain a password', () => {
  for (const patch of [{ host: 'new.example.com' }, { port: '465' }, { username: 'different' }, { username: '' }]) {
    const form = { ...channelForm(email), ...patch };
    assert.equal(destinationChanged(form, email), true);
    assert.throws(() => channelBody(form, email), /new password/);
    assert.equal(channelBody({ ...form, password: 'replacement' }, email).email?.password, 'replacement');
  }
});

test('entered SMTP passwords are cleared on every destination identity change in create and edit forms', () => {
  for (const previous of [undefined, email]) {
    for (const patch of [{ host: 'new.example.com' }, { port: '465' }, { username: 'different' }, { username: 'Alerts' }, { username: '' }]) {
      const entered = updateChannelForm(channelForm(email), { password: 'password-for-original' }, previous);
      const changed = updateChannelForm(entered, patch, previous);
      assert.equal(changed.password, '');
      if (previous || changed.username) {
        assert.match(channelFormError(changed, previous)!, /SMTP password/);
        assert.throws(() => channelBody(changed, previous), /SMTP password/);
      }
      const reentered = updateChannelForm(changed, { password: 'password-for-new' }, previous);
      assert.equal(channelBody(reentered, previous).email?.password, 'password-for-new');
    }
  }
});

test('each unsaved SMTP destination change clears its password, not just changes from the saved identity', () => {
  let form = updateChannelForm(channelForm(email), { host: 'second.example.com' }, email);
  form = updateChannelForm(form, { password: 'second-host-password' }, email);
  form = updateChannelForm(form, { host: 'third.example.com' }, email);
  assert.equal(form.password, '');
  assert.throws(() => channelBody(form, email), /SMTP password/);
  form = updateChannelForm(form, { password: 'third-host-password' }, email);
  form = updateChannelForm(form, { host: email.email!.host }, email);
  assert.equal(form.password, '');
  assert.equal(channelBody(form, email).email?.password, undefined);
});

test('SMTP normalization and unrelated edits preserve the password entered for that identity', () => {
  let form = updateChannelForm(channelForm(email), { password: 'entered-password' }, email);
  for (const patch of [{ host: ' SMTP.EXAMPLE.COM ' }, { port: '0587' }, { username: ' alerts ' }, { security: 'tls' as const }, { from: 'new@example.com' }, { to: 'other@example.com' }, { enabled: false }]) {
    form = updateChannelForm(form, patch, email);
    assert.equal(form.password, 'entered-password');
    assert.equal(destinationChanged(form, email), false);
    assert.equal(channelBody(form, email).email?.password, 'entered-password');
  }
});

test('entered webhook secrets are cleared for changed hosts and explicit ports before save', () => {
  for (const url of ['https://other.example.com/path', 'https://hooks.example.com:8443/path', 'https://hooks.example.com:443/path']) {
    const entered = updateChannelForm(channelForm(webhook), { signingSecret: 'key-for-original' }, webhook);
    const changed = updateChannelForm(entered, { url }, webhook);
    assert.equal(changed.signingSecret, '');
    assert.equal(destinationChanged(changed, webhook), true);
    assert.throws(() => channelBody(changed, webhook), /Re-enter the signing secret/);
    const reentered = updateChannelForm(changed, { signingSecret: 'key-for-new' }, webhook);
    assert.equal(channelBody(reentered, webhook).webhook?.signingSecret, 'key-for-new');
    const returned = updateChannelForm(reentered, { url: '' }, webhook);
    assert.equal(returned.signingSecret, '');
    assert.equal(destinationChanged(returned, webhook), false);
  }
});

test('webhook path, query, userinfo and case edits preserve entered secrets for the same authority', () => {
  let form = updateChannelForm(channelForm(webhook), { signingSecret: 'entered-key' }, webhook);
  for (const url of ['https://HOOKS.EXAMPLE.COM/new-path', 'https://hooks.example.com/path?query=changed#fragment', 'http://user:pass@hooks.example.com/other', '']) {
    form = updateChannelForm(form, { url }, webhook);
    assert.equal(form.signingSecret, 'entered-key');
    assert.equal(destinationChanged(form, webhook), false);
    assert.equal(channelBody(form, webhook).webhook?.signingSecret, 'entered-key');
  }
});

test('new webhook forms bind entered keys to the current host and port without requiring optional signing', () => {
  let form = updateChannelForm(channelForm(), { name: 'new-alerts', url: 'https://hooks.example.com:443/path' });
  form = updateChannelForm(form, { signingSecret: 'entered-key' });
  form = updateChannelForm(form, { url: 'https://HOOKS.EXAMPLE.COM:443/other?changed' });
  assert.equal(form.signingSecret, 'entered-key');
  form = updateChannelForm(form, { url: 'https://hooks.example.com/path' });
  assert.equal(form.signingSecret, '');
  assert.equal(channelBody(form).webhook?.signingSecret, undefined);
  form = updateChannelForm(form, { signingSecret: 'new-key' });
  form = updateChannelForm(form, { url: 'https://other.example.com/path' });
  assert.equal(form.signingSecret, '');
});

test('saved webhook explicit default ports are not normalized away', () => {
  const previous: Channel = { ...webhook, webhook: { ...webhook.webhook!, host: 'hooks.example.com:443' } };
  const entered = updateChannelForm(channelForm(previous), { signingSecret: 'entered-key' }, previous);
  const same = updateChannelForm(entered, { url: 'https://HOOKS.EXAMPLE.COM:443/other' }, previous);
  assert.equal(same.signingSecret, 'entered-key');
  assert.equal(destinationChanged(same, previous), false);
  const changed = updateChannelForm(same, { url: 'https://hooks.example.com/other' }, previous);
  assert.equal(changed.signingSecret, '');
  assert.equal(destinationChanged(changed, previous), true);
  assert.throws(() => channelBody(changed, previous), /Re-enter the signing secret/);
});

test('type changes clear entered destinations and secrets without resurrecting them on return', () => {
  const entered = { ...channelForm(email), password: 'email-password', signingSecret: 'webhook-key', url: 'https://hooks.example.com/token', slackURL: 'https://hooks.slack.com/token' };
  const switched = updateChannelForm(entered, { type: 'webhook' }, email);
  for (const field of ['password', 'signingSecret', 'url', 'slackURL'] as const) assert.equal(switched[field], '');
  assert.match(channelFormError(switched, email)!, /webhook URL/);
  const returned = updateChannelForm(switched, { type: 'email' }, email);
  assert.equal(returned.password, '');
  assert.equal(channelBody(returned, email).email?.password, undefined);
});

test('changing channel type cannot borrow a previous type secret', () => {
  const slack = { ...channelForm(webhook), type: 'slack' as const };
  assert.throws(() => channelBody(slack, webhook), /Slack incoming webhook URL/);
  assert.deepEqual(channelBody({ ...slack, slackURL: 'https://hooks.slack.com/new-token', signingSecret: 'not-for-slack', password: 'not-for-slack' }, webhook).slack, { webhookUrl: 'https://hooks.slack.com/new-token' });
  const body = channelBody({ ...slack, slackURL: 'https://hooks.slack.com/new-token' }, webhook);
  assert.equal(body.webhook, undefined);
  assert.equal(body.email, undefined);
});

test('category and namespace scopes are explicit and empty means all', () => {
  assert.equal(notificationCategories.length, 11);
  assert.deepEqual(channelBody(channelForm(webhook), webhook).categories, []);
  const form = { ...channelForm(webhook), allCategories: false, categories: ['operation-failed' as const], namespaces: ' production, staging, production\n sandbox ' };
  assert.deepEqual(channelBody(form, webhook).namespaces, ['production', 'staging', 'sandbox']);
  assert.deepEqual(channelBody(form, webhook).categories, ['operation-failed']);
  assert.throws(() => channelBody({ ...form, categories: [] }, webhook), /at least one category/);
});

test('new channels require destinations, bounded ports, recipients and SMTP authentication', () => {
  assert.match(channelFormError(channelForm())!, /1–40/);
  assert.match(channelFormError({ ...channelForm(), name: 'alerts' })!, /webhook URL/);
  const unauthenticated: Channel = { ...email, email: { ...email.email!, username: '', passwordConfigured: false } };
  assert.equal(channelFormError(channelForm(unauthenticated)), undefined);
  assert.match(channelFormError({ ...channelForm(email), password: '' })!, /SMTP password/);
  assert.match(channelFormError({ ...channelForm(email), port: '65536' }, email)!, /1 to 65535/);
  assert.match(channelFormError({ ...channelForm(email), to: '' }, email)!, /1–20 recipients/);
});
