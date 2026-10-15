// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
import assert from 'node:assert/strict';
import test from 'node:test';
import { assignedRoles, assignedUsers, certificateDownload, clusterEmptyCopy, colocatedGroups, credentialDatabaseLabel, FCV_GUIDANCE, fcvObservation, generationState, latestSuccessfulBackup, maintenanceCopy, observedVersion, passwordValid, psmdbConnect, refreshSubmissionBody, requireUpgradeBackup, scalingReview } from './detail-state.ts';
import { fetchBackupInventory } from './backup-inventory.ts';
import { elasticHealthNotices, elasticMemberSummary } from './detail-state.ts';

const resources = { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 1, maxRam: 2, minDisk: 5, maxDisk: 10 };
const spec = { components: { engine: { replicas: 1, resources } } };

test('membership review does not invent resources, TLS or exposure changes', () => {
  const copy = scalingReview(spec, { components: { engine: { replicas: 3 } } }).join(' ');
  assert.match(copy, /initial sync and voting promotion/);
  assert.doesNotMatch(copy, /restart|TLS|certificate|public/);
});
test('RabbitMQ broker scale-up preserves existing queue replica counts and points to explicit growth', () => {
  const rabbit = { ...spec, provider: 'rabbitmq' };
  const copy = scalingReview(rabbit, { components: { engine: { replicas: 3 } } }).join(' ');
  assert.match(copy, /Existing quorum queues keep their replica count/);
  assert.match(copy, /Queue recovery.*Management API/);
  for (const replicas of [1, 0]) assert.doesNotMatch(scalingReview(rabbit, { components: { engine: { replicas } } }).join(' '), /quorum queues|Queue recovery/);
  assert.doesNotMatch(scalingReview({ ...spec, provider: 'psmdb' }, { components: { engine: { replicas: 3 } } }).join(' '), /quorum queues|Queue recovery/);
});
test('CPU review describes existing-volume resource updates, not data movement', () => {
  const copy = scalingReview(spec, { components: { engine: { resources: { ...resources, maxCpu: 4 } } } }).join(' ');
  assert.match(copy, /CPU limits apply without data movement/);
  assert.doesNotMatch(copy, /TLS|certificate|public/);
});
test('RAM review describes rolling restart without unrelated effects', () => {
  const copy = scalingReview(spec, { components: { engine: { resources: { ...resources, maxRam: 4 } } } }).join(' ');
  assert.match(copy, /rolling restart/);
  assert.doesNotMatch(copy, /TLS|certificate|public/);
});
test('TLS and exposure effects require their respective patch fields', () => {
  assert.match(scalingReview(spec, { tls: { mode: 'disabled' }, exposure: { public: true } }).join(' '), /TLS.*public routes/);
  assert.deepEqual(scalingReview(spec, {}), []);
});
test('unknown host groups never assert co-location', () => {
  assert.deepEqual(colocatedGroups([{ name: 'unknown', members: ['a', 'b'] }, { name: '', members: ['c', 'd'] }]), []);
  assert.equal(colocatedGroups([{ name: 'host-fingerprint', members: ['a', 'b'] }]).length, 1);
});
test('observed versions never fall back to desired target and prefer members', () => {
  assert.equal(observedVersion({ members: [] }), undefined);
  assert.equal(observedVersion({ version: '8.0', members: [{ version: '7.0' }] }), '7.0');
  assert.equal(observedVersion({ members: [{ version: '7.0' }, { version: '8.0' }] }), '7.0, 8.0');
});
test('singleton maintenance warns about downtime and neutral engines omit MongoDB language', () => {
  assert.match(maintenanceCopy('psmdb', 1), /Downtime.*only member/);
  assert.match(maintenanceCopy('other', 3), /majority\/quorum/);
  assert.doesNotMatch(maintenanceCopy('other', 3), /primary|secondaries/i);
  assert.match(FCV_GUIDANCE, /downgrades are not supported/);
});
test('Elasticsearch maintenance explains database downtime and separate ordered stack restarts', () => {
  const singleton = maintenanceCopy('elasticsearch', 1);
  assert.match(singleton, /only Elasticsearch data node interrupts service/);
  assert.match(singleton, /elected master last.*followed by Kibana, Logstash and APM Server/);
  assert.match(singleton, /Kibana restarts separately/);
  const multiple = maintenanceCopy('elasticsearch', 3);
  assert.match(multiple, /master quorum and redundant shard copies/);
  assert.match(multiple, /Kibana restarts separately/);
  assert.doesNotMatch(multiple, /only Elasticsearch data node|Secondaries/);
});
test('newest successful backup ignores failed and pending records', () => {
  const backups = [{ phase: 'Succeeded', completedAt: '2026-01-01T00:00:00Z' }, { phase: 'Succeeded', completedAt: '2026-01-02T00:00:00Z' }, { phase: 'Failed', completedAt: '2026-01-03T00:00:00Z' }];
  assert.equal(latestSuccessfulBackup(backups), backups[1]);
  assert.equal(latestSuccessfulBackup([{ phase: 'Pending' }]), undefined);
});
test('password minimum counts Unicode codepoints and supports omitted passwords', () => {
  assert.equal(passwordValid(''), true);
  assert.equal(passwordValid('abcdefg'), false);
  assert.equal(passwordValid('éééééééé'), true);
  assert.equal(passwordValid('𐀀𐀀𐀀𐀀'), false);
});
test('role deletion identifies only users assigned the matching database role', () => {
  assert.deepEqual(assignedUsers([{ username: 'app', database: 'shop', roles: [{ role: 'reader', db: 'shop' }] }, { username: 'other', database: 'admin', roles: [{ role: 'reader', db: 'admin' }] }], { name: 'reader', database: 'shop' }), ['app@shop']);
});
test('role deletion identifies inherited assignments by role name and database', () => {
  const roles = [
    { name: 'manager', database: 'admin', roles: [{ role: 'reader', db: 'shop' }] },
    { name: 'other', database: 'shop', roles: [{ role: 'reader', db: 'admin' }] },
    { name: 'unassigned', database: 'shop', roles: [] },
  ];
  assert.deepEqual(assignedRoles(roles, { name: 'reader', database: 'shop' }), ['manager@admin']);
  assert.deepEqual(assignedRoles(roles, { name: 'missing', database: 'shop' }), []);
});
test('permission-aware empty state explains access without inviting unauthorized creation', () => {
  assert.match(clusterEmptyCopy(false).message, /Ask an administrator/);
  assert.equal(clusterEmptyCopy(true).title, 'No clusters yet');
});
test('connection examples retain URI parameters and enable CA verification', () => {
  assert.equal(psmdbConnect('mongodb://host/?replicaSet=rs').driver, 'mongodb://host/?replicaSet=rs&tlsCAFile=ca.pem');
  assert.match(psmdbConnect('mongodb://host').shell, /mongosh.*--tlsCAFile ca.pem/);
});

test('unobserved aggregate version does not masquerade as a running binary', () => {
  assert.equal(observedVersion({ version: '8.0', members: [] }), undefined);
});
test('FCV unknown state is not aligned', () => {
  assert.equal(fcvObservation('', '8.0', '8.0'), 'not yet observed');
  assert.equal(fcvObservation('8.0', undefined, '8.0'), 'binary version not yet observed');
  assert.equal(fcvObservation('7.0', '8.0', '8.0'), 'behind binary major 8.0');
});
test('completed suspension is not described as reconciling', () => {
  assert.equal(generationState('Suspended', 1, 2), 'runtime suspended');
  assert.equal(generationState('Ready', 1, 2), 'reconciling changes');
});
test('removal review does not promise adding or synchronizing members', () => {
  const copy = scalingReview({ components: { engine: { replicas: 3, resources } } }, { components: { engine: { replicas: 1 } } }).join(' ');
  assert.match(copy, /remove members/);
  assert.doesNotMatch(copy, /add members|initial sync|restart/);
});

test('credential labels match the engine and disappear when no database context exists', () => {
  assert.equal(credentialDatabaseLabel('psmdb', [{ database: 'admin' }]), 'Auth database');
  assert.equal(credentialDatabaseLabel('rabbitmq', [{ database: '/' }]), 'Virtual host');
  assert.equal(credentialDatabaseLabel('elasticsearch', [{ database: '' }]), undefined);
});
test('CA download preserves PEM lines and names the trust file ca.pem', () => {
  const pem = '-----BEGIN CERTIFICATE-----\nYWJj\n-----END CERTIFICATE-----\n';
  const download = certificateDownload(pem);
  assert.equal(download.filename, 'ca.pem');
  assert.equal(decodeURIComponent(download.href.slice(download.href.indexOf(',') + 1)), pem);
});

test('backup summaries follow every cursor and find a successful backup beyond page one', async () => {
  const cursors: (string | undefined)[] = [];
  const inventory = await fetchBackupInventory(async (cursor) => {
    cursors.push(cursor);
    return cursor ? { items: [{ phase: 'Succeeded', completedAt: '2026-01-02T00:00:00Z' }] } : { items: [{ phase: 'Failed', completedAt: '2026-01-03T00:00:00Z' }], nextCursor: 'next' };
  });
  assert.deepEqual(cursors, [undefined, 'next']);
  assert.equal(latestSuccessfulBackup(inventory.items)?.phase, 'Succeeded');
});

test('an upgrade requiring a backup cannot start without a successful record', () => {
  assert.throws(() => requireUpgradeBackup(true, false), /requires a successful backup/);
  assert.doesNotThrow(() => requireUpgradeBackup(true, true));
  assert.doesNotThrow(() => requireUpgradeBackup(false, false));
});

test('RAM-only review never claims storage changes', () => {
  const copy = scalingReview(spec, { components: { engine: { resources: { ...resources, maxRam: 4 } } } }).join(' ');
  assert.doesNotMatch(copy, /storage bounds/i);
});

test('unchanged retries retain a key, but edited submissions reset it exactly once', () => {
  const attempt = { body: 'original' };
  let resets = 0;
  const reset = () => { resets++; };
  refreshSubmissionBody(attempt, 'original', reset);
  assert.equal(resets, 0);
  refreshSubmissionBody(attempt, 'edited', reset);
  assert.equal(resets, 1);
  refreshSubmissionBody(attempt, 'edited', reset);
  assert.equal(resets, 1);
});

test('Elasticsearch scale-down retains the draining master in observed counts, not healthy counts', () => {
  const members = [
    { role: 'master,data', state: 'joined', health: true },
    { role: 'master,data', state: 'draining', health: true },
    { role: 'master,data', state: 'removing', health: false },
    { role: 'kibana', state: 'joined', health: true },
  ];
  assert.deepEqual(elasticMemberSummary({ engine: { replicas: 1 } }, members), { desired: 1, observed: 3, draining: 2, healthy: 1, detail: '1 desired · 3 observed · 2 draining' });
  assert.equal(elasticMemberSummary({ engine: { replicas: 1 }, extra: { master: { replicas: 3 } } }, members).desired, 4);
});

test('Elasticsearch drain and yellow-health notices are visible independently of the condition table', () => {
  const message = 'Shard drain blocked for searchn2: insufficient replicas. Add capacity or lower index replicas; never force removal.';
  const notices = elasticHealthNotices([
    { type: 'ShardDrainBlocked', status: 'True', message },
    { type: 'ClusterHealth', status: 'True', reason: 'yellow' },
  ]);
  assert.deepEqual(notices[0], { title: 'Shard drain blocked', message });
  assert.match(notices[1]!.message, /Primary shards are allocated.*replica shards are unassigned.*redundancy is reduced/);
  assert.deepEqual(elasticHealthNotices([{ type: 'ShardDrainBlocked', status: 'False', message }]), []);
});
