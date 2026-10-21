// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import createClient from 'openapi-fetch';
import { QueryClient } from '@tanstack/react-query';
import type { paths } from '../../api/schema';
import type { Schemas } from '../../api/client';
import { INITIAL_REVEAL, revealReducer, revealUnsealKeys, shareLabel, unsealKeysExport, type UnsealKeys } from './reveal.ts';
import { buildConnectionWrite, buildPatch, buildRestoreShares, connectionTargets, emptyRestoreShares, emptyRole, formatTtl, memberSealState, openbaoChangeReview, optionsPatch, readOptions, sealOverview, specSummary, storageClusters, validateOptions, validateRestore, validateSpec, writeOptions, type OpenBaoOptions } from './spec.ts';
import { openbaoMetadata, openbaoMonitoringPanels } from './metadata.ts';
import { engines, monitoringPanels, publicExposureMetadata, tabAvailable } from '../index.ts';
import { validateWizard } from '../../pages/clusters/wizard/validation.ts';
import { applyMergePatch } from '../changes.ts';

const resources: Schemas['Resources'] = { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 1, maxRam: 2, minDisk: 5, maxDisk: 20 };
const spec: Schemas['ClusterSpec'] = { provider: 'openbao', version: '2.7.1', topology: { type: 'cluster' }, components: { engine: { replicas: 3, resources } }, tls: { mode: 'requireTLS', ca: 'selfSigned' } };
const defaults = readOptions(spec);
const withOptions = (patch: (o: OpenBaoOptions) => OpenBaoOptions) => writeOptions(spec, patch(structuredClone(defaults)));

test('registry descriptor advertises the HTTPS API port, Raft summary and catalog metric series', () => {
  assert.equal(engines.openbao, openbaoMetadata);
  assert.equal(engines.openbao?.label, 'OpenBao');
  assert.match(engines.openbao?.description ?? '', /integrated Raft storage/);
  assert.deepEqual(publicExposureMetadata(engines.openbao, spec), { publicPort: 8200, publicPortLabel: 'HTTPS API' });
  const names = monitoringPanels('openbao').flatMap((panel) => panel.names);
  for (const name of ['up', 'initialized', 'sealed', 'active', 'raftCommittedIndex', 'raftAppliedIndex', 'raftLeaderLastContactSeconds', 'autopilotHealthy', 'autopilotFailureTolerance', 'requests', 'leases', 'memoryBytes']) assert.ok(names.includes(`openbao.${name}`), name);
  assert.ok(names.includes('cpu.percent'));
  assert.equal(openbaoMonitoringPanels.find((panel) => panel.names.includes('openbao.active'))?.format?.(1), 'Active');
  assert.equal(engines.openbao?.topologySummary?.({ spec } as Schemas['Cluster']), '3-member OpenBao cluster · 3 of 5 shares · Raft storage');
  assert.equal(specSummary(withOptions((o) => ({ ...o, seal: { ...o.seal, type: 'awskms' }, storage: { ...o.storage, type: 'postgresql', postgresql: { cluster: 'pg' } } }))), '3-member OpenBao cluster · AWS KMS auto-unseal · PostgreSQL storage');
  assert.equal(tabAvailable({ name: 'openbao', capabilities: ['monitoring', 'tls', 'publicExposure'] }, 'users'), false);
  assert.equal(tabAvailable({ name: 'openbao', capabilities: ['monitoring', 'tls', 'publicExposure'] }, 'backups'), false);
  assert.equal(tabAvailable({ name: 'openbao', capabilities: ['monitoring', 'tls', 'publicExposure'] }, 'openbao'), true);
});

test('options read defaults tolerantly and write only the selected seal and storage blocks', () => {
  assert.deepEqual(defaults.unseal, { shares: 5, threshold: 3 });
  assert.equal(defaults.seal.type, 'shamir');
  assert.equal(defaults.seal.transit.mountPath, 'transit/');
  assert.equal(defaults.storage.type, 'raft');
  assert.equal(defaults.logLevel, 'info');
  assert.equal(readOptions({ engineOptions: { seal: { type: 'bogus' }, unseal: { shares: 'x' }, logLevel: 42 } }).seal.type, 'shamir');
  const transit = withOptions((o) => ({ ...o, seal: { ...o.seal, type: 'transit', transit: { ...o.seal.transit, address: 'https://vault.example.net', keyName: 'unseal', token: 's.secret', mountPath: '' } }, logLevel: 'debug' }));
  assert.deepEqual(transit.engineOptions, { unseal: { shares: 5, threshold: 3 }, seal: { type: 'transit', transit: { address: 'https://vault.example.net', mountPath: 'transit/', keyName: 'unseal', token: 's.secret' } }, storage: { type: 'raft' }, logLevel: 'debug' });
  const aws = withOptions((o) => ({ ...o, seal: { ...o.seal, type: 'awskms', awskms: { region: 'eu-central-1', kmsKeyId: 'alias/bao', endpoint: '', accessKeyId: 'AKIA', secretAccessKey: 'secret' } }, storage: { type: 'postgresql', postgresql: { cluster: 'pg' } } }));
  assert.deepEqual(aws.engineOptions?.seal, { type: 'awskms', awskms: { region: 'eu-central-1', kmsKeyId: 'alias/bao', accessKeyId: 'AKIA', secretAccessKey: 'secret' } });
  assert.deepEqual(aws.engineOptions?.storage, { type: 'postgresql', postgresql: { cluster: 'pg' } });
  assert.equal(JSON.stringify(aws.engineOptions).includes('transit'), false);
});

test('validateSpec mirrors the server share, seal, storage and TLS rules', () => {
  assert.deepEqual(validateSpec(spec), []);
  assert.deepEqual(validateSpec(withOptions((o) => ({ ...o, unseal: { shares: 1, threshold: 1 } }))), []);
  assert.match(validateSpec(withOptions((o) => ({ ...o, unseal: { shares: 11, threshold: 3 } })))[0]!, /between 1 and 10/);
  assert.match(validateSpec(withOptions((o) => ({ ...o, unseal: { shares: 3, threshold: 4 } })))[0]!, /cannot exceed/);
  assert.match(validateSpec(withOptions((o) => ({ ...o, unseal: { shares: 3, threshold: 1 } })))[0]!, /at least 2/);
  assert.match(validateSpec(withOptions((o) => ({ ...o, unseal: { shares: 2.5, threshold: 2 } })))[0]!, /whole number/);
  assert.match(validateSpec(withOptions((o) => ({ ...o, unseal: { shares: 3, threshold: 0 } })))[0]!, /at least 1/);
  const transit = (patch: Partial<OpenBaoOptions['seal']['transit']>) => validateSpec(withOptions((o) => ({ ...o, seal: { ...o.seal, type: 'transit', transit: { ...o.seal.transit, address: 'https://vault.example.net', keyName: 'k', token: 't', ...patch } } })));
  assert.deepEqual(transit({}), []);
  assert.match(transit({ address: 'http://vault.example.net' })[0]!, /https:\/\//);
  assert.match(transit({ keyName: ' ' })[0]!, /key name/);
  assert.match(transit({ mountPath: 'transit' })[0]!, /end with a slash/);
  assert.match(transit({ token: '' })[0]!, /token is required/);
  const aws = (patch: Partial<OpenBaoOptions['seal']['awskms']>) => validateSpec(withOptions((o) => ({ ...o, seal: { ...o.seal, type: 'awskms', awskms: { region: 'eu-central-1', kmsKeyId: 'key', endpoint: '', accessKeyId: 'a', secretAccessKey: 's', ...patch } } })));
  assert.deepEqual(aws({}), []);
  assert.match(aws({ region: '' })[0]!, /region/);
  assert.match(aws({ kmsKeyId: '' })[0]!, /key ID/);
  assert.match(aws({ endpoint: 'kms.local' })[0]!, /endpoint must be an https/);
  assert.match(aws({ secretAccessKey: '' })[0]!, /both required/);
  assert.match(validateSpec(withOptions((o) => ({ ...o, storage: { ...o.storage, type: 'postgresql' } })))[0]!, /Choose the PostgreSQL cluster/);
  assert.deepEqual(validateSpec(withOptions((o) => ({ ...o, storage: { type: 'postgresql', postgresql: { cluster: 'pg' } } }))), []);
  assert.match(validateSpec({ ...spec, tls: { mode: 'disabled', ca: 'selfSigned' } })[0]!, /requires TLS/);
  assert.match(validateSpec({ ...spec, engineConfig: 'ui = true' })[0]!, /engine configuration overrides/);
  // Persisted specs never echo write-only credentials: the detail editor must not demand them again.
  const persisted = readOptions(withOptions((o) => ({ ...o, seal: { ...o.seal, type: 'transit', transit: { ...o.seal.transit, address: 'https://vault.example.net', keyName: 'k' } } })));
  assert.match(validateOptions(persisted, true)[0]!, /token is required/);
  assert.deepEqual(validateOptions(persisted, false), []);
  // The wizard surfaces descriptor errors in the Basics step.
  const provider: Schemas['Provider'] = { name: 'openbao', displayName: 'OpenBao', topologies: [{ type: 'cluster', displayName: 'Cluster', defaults: spec, componentSchemas: {}, limits: {} }], versions: [{ version: '2.7.1', major: '2', status: 'supported', default: true }], backupTypes: [], capabilities: ['monitoring', 'tls', 'publicExposure'], presets: [], upgradePaths: [] };
  assert.ok(validateWizard('bao', 'default', withOptions((o) => ({ ...o, unseal: { shares: 3, threshold: 4 } })), [], provider).basics.some((e) => /cannot exceed/.test(e)));
});

test('post-creation patches touch only the log level and describe Raft membership consequences', () => {
  const current = withOptions((o) => ({ ...o, seal: { ...o.seal, type: 'shamir' } }));
  assert.deepEqual(optionsPatch(current, current), {});
  const relevelled = writeOptions(current, { ...readOptions(current), logLevel: 'warn', unseal: { shares: 7, threshold: 4 }, storage: { type: 'postgresql', postgresql: { cluster: 'pg' } } });
  assert.deepEqual(optionsPatch(current, relevelled), { engineOptions: { logLevel: 'warn' } });
  const grown = { ...relevelled, components: { engine: { replicas: 5, resources } } };
  assert.deepEqual(buildPatch(current, grown), { components: { engine: { replicas: 5 } }, engineOptions: { logLevel: 'warn' } });
  assert.deepEqual(readOptions(applyMergePatch(current, buildPatch(current, grown)) as Schemas['ClusterSpec']).unseal, { shares: 5, threshold: 3 });
  const notes = openbaoChangeReview.disruptions(current, grown);
  assert.match(notes[0]!, /grows from 3 to 5/);
  assert.match(notes[1]!, /standbys first/);
  assert.match(openbaoChangeReview.disruptions(grown, current)[0]!, /QuorumBlocked/);
  assert.match(openbaoChangeReview.disruptions(current, { ...current, components: { engine: { replicas: 1, resources } }, exposure: { public: true, ipType: 'IPV4', allowlist: ['203.0.113.0/24'] } }).at(-1)!, /public port 8200/);
  assert.deepEqual(openbaoChangeReview.disruptions(current, current), []);
});

test('seal overview maps edge member encoding, Raft voter conditions and cluster conditions to guidance', () => {
  const member = (name: string, role: Schemas['Member']['role'], state: string, health = true): Schemas['Member'] => ({ name, role, state, set: 'openbao', health });
  assert.equal(memberSealState(member('a', 'primary', 'RUNNING')), 'active');
  assert.equal(memberSealState(member('a', 'secondary', 'RUNNING')), 'standby');
  assert.equal(memberSealState(member('a', 'unknown', 'SEALED')), 'sealed');
  assert.equal(memberSealState(member('a', 'unknown', 'UNINITIALIZED')), 'uninitialized');
  assert.equal(memberSealState(member('a', 'unknown', 'PENDING')), 'pending');
  assert.equal(memberSealState(member('a', 'unknown', 'LEAVING_BLOCKED')), 'leaving');
  assert.equal(memberSealState(member('a', 'unknown', 'UNKNOWN')), 'unknown');
  const overview = sealOverview({
    members: [member('baom0', 'primary', 'RUNNING'), member('baom1', 'unknown', 'SEALED', false), member('baom2', 'secondary', 'RUNNING')],
    conditions: [
      { type: 'Sealed', status: 'True', reason: 'MemberSealed', message: 'baom1 is sealed', lastTransitionTime: '' },
      { type: 'QuorumBlocked', status: 'False', reason: 'Healthy', message: '', lastTransitionTime: '' },
      { type: 'RaftVoter/baom0', status: 'True', reason: 'HealthyVoter', message: 'voter', lastTransitionTime: '' },
      { type: 'RaftVoter/baom1', status: 'Unknown', reason: 'ObservationUnavailable', message: 'sealed', lastTransitionTime: '' },
    ],
  });
  assert.equal(overview.active, 'baom0');
  assert.deepEqual(overview.sealed, ['baom1']);
  assert.equal(overview.conditions.sealed?.message, 'baom1 is sealed');
  assert.equal(overview.conditions.quorumBlocked, undefined);
  assert.deepEqual(overview.members[0]?.voter, { status: 'True', reason: 'HealthyVoter', message: 'voter' });
  assert.equal(overview.members[1]?.voter?.reason, 'ObservationUnavailable');
  assert.equal(overview.members[2]?.voter, undefined);
});

test('reveal state holds keys only until hidden and the request never touches the query cache', async () => {
  const keys: UnsealKeys = { sealType: 'shamir', recoveryShares: false, threshold: 2, shares: ['c2hhcmUx', 'c2hhcmUy', 'c2hhcmUz'], initialRootToken: 's.root', initializedAt: '2026-10-21T00:00:00Z' };
  let state = revealReducer(INITIAL_REVEAL, { type: 'request' });
  assert.deepEqual(state, { keys: null, busy: true, error: null });
  state = revealReducer(state, { type: 'resolved', keys });
  assert.equal(state.keys, keys);
  assert.deepEqual(revealReducer(state, { type: 'hide' }), INITIAL_REVEAL);
  assert.deepEqual(revealReducer(state, { type: 'failed', error: 'denied' }), { keys: null, busy: false, error: 'denied' });
  assert.equal(revealReducer(state, { type: 'request' }).keys, null);

  const requests: Request[] = [];
  const client = createClient<paths>({ baseUrl: 'https://example.test/api/v1', fetch: async (request) => {
    requests.push(request);
    return new Response(JSON.stringify(keys), { headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' } });
  } });
  const cache = new QueryClient();
  const revealed = await revealUnsealKeys(client, 'prod', 'bao', 'bao');
  assert.deepEqual(revealed.data, keys);
  assert.equal(requests.length, 1);
  assert.equal(requests[0]!.method, 'POST');
  assert.equal(new URL(requests[0]!.url).pathname, '/api/v1/clusters/bao/openbao/unseal-keys/reveal');
  assert.equal(new URL(requests[0]!.url).searchParams.get('namespace'), 'prod');
  assert.deepEqual(await requests[0]!.json(), { confirm: 'bao' });
  assert.equal(cache.getQueryCache().getAll().length, 0);
  assert.equal(shareLabel(keys, 0), 'Unseal share 1');
  assert.equal(shareLabel({ recoveryShares: true }, 2), 'Recovery share 3');
  const exported = unsealKeysExport('bao', keys);
  assert.match(exported, /^# OpenBao cluster bao — unseal shares \(2 of 3 required\)/);
  assert.match(exported, /unseal-share-3: c2hhcmUz\ninitial-root-token: s\.root$/);
});

test('a pending reveal request can be aborted when its identity-bound panel unmounts', async () => {
  const controller = new AbortController();
  const started = Promise.withResolvers<void>();
  const response = Promise.withResolvers<Response>();
  const client = createClient<paths>({ baseUrl: 'https://example.test/api/v1', fetch: (request) => {
    request.signal.addEventListener('abort', () => response.reject(request.signal.reason), { once: true });
    started.resolve();
    return response.promise;
  } });
  const pending = revealUnsealKeys(client, 'prod', 'bao', 'bao', controller.signal);
  const rejected = assert.rejects(pending, { name: 'AbortError' });
  await started.promise;
  controller.abort();
  await rejected;
});

test('database connection drafts validate names, roles and TTLs and pick eligible targets', () => {
  const clusters = [
    { name: 'pg', namespace: 'default', spec: { provider: 'postgres', topology: { type: 'ha' } } },
    { name: 'mongo', namespace: 'default', spec: { provider: 'psmdb', topology: { type: 'replicaSet' } } },
    { name: 'shards', namespace: 'default', spec: { provider: 'psmdb', topology: { type: 'sharded' } } },
    { name: 'mq', namespace: 'default', spec: { provider: 'rabbitmq', topology: { type: 'cluster' } } },
    { name: 'search', namespace: 'default', spec: { provider: 'elasticsearch', topology: { type: 'cluster' } } },
    { name: 'other', namespace: 'dev', spec: { provider: 'postgres', topology: { type: 'ha' } } },
  ];
  assert.deepEqual(connectionTargets(clusters, 'default').map((c) => c.name), ['pg', 'mongo', 'mq']);
  assert.deepEqual(storageClusters(clusters, 'default').map((c) => c.name), ['pg']);
  const ok = buildConnectionWrite(' app ', 'pg', 'postgres', [{ ...emptyRole(), name: 'reader', database: 'ignored' }, { ...emptyRole(), name: 'writer', access: 'readwrite', defaultTtl: '600', maxTtl: '7200' }]);
  assert.deepEqual(ok.errors, []);
  assert.deepEqual(ok.body, { name: 'app', targetCluster: 'pg', roles: [{ name: 'reader', access: 'readonly', defaultTtlSeconds: 3600, maxTtlSeconds: 86400 }, { name: 'writer', access: 'readwrite', defaultTtlSeconds: 600, maxTtlSeconds: 7200 }] });
  assert.deepEqual(buildConnectionWrite('app', 'mq', 'rabbitmq', [{ ...emptyRole(), name: 'consumer', database: 'events' }]).body.roles, [{ name: 'consumer', access: 'readonly', database: 'events', defaultTtlSeconds: 3600, maxTtlSeconds: 86400 }]);
  const bad = buildConnectionWrite('App', '', 'postgres', [{ ...emptyRole(), name: 'x', defaultTtl: '7200', maxTtl: '60' }, { ...emptyRole(), name: 'x', defaultTtl: '1.5' }]);
  assert.ok(bad.errors.some((e) => /Connection name must start/.test(e)));
  assert.ok(bad.errors.some((e) => /Choose a target cluster/.test(e)));
  assert.ok(bad.errors.some((e) => /Role 1: maximum TTL must be at least/.test(e)));
  assert.ok(bad.errors.some((e) => /Role 2: role names must be unique/.test(e)));
  assert.ok(bad.errors.some((e) => /Role 2: default TTL must be a whole number/.test(e)));
  assert.ok(buildConnectionWrite('app', 'pg', 'postgres', []).errors.some((e) => /at least one role/.test(e)));
  assert.ok(buildConnectionWrite('app', 'pg', 'postgres', [{ ...emptyRole(), name: 'a'.repeat(32) }]).errors.some((e) => /Role 1: name must start/.test(e)));
  assert.equal(formatTtl(undefined, 3600), '1h');
  assert.equal(formatTtl(86400, 1), '1d');
  assert.equal(formatTtl(90, 1), '90s');
  assert.equal(formatTtl(600, 1), '10m');
});

test('restore shares are built from a pasted offline copy and validated as a complete threshold set', () => {
  assert.deepEqual(validateRestore({}), []);
  assert.equal(engines.openbao?.validateRestore, validateRestore);
  const shamir = buildRestoreShares({ ...emptyRestoreShares(), threshold: '2', shares: 'c2hhcmUx\n c2hhcmUy \n\nc2hhcmUz\n' });
  assert.deepEqual(shamir, { shares: ['c2hhcmUx', 'c2hhcmUy', 'c2hhcmUz'], threshold: 2, sealType: 'shamir' });
  assert.deepEqual(validateRestore({ openbaoUnsealShares: shamir }), []);
  const recovery = buildRestoreShares({ sealType: 'awskms', threshold: '1', shares: 'cmVjb3Zlcnk=' });
  assert.deepEqual(recovery, { shares: ['cmVjb3Zlcnk='], threshold: 1, sealType: 'awskms', recovery: true });
  assert.match(validateRestore({ openbaoUnsealShares: buildRestoreShares({ ...emptyRestoreShares(), threshold: '3', shares: 'c2hhcmUx' }) })[0]!, /needs at least that many shares; 1 pasted/);
  assert.match(validateRestore({ openbaoUnsealShares: buildRestoreShares({ ...emptyRestoreShares(), threshold: '', shares: 'c2hhcmUx' }) })[0]!, /whole number of at least 1/);
  assert.match(validateRestore({ openbaoUnsealShares: buildRestoreShares({ ...emptyRestoreShares(), threshold: '1', shares: '' }) })[0]!, /at least one unseal share/);
  assert.match(validateRestore({ openbaoUnsealShares: buildRestoreShares({ ...emptyRestoreShares(), threshold: '1', shares: 'not base64!' }) })[0]!, /base64 values exactly as revealed/);
  assert.match(validateRestore({ openbaoUnsealShares: { ...shamir, shares: [shamir.shares[0]!, shamir.shares[0]!, shamir.shares[0]!], threshold: 3 } })[0]!, /must be distinct/);
  const distinct = Array.from({ length: 11 }, (_, index) => Buffer.from(`share-${index}`).toString('base64'));
  assert.match(validateRestore({ openbaoUnsealShares: { ...shamir, shares: distinct, threshold: 3 } })[0]!, /at most 10 distinct/);
  assert.deepEqual(validateRestore({ openbaoUnsealShares: { ...shamir, shares: distinct.slice(0, 10), threshold: 3 } }), []);
});
