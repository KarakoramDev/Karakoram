// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { Schemas } from '@/api/client';
import { claimSubmission, clearDisabledFlags, firstInvalidStep, nextStep, normalizeWizardSpec, resetEditedAttempt, type Validation } from './state.ts';
import { memberChoices, nameError, topologyErrors, validateWizard } from './validation.ts';
import { resourceErrors, resourceMinimums } from './resources.ts';
import { cleanTLS, coversAllAddresses, validateExposure, validateTLS, wireExposure } from './access.ts';
import { scheduleErrors } from './schedules.ts';
import { exposureSummary, totalFootprint } from './review.ts';
import { engineConsequences, engineRules } from './engine.ts';
import { validatePitrPoint } from './restore-point.ts';

const resources: Schemas['Resources'] = { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 1, maxRam: 2, minDisk: 5, maxDisk: 10 };
const spec: Schemas['ClusterSpec'] = { provider: 'psmdb', version: '8.0.32-14', topology: { type: 'replicaSet' }, components: { engine: { replicas: 3, resources } }, tls: { ca: 'selfSigned', mode: 'requireTLS' }, exposure: { public: false, ipType: 'IPV6', allowlist: [] } };
const topology: Schemas['ProviderTopology'] = { type: 'replicaSet', displayName: 'replicaSet', defaults: spec, componentSchemas: {}, limits: { engine: { allowedReplicas: [1, 3, 5, 7], minReplicas: 1, maxReplicas: 7 }, configServer: { allowedReplicas: [1, 3, 5, 7] }, router: { minReplicas: 1, maxReplicas: 32 } } };
const provider: Schemas['Provider'] = { name: 'psmdb', displayName: 'MongoDB', topologies: [topology], versions: [{ version: spec.version, major: '8.0', default: true, status: 'supported' }], presets: [], upgradePaths: [], backupTypes: ['logical', 'physical'] };
const empty: Validation = { basics: [], topology: [], resources: [], backups: [], access: [] };

test('Continue stays on every invalid step and advances only a valid step', () => {
  for (const step of ['basics', 'topology', 'resources', 'backups', 'access'] as const) {
    const invalid = { ...empty, [step]: ['Correct this field.'] };
    assert.equal(nextStep(step, invalid), step);
    assert.equal(firstInvalidStep(invalid), step);
  }
  assert.equal(nextStep('basics', empty), 'topology');
  assert.equal(nextStep('access', empty), 'review');
  assert.equal(nextStep('review', empty), 'review');
  assert.equal(firstInvalidStep(empty), undefined);
});

test('Create selects the first invalid step in creation and clone forms', () => {
  assert.equal(firstInvalidStep(validateWizard('Bad_Name', 'default', spec, [], provider)), 'basics');
  assert.equal(firstInvalidStep(validateWizard('orders', 'default', { ...spec, dataSource: { cluster: 'source' } }, [], provider)), 'backups');
  assert.equal(firstInvalidStep(validateWizard('orders', 'default', { ...spec, dataSource: { cluster: 'source', backup: 'nightly' } }, [], provider)), undefined);
});

test('cluster names enforce API pattern, length, missing values and collisions', () => {
  for (const name of ['', 'x', 'Badname', 'bad_name', '0orders', 'abcdefghijklmn', 'orders-1']) assert.ok(nameError(name, []));
  assert.equal(nameError('orders12', []), undefined);
  assert.ok(nameError('orders12', ['orders12']));
});

test('resource errors belong to the corresponding numeric fields', () => {
  const errors = resourceErrors({ ...resources, minRam: 0.25, minDisk: 1, minCpu: 0 });
  assert.match(errors.minRam!, /0.5 GiB/);
  assert.match(errors.minDisk!, /5 GiB/);
  assert.match(errors.minCpu!, /1 core/);
  assert.equal(firstInvalidStep(validateWizard('orders', 'default', { ...spec, components: { engine: { replicas: 3, resources: { ...resources, minRam: 0.25 } } } }, [], provider)), 'resources');
});

test('catalog minima override fallback rules per component, not preset sizes', () => {
  const catalog = { ...topology, componentSchemas: {
    engine: { properties: { resources: { properties: { minCpu: { minimum: 2 }, minRam: { minimum: 1.5 }, minDisk: { minimum: 8 } } } } },
    frontend: { properties: { resources: { properties: { minRam: { minimum: 1 } } } } },
  } };
  assert.deepEqual(resourceMinimums('psmdb', catalog, 'engine'), { cpu: 2, ram: 1.5, disk: 8 });
  assert.equal(resourceMinimums('psmdb', catalog, 'frontend').ram, 1);
  assert.equal(resourceMinimums('psmdb', topology, 'engine').ram, 0.5);
  assert.ok(resourceErrors(resources, undefined, false, resourceMinimums('psmdb', catalog, 'engine')).minRam);
});

test('CPU integrality, start bounds, finite numbers, ordering and disk growth are inline errors', () => {
  assert.ok(resourceErrors({ ...resources, maxCpu: 1.5 }).maxCpu);
  assert.ok(resourceErrors({ ...resources, startCpu: 3 }).startCpu);
  assert.ok(resourceErrors({ ...resources, minRam: NaN }).minRam);
  assert.ok(resourceErrors({ ...resources, minCpu: 3 }).maxCpu);
  assert.ok(resourceErrors({ ...resources, minRam: 3 }).maxRam);
  assert.ok(resourceErrors({ ...resources, minDisk: 11 }).maxDisk);
  assert.ok(resourceErrors(resources, { ...resources, minDisk: 8 }).minDisk);
  assert.equal(resourceErrors({ ...resources, minDisk: 1 }, undefined, true).minDisk, undefined);
});

test('config-server choices expose the full documented odd-member set', () => {
  assert.deepEqual(memberChoices(topology, 'configServer'), [1, 3, 5, 7]);
  const sharded = { ...spec, topology: { type: 'sharded' as const, shards: 1 }, components: { ...spec.components, configServer: { replicas: 7, resources }, router: { replicas: 1, resources } } };
  assert.equal(topologyErrors(sharded, topology).configServer, undefined);
  assert.ok(topologyErrors({ ...sharded, components: { ...sharded.components, configServer: { replicas: 2, resources } } }, topology).configServer);
  assert.ok(topologyErrors({ ...sharded, topology: { type: 'sharded', shards: 0 } }, topology).shards);
  assert.ok(topologyErrors({ ...sharded, components: { ...sharded.components, router: { replicas: 33, resources } } }, topology).router);
});

test('disabling a dependency clears checked flags without mutating the original', () => {
  const settings = { public: true, other: true, replicas: 0 };
  assert.deepEqual(clearDisabledFlags(settings, { public: false }), { public: false, other: true, replicas: 0 });
  assert.equal(settings.public, true);
  assert.equal(clearDisabledFlags(settings, { public: true }), settings);
  const backup = { storage: '', pitr: { enabled: true, oplogSpanMinutes: 10 } };
  assert.equal(normalizeWizardSpec({ ...spec, backup }).backup?.pitr?.enabled, false);
  assert.equal(backup.pitr.enabled, true);
});

test('retention zero is an explicit error instead of a replacement value', () => {
  const schedule: Schemas['BackupSchedule'] = { name: 'daily', cron: '0 2 * * *', type: 'logical', storage: 'store', retainCount: 0, enabled: true };
  assert.match(scheduleErrors(schedule, [schedule]).retainCount!, /at least 1/);
  assert.equal(schedule.retainCount, 0);
  assert.ok(scheduleErrors({ ...schedule, retainCount: 1.5 }, []).retainCount);
  assert.equal(scheduleErrors({ ...schedule, retainCount: 1 }, []).retainCount, undefined);
});

test('all-address CIDRs require explicit acknowledgement including noncanonical /0 ranges', () => {
  for (const entry of ['0.0.0.0/0', '1.2.3.4/0', '::/0', '2001:db8::/0']) {
    const exposure: Schemas['Exposure'] = { public: true, ipType: entry.includes(':') ? 'IPV6' : 'IPV4', allowlist: [entry] };
    assert.equal(coversAllAddresses(exposure), true);
    assert.match(validateExposure(exposure).join(' '), /Confirm/);
    assert.deepEqual(validateExposure(exposure, true), []);
  }
  assert.equal(coversAllAddresses({ public: false, ipType: 'IPV6', allowlist: ['::/0'] }), false);
});

test('exposure payload acknowledges only confirmed public all-address CIDRs', () => {
  for (const entry of ['0.0.0.0/0', '1.2.3.4/0', '::/0', '2001:db8::/0']) {
    const exposure: Schemas['Exposure'] = { public: true, ipType: entry.includes(':') ? 'IPV6' : 'IPV4', allowlist: [entry], allowAllAddresses: true };
    assert.equal(wireExposure(exposure, true).allowAllAddresses, true);
    assert.equal(wireExposure(exposure, false).allowAllAddresses, false);
    assert.equal(wireExposure({ ...exposure, public: false }, true).allowAllAddresses, false);
    assert.equal(exposure.allowAllAddresses, true);
  }
  for (const allowlist of [[], ['2001:db8::/32'], ['::::/0']]) {
    assert.equal(wireExposure({ public: true, ipType: 'IPV6', allowlist, allowAllAddresses: true }, true).allowAllAddresses, false);
  }
});

test('allowlist families must match and malformed CIDRs fail validation', () => {
  assert.match(validateExposure({ public: true, ipType: 'IPV6', allowlist: ['0.0.0.0/0'] }, true).join(' '), /IPv6/);
  assert.match(validateExposure({ public: true, ipType: 'IPV4', allowlist: ['2001:db8::/32'] }).join(' '), /IPv4/);
  for (const cidr of ['999.1.1.1/32', '1.2.3.4/33', '::/129', '::::/0', '2001:db8/32', '1.2.3.4', '::1/-1']) assert.match(validateExposure({ public: true, ipType: 'IPV6', allowlist: [cidr] }, true).join(' '), /Invalid CIDR/);
  assert.deepEqual(validateExposure({ public: true, ipType: 'IPV6', allowlist: ['2001:db8::/32', '::ffff:192.0.2.1/128'] }), []);
});

test('user-provided PKI requires CA signing pair and permits optional paired leaves', () => {
  const tls: Schemas['TLS'] = { mode: 'requireTLS', ca: 'userProvided', caCertificate: 'CA', caPrivateKey: 'KEY' };
  assert.deepEqual(validateTLS(tls), []);
  assert.match(validateTLS({ ...tls, caPrivateKey: '' }).join(' '), /signing private key/);
  assert.ok(validateTLS({ ...tls, certificate: 'LEAF' }).length);
  assert.ok(validateTLS({ ...tls, privateKey: 'LEAF KEY' }).length);
  assert.deepEqual(validateTLS({ ...tls, certificate: 'LEAF', privateKey: 'LEAF KEY' }), []);
  assert.deepEqual(validateTLS({ ca: 'userProvided', mode: 'requireTLS' }, true), []);
  assert.deepEqual(validateTLS({ ca: 'userProvided', mode: 'disabled' }), []);
});

test('Review totals include every shard, config server, router and data volume', () => {
  const sharded: Schemas['ClusterSpec'] = { ...spec, topology: { type: 'sharded', shards: 2 }, components: { engine: { replicas: 3, resources }, configServer: { replicas: 3, resources: { ...resources, minRam: 0.5, maxRam: 1 } }, router: { replicas: 2, resources: { ...resources, minRam: 0.5, maxRam: 0.5 } } } };
  assert.deepEqual(totalFootprint(sharded), { members: 11, volumes: 9, minCpu: 11, maxCpu: 22, minRam: 8.5, maxRam: 16, minDisk: 45, maxDisk: 90, runtimeMinDisk: 11, runtimeMaxDisk: 33 });
  assert.equal(totalFootprint(spec).volumes, 3);
  assert.equal(totalFootprint(spec).minRam, 3);
});

test('Review exposure and engine consequences reflect the enabled surface and recovery policy', () => {
  assert.match(exposureSummary(spec), /private/);
  const exposed = { ...spec, exposure: { public: true, ipType: 'IPV6' as const, allowlist: ['2001:db8::/32'] }, backup: { storage: 'store', pitr: { enabled: true, oplogSpanMinutes: 10 } } };
  assert.match(exposureSummary(exposed), /Public database.*IPV6.*2001:db8/);
  assert.match(engineConsequences(exposed).join(' '), /every database member/);
  assert.match(engineConsequences(exposed).join(' '), /completed base backup/);
  assert.match(engineRules('psmdb').topology.replicaSet!.title, /Replica set/);
  assert.match(engineRules('psmdb').topology.sharded!.description, /routers/);
});

test('Create synchronously locks duplicate clicks and unlocks for a failed-attempt retry', () => {
  const lock = { current: false };
  assert.equal(claimSubmission(lock), true);
  assert.equal(claimSubmission(lock), false);
  lock.current = false;
  assert.equal(claimSubmission(lock), true);
});

test('PITR selection cannot pass a pending, unavailable or out-of-window source', () => {
  const window = { available: true, start: '2026-10-01T00:00:00Z', end: '2026-10-02T00:00:00Z' };
  assert.ok(validatePitrPoint(window.start, null).length);
  assert.ok(validatePitrPoint(window.start, { available: false }).length);
  assert.ok(validatePitrPoint(undefined, window).length);
  assert.ok(validatePitrPoint('2099-01-01T00:00:00Z', window).length);
  assert.ok(validatePitrPoint('not-a-date', window).length);
  assert.deepEqual(validatePitrPoint(window.start, window), []);
  assert.deepEqual(validatePitrPoint(window.end, window), []);
});

test('blank optional TLS leaf fields are omitted instead of submitted as invalid PEM', () => {
  assert.deepEqual(cleanTLS({ mode: 'requireTLS', ca: 'userProvided', caCertificate: 'CA', caPrivateKey: 'KEY', certificate: '', privateKey: '  ' }), { mode: 'requireTLS', ca: 'userProvided', caCertificate: 'CA', caPrivateKey: 'KEY' });
  assert.deepEqual(cleanTLS({ mode: 'requireTLS', ca: 'selfSigned', caCertificate: 'CA', caPrivateKey: 'KEY' }), { mode: 'requireTLS', ca: 'selfSigned' });
});

test('editing a submitted form starts a new key attempt, but pending and unchanged retries do not', () => {
  const submitted = { current: false };
  assert.equal(resetEditedAttempt(submitted, false), false);
  submitted.current = true;
  assert.equal(resetEditedAttempt(submitted, true), false);
  assert.equal(submitted.current, true);
  assert.equal(resetEditedAttempt(submitted, false), true);
  assert.equal(submitted.current, false);
  assert.equal(resetEditedAttempt(submitted, false), false);
});
