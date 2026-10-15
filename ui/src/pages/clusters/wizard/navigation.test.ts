// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { register } from 'node:module';
import type { ButtonHTMLAttributes, MouseEvent, ReactElement, SetStateAction } from 'react';
import { STEPS, type StepId, type Validation } from './state.ts';

register('../../../../test/tsx-loader.mjs', import.meta.url);
// Static imports execute before registration and cannot load TSX in Node.
const { WizardNavigation } = await import('./Navigation.tsx');
const valid: Validation = { basics: [], topology: [], resources: [], backups: [], access: [] };

function wizard(initial: StepId, validation = valid) {
  let step = initial;
  let submitted = 0;
  let cancelled = 0;
  const touched: StepId[] = [];
  const setStep = (update: SetStateAction<StepId>) => { step = typeof update === 'function' ? update(step) : update; };
  return {
    get step() { return step; },
    get submitted() { return submitted; },
    get cancelled() { return cancelled; },
    touched,
    render() {
      const element = WizardNavigation({ step, validation, setStep, touchStep: (id) => touched.push(id), pending: false, onCancel: () => cancelled++, onSubmit: () => submitted++ });
      return element.props.children as ReactElement<ButtonHTMLAttributes<HTMLButtonElement>>[];
    },
  };
}

function click(button: ReactElement<ButtonHTMLAttributes<HTMLButtonElement>>, detail: number) {
  button.props.onClick!({ detail, currentTarget: {} } as MouseEvent<HTMLButtonElement>);
}

test('double-clicking Continue on Basics reaches only Topology, including after a render', () => {
  const form = wizard('basics');
  const firstContinue = form.render()[1]!;
  click(firstContinue, 1);
  assert.equal(form.step, 'topology');
  // Browsers report detail=2 for the second click even if React has rendered.
  click(form.render()[1]!, 2);
  assert.equal(form.step, 'topology');
  // An old handler cannot advance the newly rendered step either.
  click(firstContinue, 1);
  assert.equal(form.step, 'topology');
});

test('Continue and Back move only one step for every shared creation and clone path', () => {
  for (let index = 0; index < STEPS.length - 1; index++) {
    const form = wizard(STEPS[index]!.id);
    click(form.render()[1]!, 1);
    click(form.render()[1]!, 2);
    assert.equal(form.step, STEPS[index + 1]!.id);
    assert.equal(form.submitted, 0, 'the second Continue click must not submit Review');
  }
  for (let index = 1; index < STEPS.length; index++) {
    const form = wizard(STEPS[index]!.id);
    const back = form.render()[0]!;
    click(back, 1);
    click(form.render()[0]!, 2);
    click(back, 1);
    assert.equal(form.step, STEPS[index - 1]!.id);
    assert.equal(form.cancelled, 0, 'the second Back click must not activate Cancel');
  }
});

test('keyboard navigation works and invalid steps remain in place', () => {
  const form = wizard('basics');
  click(form.render()[1]!, 0);
  assert.equal(form.step, 'topology');
  click(form.render()[0]!, 0);
  assert.equal(form.step, 'basics');
  const invalid = wizard('topology', { ...valid, topology: ['Choose a valid member count.'] });
  click(invalid.render()[1]!, 1);
  assert.equal(invalid.step, 'topology');
  assert.deepEqual(invalid.touched, ['topology']);
});
