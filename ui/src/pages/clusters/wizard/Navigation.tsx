// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Dispatch, SetStateAction } from 'react';
import { Button } from '@/components/ui/Button';
import { navigateStep, nextStep, STEPS, type StepId, type Validation } from './state';

interface Props {
  step: StepId;
  validation: Validation;
  setStep: Dispatch<SetStateAction<StepId>>;
  touchStep: (step: StepId) => void;
  pending: boolean;
  onCancel: () => void;
  onSubmit: (button: HTMLButtonElement) => void;
}

/** Keep a multi-click gesture on the step where it began, including Review. */
export function WizardNavigation({ step, validation, setStep, touchStep, pending, onCancel, onSubmit }: Props) {
  const index = STEPS.findIndex((item) => item.id === step);
  return (
    <div className="wizard-foot">
      <Button disabled={pending} onClick={(event) => {
        if (event.detail > 1) return;
        if (index === 0) onCancel();
        else {
          touchStep(step);
          setStep((current) => navigateStep(current, step, STEPS[index - 1]!.id, event.detail));
        }
      }}>{index === 0 ? 'Cancel' : 'Back'}</Button>
      {step === 'review' ? (
        <Button variant="primary" onClick={(event) => { if (event.detail <= 1) onSubmit(event.currentTarget); }} loading={pending}>
          Create cluster
        </Button>
      ) : (
        <Button variant="primary" onClick={(event) => {
          if (event.detail > 1) return;
          touchStep(step);
          setStep((current) => navigateStep(current, step, nextStep(step, validation), event.detail));
        }}>
          Continue
        </Button>
      )}
    </div>
  );
}
