// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import {
  forwardRef,
  useId,
  type InputHTMLAttributes,
  type ReactNode,
  type SelectHTMLAttributes,
  type TextareaHTMLAttributes,
} from 'react';

interface FieldShellProps {
  label: ReactNode;
  hint?: ReactNode;
  error?: ReactNode;
  required?: boolean;
  id: string;
  children: ReactNode;
  className?: string;
}

function FieldShell({ label, hint, error, required, id, children, className }: FieldShellProps) {
  return (
    <div className={['field', className ?? ''].join(' ')}>
      <label className="field-label" htmlFor={id}>
        {label}
        {required ? <span className="req" aria-hidden>*</span> : null}
      </label>
      {children}
      {error ? (
        <span className="field-error" id={`${id}-err`} role="alert">
          {error}
        </span>
      ) : hint ? (
        <span className="field-hint" id={`${id}-hint`}>
          {hint}
        </span>
      ) : null}
    </div>
  );
}

function describedBy(id: string, hint: unknown, error: unknown) {
  if (error) return `${id}-err`;
  if (hint) return `${id}-hint`;
  return undefined;
}

export interface TextFieldProps extends Omit<InputHTMLAttributes<HTMLInputElement>, 'id'> {
  label: ReactNode;
  hint?: ReactNode;
  error?: ReactNode;
  mono?: boolean;
  id?: string;
  className?: string;
}

export const TextField = forwardRef<HTMLInputElement, TextFieldProps>(function TextField(
  { label, hint, error, mono, id: idProp, className, required, ...rest },
  ref,
) {
  const auto = useId();
  const id = idProp ?? auto;
  return (
    <FieldShell label={label} hint={hint} error={error} required={required} id={id} className={className}>
      <input
        ref={ref}
        id={id}
        className={['input', mono ? 'input-mono' : ''].join(' ')}
        aria-invalid={error ? true : undefined}
        aria-describedby={describedBy(id, hint, error)}
        required={required}
        {...rest}
      />
    </FieldShell>
  );
});

export interface SelectFieldProps extends Omit<SelectHTMLAttributes<HTMLSelectElement>, 'id'> {
  label: ReactNode;
  hint?: ReactNode;
  error?: ReactNode;
  id?: string;
  className?: string;
}

export const SelectField = forwardRef<HTMLSelectElement, SelectFieldProps>(function SelectField(
  { label, hint, error, id: idProp, className, required, children, ...rest },
  ref,
) {
  const auto = useId();
  const id = idProp ?? auto;
  return (
    <FieldShell label={label} hint={hint} error={error} required={required} id={id} className={className}>
      <select
        ref={ref}
        id={id}
        className="select"
        aria-invalid={error ? true : undefined}
        aria-describedby={describedBy(id, hint, error)}
        required={required}
        {...rest}
      >
        {children}
      </select>
    </FieldShell>
  );
});

export interface TextAreaFieldProps extends Omit<TextareaHTMLAttributes<HTMLTextAreaElement>, 'id'> {
  label: ReactNode;
  hint?: ReactNode;
  error?: ReactNode;
  mono?: boolean;
  id?: string;
  className?: string;
}

export const TextAreaField = forwardRef<HTMLTextAreaElement, TextAreaFieldProps>(function TextAreaField(
  { label, hint, error, mono, id: idProp, className, required, ...rest },
  ref,
) {
  const auto = useId();
  const id = idProp ?? auto;
  return (
    <FieldShell label={label} hint={hint} error={error} required={required} id={id} className={className}>
      <textarea
        ref={ref}
        id={id}
        className={['textarea', mono ? 'input-mono' : ''].join(' ')}
        aria-invalid={error ? true : undefined}
        aria-describedby={describedBy(id, hint, error)}
        required={required}
        {...rest}
      />
    </FieldShell>
  );
});

export interface CheckboxProps extends Omit<InputHTMLAttributes<HTMLInputElement>, 'type'> {
  label: ReactNode;
  description?: ReactNode;
}

export function Checkbox({ label, description, ...rest }: CheckboxProps) {
  return (
    <label className="check">
      <input type="checkbox" {...rest} />
      <span>
        <span className="check-title">{label}</span>
        {description ? (
          <>
            <br />
            <span className="faint xs">{description}</span>
          </>
        ) : null}
      </span>
    </label>
  );
}

export interface SwitchProps extends Omit<InputHTMLAttributes<HTMLInputElement>, 'type'> {
  label: ReactNode;
}

export function Switch({ label, ...rest }: SwitchProps) {
  return (
    <label className="switch">
      <input type="checkbox" role="switch" {...rest} />
      <span className="switch-track" aria-hidden />
      <span>{label}</span>
    </label>
  );
}

export interface ChoiceOption<T extends string> {
  value: T;
  title: ReactNode;
  description?: ReactNode;
  disabled?: boolean;
}

export interface ChoiceGroupProps<T extends string> {
  name: string;
  legend: ReactNode;
  value: T;
  options: ChoiceOption<T>[];
  onChange: (v: T) => void;
  hint?: ReactNode;
}

export function ChoiceGroup<T extends string>({ name, legend, value, options, onChange, hint }: ChoiceGroupProps<T>) {
  return (
    <fieldset className="field" style={{ border: 0, padding: 0, margin: 0 }}>
      <legend className="field-label" style={{ padding: 0, marginBottom: 6 }}>
        {legend}
      </legend>
      <div className="choice-grid">
        {options.map((o) => (
          <label key={o.value} className="choice">
            <input
              type="radio"
              name={name}
              value={o.value}
              checked={value === o.value}
              disabled={o.disabled}
              onChange={() => onChange(o.value)}
            />
            <span className="choice-title">{o.title}</span>
            {o.description ? <span className="choice-desc">{o.description}</span> : null}
          </label>
        ))}
      </div>
      {hint ? <span className="field-hint">{hint}</span> : null}
    </fieldset>
  );
}
