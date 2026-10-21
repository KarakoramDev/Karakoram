// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import {
  forwardRef,
  useId,
  useState,
  type InputHTMLAttributes,
  type KeyboardEvent,
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

export interface TagListFieldProps {
  label: ReactNode;
  hint?: ReactNode;
  error?: ReactNode;
  value: string[];
  onChange: (value: string[]) => void;
  placeholder?: string;
  id?: string;
  disabled?: boolean;
}

/** Short string lists as removable tags; Enter, comma or blur commits the typed entry. */
export function TagListField({ label, hint, error, value, onChange, placeholder, id: idProp, disabled }: TagListFieldProps) {
  const auto = useId();
  const id = idProp ?? auto;
  const [pending, setPending] = useState('');
  const commit = () => {
    const entries = pending.split(',').map((s) => s.trim()).filter((s) => s && !value.includes(s));
    if (entries.length) onChange([...value, ...entries]);
    setPending('');
  };
  const onKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' || e.key === ',') {
      e.preventDefault();
      commit();
    } else if (e.key === 'Backspace' && !pending && value.length) {
      onChange(value.slice(0, -1));
    }
  };
  return (
    <FieldShell label={label} hint={hint} error={error} id={id}>
      <div className="taglist" aria-invalid={error ? true : undefined}>
        {value.map((item) => (
          <span key={item} className="taglist-item">
            {item}
            <button type="button" aria-label={`Remove ${item}`} disabled={disabled} onClick={() => onChange(value.filter((v) => v !== item))}>
              ×
            </button>
          </span>
        ))}
        <input
          id={id}
          value={pending}
          placeholder={value.length ? undefined : placeholder}
          disabled={disabled}
          aria-describedby={describedBy(id, hint, error)}
          autoComplete="off"
          spellCheck={false}
          onChange={(e) => setPending(e.target.value)}
          onKeyDown={onKeyDown}
          onBlur={commit}
        />
      </div>
    </FieldShell>
  );
}
