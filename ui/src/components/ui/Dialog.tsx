// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useEffect, useId, useRef, useState, type FormEvent, type ReactNode } from 'react';
import { Button } from './Button';
import { TextField } from './Field';

export interface DialogProps {
  open: boolean;
  onClose: () => void;
  title: ReactNode;
  children: ReactNode;
  footer?: ReactNode;
  wide?: boolean;
  /** When set, the body is wrapped in a form and submit triggers this. */
  onSubmit?: (e: FormEvent<HTMLFormElement>) => void;
}

/**
 * Accessible modal built on the native <dialog> element: focus trapping,
 * Escape handling and inert background come from the platform.
 */
export function Dialog({ open, onClose, title, children, footer, wide, onSubmit }: DialogProps) {
  const ref = useRef<HTMLDialogElement>(null);
  const titleId = useId();

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    if (open && !el.open) el.showModal();
    if (!open && el.open) el.close();
  }, [open]);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const onCancel = (e: Event) => {
      e.preventDefault();
      onClose();
    };
    el.addEventListener('cancel', onCancel);
    return () => el.removeEventListener('cancel', onCancel);
  }, [onClose]);

  const inner = (
    <>
      <div className="dialog-head">
        <h3 id={titleId}>{title}</h3>
        <Button variant="ghost" size="sm" icon aria-label="Close" onClick={onClose}>
          <CloseIcon />
        </Button>
      </div>
      <div className="dialog-body">{children}</div>
      {footer ? <div className="dialog-foot">{footer}</div> : null}
    </>
  );

  return (
    <dialog
      ref={ref}
      className={['dialog', wide ? 'dialog-wide' : ''].join(' ')}
      aria-labelledby={titleId}
      onClick={(e) => {
        // Click on the backdrop (outside the content box) closes.
        if (e.target === ref.current) onClose();
      }}
    >
      {onSubmit ? (
        <form
          className="dialog-form"
          onSubmit={(e) => {
            e.preventDefault();
            onSubmit(e);
          }}
        >
          {inner}
        </form>
      ) : (
        <div className="dialog-form">{inner}</div>
      )}
    </dialog>
  );
}

function CloseIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" aria-hidden>
      <path d="M18 6 6 18M6 6l12 12" />
    </svg>
  );
}

export interface ConfirmDialogProps {
  open: boolean;
  onClose: () => void;
  onConfirm: () => unknown;
  title: ReactNode;
  children?: ReactNode;
  confirmLabel?: string;
  danger?: boolean;
  /** Require typing this exact string before confirming. */
  typeToConfirm?: string;
  busy?: boolean;
}

export function ConfirmDialog({
  open,
  onClose,
  onConfirm,
  title,
  children,
  confirmLabel = 'Confirm',
  danger,
  typeToConfirm,
  busy,
}: ConfirmDialogProps) {
  const [typed, setTyped] = useState('');
  useEffect(() => {
    if (!open) setTyped('');
  }, [open]);
  const ok = !typeToConfirm || typed === typeToConfirm;
  return (
    <Dialog
      open={open}
      onClose={onClose}
      title={title}
      onSubmit={() => {
        if (ok) void onConfirm();
      }}
      footer={
        <>
          <Button onClick={onClose} disabled={busy}>
            Cancel
          </Button>
          <Button type="submit" variant={danger ? 'danger' : 'primary'} disabled={!ok} loading={busy}>
            {confirmLabel}
          </Button>
        </>
      }
    >
      {children}
      {typeToConfirm ? (
        <TextField
          label={
            <>
              Type <code>{typeToConfirm}</code> to confirm
            </>
          }
          value={typed}
          onChange={(e) => setTyped(e.target.value)}
          autoComplete="off"
          mono
          autoFocus
        />
      ) : null}
    </Dialog>
  );
}
