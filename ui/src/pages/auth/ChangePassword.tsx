// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState, type FormEvent } from 'react';
import { useNavigate } from 'react-router-dom';
import { api, errorMessage, unwrap } from '@/api/client';
import { useAuth } from '@/auth/session';
import { Button } from '@/components/ui/Button';
import { TextField } from '@/components/ui/Field';
import { Callout } from '@/components/ui/Primitives';
import { Logo } from '@/components/Logo';

export function ChangePasswordPage() {
  const auth = useAuth();
  const forced = auth.user?.mustChangePassword === true;
  const navigate = useNavigate();
  const [current, setCurrent] = useState('');
  const [next, setNext] = useState('');
  const [confirm, setConfirm] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const mismatch = confirm.length > 0 && next !== confirm;
  const tooShort = next.length > 0 && next.length < 12;

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (mismatch || tooShort) return;
    setBusy(true);
    setError(null);
    try {
      await unwrap(api.POST('/auth/change-password', { body: { currentPassword: current, newPassword: next } }));
      // The server may invalidate the current session; re-read identity and
      // fall back to login when needed.
      await auth.refresh();
      navigate('/clusters', { replace: true });
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="auth-screen" style={{ gridTemplateColumns: 'minmax(0,1fr)' }}>
      <section className="auth-panel" style={{ alignItems: 'center', borderLeft: 0 }}>
        <form className="auth-form" onSubmit={onSubmit} aria-labelledby="cp-title">
          <div className="row" style={{ gap: 10, marginBottom: 8 }}>
            <Logo size={32} />
            <div>
              <h1 id="cp-title" style={{ fontSize: 'var(--fs-xl)' }}>
                {forced ? 'Set a new password' : 'Change password'}
              </h1>
              <p className="small muted">
                {forced ? `Welcome, ${auth.user?.name}. A password change is required before you continue.` : `Signed in as ${auth.user?.name}.`}
              </p>
            </div>
          </div>
          {error ? <Callout tone="danger">{error}</Callout> : null}
          <TextField label="Current password" type="password" autoComplete="current-password" value={current} onChange={(e) => setCurrent(e.target.value)} required autoFocus />
          <TextField
            label="New password"
            type="password"
            autoComplete="new-password"
            value={next}
            onChange={(e) => setNext(e.target.value)}
            required
            minLength={12}
            hint="At least 12 characters."
            error={tooShort ? 'Use at least 12 characters.' : undefined}
          />
          <TextField
            label="Confirm new password"
            type="password"
            autoComplete="new-password"
            value={confirm}
            onChange={(e) => setConfirm(e.target.value)}
            required
            error={mismatch ? 'Passwords do not match.' : undefined}
          />
          <div className="row" style={{ justifyContent: 'space-between' }}>
            {forced ? (
              <Button
                variant="ghost"
                onClick={async () => {
                  await auth.logout();
                  navigate('/login');
                }}
              >
                Sign out
              </Button>
            ) : (
              <Button variant="ghost" onClick={() => navigate(-1)}>
                Cancel
              </Button>
            )}
            <Button type="submit" variant="primary" loading={busy} disabled={mismatch || tooShort}>
              Update password
            </Button>
          </div>
        </form>
      </section>
    </div>
  );
}
