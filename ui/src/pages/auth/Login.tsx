// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState, type FormEvent } from 'react';
import { Navigate, useLocation, useNavigate } from 'react-router-dom';
import { API_BASE, errorMessage } from '@/api/client';
import { useVersion } from '@/api/hooks';
import { useAuth } from '@/auth/session';
import { Button } from '@/components/ui/Button';
import { TextField } from '@/components/ui/Field';
import { Callout } from '@/components/ui/Primitives';
import { Logo } from '@/components/Logo';

function safeReturnTo(raw: string | null): string {
  if (!raw || !raw.startsWith('/') || raw.startsWith('//')) return '/clusters';
  return raw;
}

export function LoginPage() {
  const auth = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const version = useVersion();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const params = new URLSearchParams(location.search);
  const returnTo = safeReturnTo(params.get('returnTo'));
  const ssoError = params.get('error');

  if (auth.status === 'authenticated') {
    return <Navigate to={auth.user?.mustChangePassword ? '/change-password' : returnTo} replace />;
  }

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const user = await auth.login(username.trim(), password);
      navigate(user.mustChangePassword ? '/change-password' : returnTo, { replace: true });
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setBusy(false);
    }
  };

  const oidcEnabled = version.data?.oidcEnabled === true;

  return (
    <div className="auth-screen">
      <AuthArt />
      <section className="auth-panel">
        <form className="auth-form" onSubmit={onSubmit} aria-labelledby="login-title">
          <div className="row" style={{ gap: 10, marginBottom: 8 }}>
            <Logo size={32} />
            <div>
              <h1 id="login-title" style={{ fontSize: 'var(--fs-xl)' }}>
                Sign in to Karakoram
              </h1>
              <p className="small muted">Database orchestration for your Zerops project.</p>
            </div>
          </div>
          {ssoError ? <Callout tone="danger">Single sign-on failed: {ssoError}</Callout> : null}
          {error ? <Callout tone="danger">{error}</Callout> : null}
          <TextField label="Username" name="username" autoComplete="username" value={username} onChange={(e) => setUsername(e.target.value)} required autoFocus />
          <TextField label="Password" name="password" type="password" autoComplete="current-password" value={password} onChange={(e) => setPassword(e.target.value)} required />
          <Button type="submit" variant="primary" loading={busy} style={{ height: 38 }}>
            Sign in
          </Button>
          {oidcEnabled ? (
            <>
              <div className="row" style={{ gap: 10, color: 'var(--text-3)', fontSize: 'var(--fs-xs)' }}>
                <span className="divider grow" /> or <span className="divider grow" />
              </div>
              <a className="btn" style={{ height: 38 }} href={`${API_BASE}/auth/oidc/login?returnTo=${encodeURIComponent(returnTo)}`}>
                Sign in with SSO
              </a>
            </>
          ) : null}
          <p className="xs faint" style={{ marginTop: 12 }}>
            {version.data ? `Karakoram ${version.data.version} · API ${version.data.apiVersion}` : ''}
          </p>
        </form>
      </section>
    </div>
  );
}

function AuthArt() {
  return (
    <aside className="auth-art" aria-hidden>
      <svg viewBox="0 0 800 600" preserveAspectRatio="xMidYMax slice">
        <defs>
          <linearGradient id="sky" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0" stopColor="#0b1322" />
            <stop offset="1" stopColor="#182841" />
          </linearGradient>
          <linearGradient id="ridge1" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0" stopColor="#3b5584" />
            <stop offset="1" stopColor="#182841" />
          </linearGradient>
          <linearGradient id="ridge2" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0" stopColor="#223558" />
            <stop offset="1" stopColor="#0f1a2b" />
          </linearGradient>
        </defs>
        <rect width="800" height="600" fill="url(#sky)" />
        <circle cx="640" cy="120" r="46" fill="#7fb3ff" opacity="0.18" />
        <path d="M0 420 L110 250 L180 330 L260 180 L340 300 L420 230 L500 340 L600 200 L700 330 L800 260 L800 600 L0 600Z" fill="url(#ridge1)" />
        <path d="M260 180 L300 240 L260 232 L222 244Z" fill="#dfe8f7" opacity="0.9" />
        <path d="M600 200 L640 262 L600 252 L560 266Z" fill="#dfe8f7" opacity="0.7" />
        <path d="M0 500 L90 400 L170 460 L280 370 L370 450 L480 380 L560 470 L660 400 L760 470 L800 440 L800 600 L0 600Z" fill="url(#ridge2)" />
      </svg>
      <div className="auth-art-text">
        <h2>Replica sets, sharding, backups and restores — on your own Zerops project.</h2>
        <p>Karakoram runs inside your project and operates every member over the private network. Nothing leaves your account.</p>
      </div>
    </aside>
  );
}
