// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { api, csrf, unwrap, UNAUTHORIZED_EVENT, resetSessionRecovery, errorMessage, ApiError, type Schemas } from '@/api/client';
import { evaluate, type Action, type Resource } from './permissions';

type CurrentUser = Schemas['CurrentUser'];
type Permissions = Schemas['Permissions'];

interface AuthState {
  status: 'loading' | 'unavailable' | 'anonymous' | 'authenticated';
  user: CurrentUser | null;
  permissions: Permissions | null;
  unavailable?: string;
}

interface AuthApi extends AuthState {
  login: (username: string, password: string) => Promise<CurrentUser>;
  logout: () => Promise<void>;
  /** Clear a session already invalidated by a successful password change. */
  endSession: () => void;
  /** Re-read /auth/me and /permissions (after password change, policy edits). */
  refresh: () => Promise<void>;
  can: (resource: Resource, action: Action, object?: string) => boolean;
}

const AuthContext = createContext<AuthApi | null>(null);

async function loadIdentity(): Promise<AuthState> {
  try {
    const user = await unwrap(api.GET('/auth/me'));
    // Sessions established by the OIDC callback never pass through login(),
    // so the CSRF token is discovered here.
    if (user.csrfToken) csrf.set(user.csrfToken);
    let permissions: Permissions | null = null;
    try {
      permissions = await unwrap(api.GET('/permissions'));
    } catch {
      // A missing permissions read leaves the UI conservative: nothing is shown
      // as allowed beyond what the server lets us read.
      permissions = { subject: user.subject, groups: user.groups, permissions: [] };
    }
    return { status: 'authenticated', user, permissions };
  } catch (err) {
    if (err instanceof ApiError && err.status === 401) return { status: 'anonymous', user: null, permissions: null };
    return { status: 'unavailable', user: null, permissions: null, unavailable: errorMessage(err) };
  }
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<AuthState>({ status: 'loading', user: null, permissions: null });
  const queryClient = useQueryClient();

  const refresh = useCallback(async () => {
    const next = await loadIdentity();
    setState((previous) => next.status === 'unavailable' && previous.status === 'authenticated'
      ? { ...previous, unavailable: next.unavailable }
      : next);
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);


  useEffect(() => {
    if (!state.unavailable) return;
    const timer = window.setTimeout(() => { void refresh(); }, 5_000);
    return () => window.clearTimeout(timer);
  }, [state.unavailable, state, refresh]);
  const endSession = useCallback(() => {
    csrf.clear();
    queryClient.clear();
    setState({ status: 'anonymous', user: null, permissions: null });
  }, [queryClient]);

  useEffect(() => {
    window.addEventListener(UNAUTHORIZED_EVENT, endSession);
    return () => window.removeEventListener(UNAUTHORIZED_EVENT, endSession);
  }, [endSession]);

  const login = useCallback(
    async (username: string, password: string) => {
      const session = await unwrap(api.POST('/auth/login', { body: { username, password } }));
      csrf.set(session.csrfToken);
      resetSessionRecovery();
      queryClient.clear();
      await refresh();
      return session.account;
    },
    [queryClient, refresh],
  );

  const logout = useCallback(async () => {
    try {
      await unwrap(api.POST('/auth/logout'));
    } finally {
      endSession();
    }
  }, [endSession]);

  const can = useCallback(
    (resource: Resource, action: Action, object?: string) => {
      if (!state.permissions) return false;
      return evaluate(state.permissions.permissions, resource, action, object);
    },
    [state.permissions],
  );

  const value = useMemo<AuthApi>(() => ({ ...state, login, logout, endSession, refresh, can }), [state, login, logout, endSession, refresh, can]);
  return <AuthContext.Provider value={value}>
    {state.unavailable ? <div role="alert" style={{ padding: 16, textAlign: 'center' }}>
      <p>Karakoram is temporarily unavailable. Your session has been kept.</p>
      <p>{state.unavailable}</p>
      <button type="button" onClick={() => { void refresh(); }}>Retry now</button>
      <p>Retrying automatically…</p>
    </div> : null}
    {children}
  </AuthContext.Provider>;
}

export function useAuth(): AuthApi {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within AuthProvider');
  return ctx;
}
