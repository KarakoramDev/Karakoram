// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { api, csrf, unwrap, UNAUTHORIZED_EVENT, type Schemas } from '@/api/client';
import { evaluate, type Action, type Resource } from './permissions';

type CurrentUser = Schemas['CurrentUser'];
type Permissions = Schemas['Permissions'];

interface AuthState {
  status: 'loading' | 'anonymous' | 'authenticated';
  user: CurrentUser | null;
  permissions: Permissions | null;
}

interface AuthApi extends AuthState {
  login: (username: string, password: string) => Promise<CurrentUser>;
  logout: () => Promise<void>;
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
  } catch {
    return { status: 'anonymous', user: null, permissions: null };
  }
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<AuthState>({ status: 'loading', user: null, permissions: null });
  const queryClient = useQueryClient();

  const refresh = useCallback(async () => {
    setState(await loadIdentity());
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  useEffect(() => {
    const onUnauthorized = () => {
      csrf.clear();
      queryClient.clear();
      setState({ status: 'anonymous', user: null, permissions: null });
    };
    window.addEventListener(UNAUTHORIZED_EVENT, onUnauthorized);
    return () => window.removeEventListener(UNAUTHORIZED_EVENT, onUnauthorized);
  }, [queryClient]);

  const login = useCallback(
    async (username: string, password: string) => {
      const session = await unwrap(api.POST('/auth/login', { body: { username, password } }));
      csrf.set(session.csrfToken);
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
      csrf.clear();
      queryClient.clear();
      setState({ status: 'anonymous', user: null, permissions: null });
    }
  }, [queryClient]);

  const can = useCallback(
    (resource: Resource, action: Action, object = '*') => {
      if (!state.permissions) return false;
      return evaluate(state.permissions.permissions, resource, action, object);
    },
    [state.permissions],
  );

  const value = useMemo<AuthApi>(() => ({ ...state, login, logout, refresh, can }), [state, login, logout, refresh, can]);
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthApi {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within AuthProvider');
  return ctx;
}
