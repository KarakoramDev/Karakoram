// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { Navigate, Outlet, useLocation } from 'react-router-dom';
import { useAuth } from './session';
import { Loading } from '@/components/ui/Primitives';

export function ProtectedRoute({ allowPasswordChange = false }: { allowPasswordChange?: boolean }) {
  const auth = useAuth();
  const location = useLocation();

  if (auth.status === 'loading' || auth.status === 'unavailable') {
    return (
      <div style={{ minHeight: '100vh', display: 'grid', placeItems: 'center' }}>
        <Loading label={auth.status === 'unavailable' ? 'Waiting for the server…' : 'Checking session…'} />
      </div>
    );
  }
  if (auth.status === 'anonymous') {
    const returnTo = encodeURIComponent(location.pathname + location.search);
    return <Navigate to={`/login?returnTo=${returnTo}`} replace />;
  }
  if (auth.user?.mustChangePassword && !allowPasswordChange) {
    return <Navigate to="/change-password" replace />;
  }
  return <Outlet />;
}
