// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { Component, type ReactNode } from 'react';

export function AppErrorFallback() {
  return (
    <main role="alert">
      <h1>The page could not be displayed</h1>
      <p>Reload the page to try again. Check the existing operation before submitting an action again.</p>
      <button type="button" onClick={() => window.location.reload()}>Reload page</button>
    </main>
  );
}

export class AppErrorBoundary extends Component<{ children: ReactNode }, { failed: boolean }> {
  state = { failed: false };

  static getDerivedStateFromError() {
    return { failed: true };
  }

  render() {
    return this.state.failed ? <AppErrorFallback /> : this.props.children;
  }
}
