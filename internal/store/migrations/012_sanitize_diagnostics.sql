-- SPDX-License-Identifier: Apache-2.0
-- Copyright 2026 The Karakoram Authors
-- Diagnostic data migration is implemented in sanitizeLegacyDiagnostics so it
-- can reuse the native diagnostic sanitizer and commit bounded batches. This
-- entry is recorded only after every batch completes successfully.
SELECT 1;
