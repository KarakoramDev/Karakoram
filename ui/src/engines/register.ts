// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Engine descriptors that carry React components live in `./<provider>/index.tsx`
// and are attached here, so `./index.ts` stays free of JSX and loadable by
// `node --test`. The app imports this module once before rendering.

import { engines } from './index';
import { rabbitmqUI } from './rabbitmq/index.ts';
import { elasticsearchUI } from './elasticsearch';
import { postgresUI } from './postgres';
import { supabaseUI } from './supabase';
import { keycloakUI } from './keycloak';
import { openbaoUI } from './openbao/index.ts';
import { temporalUI } from './temporal';

engines.rabbitmq = rabbitmqUI;

engines.elasticsearch = elasticsearchUI;
engines.postgres = postgresUI;
engines.supabase = supabaseUI;
engines.keycloak = keycloakUI;
engines.openbao = openbaoUI;
engines.temporal = temporalUI;
