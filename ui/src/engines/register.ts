// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Engine descriptors that carry React components live in `./<provider>/index.tsx`
// and are attached here, so `./index.ts` stays free of JSX and loadable by
// `node --test`. The app imports this module once before rendering.

import { engines } from './index';
import { rabbitmqUI } from './rabbitmq/index.ts';
import { elasticsearchUI } from './elasticsearch';

engines.rabbitmq = rabbitmqUI;

engines.elasticsearch = elasticsearchUI;
