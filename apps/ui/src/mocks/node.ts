import { setupServer } from 'msw/node';

import { handlers } from './handlers';

/** MSW server for vitest; tests layer per-case overrides with server.use(). */
export const server = setupServer(...handlers);
