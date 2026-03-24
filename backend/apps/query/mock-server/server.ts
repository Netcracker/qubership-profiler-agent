import { handlers } from '@mock-server/handlers';
import { setupServer } from 'msw/node';

/**
 * Use that server for unit tests
 */
export const server = setupServer(...handlers);
