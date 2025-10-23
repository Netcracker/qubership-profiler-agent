import { handlers } from '@mock-server/handlers';
import { setupWorker } from 'msw';

// This configures a Service Worker with the given request handlers.
/**
 * Use that server in local development
 */
export const worker = setupWorker(...handlers);
