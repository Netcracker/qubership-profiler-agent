import { setupWorker } from 'msw/browser';
import { handlers } from '../mocks/handlers';

// Start the MSW worker
const worker = setupWorker(...handlers);
worker.start();

// Load the tree.html content
const iframe = document.createElement('iframe');
iframe.src = '/tree.html';
iframe.width = '100%';
iframe.height = '800px';
document.body.appendChild(iframe);
