import { http, HttpResponse } from 'msw';
import { mockTreeData } from './tree-data';

export const handlers = [
    http.get('/js/tree.js', () => {
        // Return a JavaScript that calls the treedata function
        return new HttpResponse(
            mockTreeData,
            { headers: { 'Content-Type': 'application/javascript' } }
        );
    }),

    http.get('/tree/:filename', () => {
        // Mock the tree download endpoint
        return new HttpResponse(
            mockTreeData,
            { headers: { 'Content-Type': 'application/javascript' } }
        );
    })
];
