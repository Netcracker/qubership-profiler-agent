/* eslint-disable @typescript-eslint/ban-ts-comment */
// jest-dom adds custom jest matchers for asserting on DOM nodes.
import '@testing-library/jest-dom';
// allows you to do things like:
// expect(element).toHaveTextContent(/react/i)
// learn more: https://github.com/testing-library/jest-dom
import '@testing-library/jest-dom/extend-expect';
import crypto from 'crypto';
import { server } from '@mock-server/server';

Object.defineProperty(window, 'matchMedia', {
    value: () => {
        return {
            matches: false,
            addListener: jest.fn(),
            removeListener: jest.fn(),
        };
    },
});

window.ResizeObserverEntry = jest.fn();
// Element.prototype.scroll = jest.fn();
Element.prototype.scrollIntoView = jest.fn();

Object.defineProperty(global.self, 'crypto', {
    value: {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        getRandomValues: (arr: any) => crypto.randomFillSync(arr),
    },
});

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterAll(() => server.close());
afterEach(() => {
    server.resetHandlers();
    // This is the solution to clear RTK Query cache after each test
    // store.dispatch(baseApi.util.resetApiState());
});
