import { createCallUrl } from '@app/features/cdt/calls/create-call-url';
import type { CallInfo } from '@app/store/cdt-openapi';

describe('createCallUrl', () => {
    it('should return a valid URL when passed a single CallInfo object', () => {
        const callInfo: CallInfo = {
            ts: 1695383812106,
            traceId: '1_2_dsa_3',
            pod: {
                pod: 'pod-1',
                startTime: 1695383812106,
            },
        };

        const result = createCallUrl(callInfo);

        expect(result).toMatchInlineSnapshot(
            `"/esc/tree.html#params-trim-size=15000&s=1695383812106&e=1695383812106&f%5B_0%5D=pod-1_1695383812106&i=0_1_2_dsa_3"`
        );
    });

    it('should return a valid URL when passed an array of CallInfo objects', () => {
        const callInfo1: CallInfo = {
            ts: 1695383812106,
            traceId: '1_2_dsa_3',
            pod: {
                pod: 'pod-1',
                startTime: 1695383812106,
            },
        };

        const callInfo2: CallInfo = {
            ts: 1695383769431,
            traceId: '0987654321',
            pod: {
                pod: 'pod-2',
                startTime: 1695383812189,
            },
        };

        const callInfos = [callInfo1, callInfo2];

        const result = createCallUrl(callInfos);

        expect(result).toMatchInlineSnapshot(
            `"/esc/tree.html#params-trim-size=15000&s=1695383769431&e=1695383812106&f%5B_0%5D=pod-1_1695383812106&f%5B_1%5D=pod-2_1695383812189&i=0_1_2_dsa_3&i=1_0987654321"`
        );
    });

    it('should handle CallInfo objects with missing optional properties', () => {
        const callInfo: CallInfo = {
            ts: 1695383769431,
            traceId: '1_2_dsa_3',
            pod: {
                pod: 'pod-1',
                startTime: 1695383812189,
            },
        };

        const result = createCallUrl(callInfo);

        expect(result).toMatchInlineSnapshot(
            `"/esc/tree.html#params-trim-size=15000&s=1695383769431&e=1695383769431&f%5B_0%5D=pod-1_1695383812189&i=0_1_2_dsa_3"`
        );
    });

    it('should return a valid URL when passed an empty array', () => {
        const callInfos: CallInfo[] = [];

        const result = createCallUrl(callInfos);

        expect(result).toMatchInlineSnapshot(`"/esc/tree.html#params-trim-size=15000&s=0&e=0"`);
    });
});
