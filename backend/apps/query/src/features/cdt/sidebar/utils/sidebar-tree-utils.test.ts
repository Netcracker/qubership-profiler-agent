import { filterTreeData } from '@app/features/cdt/sidebar/utils/sidebar-tree-utils';
import type { Container } from '@app/store/cdt-openapi';
import type { LastAck } from '@app/store/cdt-openapi';

describe('filterTreeData', () => {

    type ServiceDto = {
        name: string;
        lastAck?: LastAck;
        activePods?: number;
    };

    it('should return valid a single Service search result', () => {
        const services: ServiceDto[] = [{ name: 'name1', lastAck: 1, activePods: 1 }];

        const data: Container[] = [{ namespace: 'namespace1', services }];

        const search = 'name1';

        const result = filterTreeData(data, search);

        expect(result).toMatchInlineSnapshot(`
            [
              {
                "namespace": "namespace1",
                "services": [
                  {
                    "activePods": 1,
                    "lastAck": 1,
                    "name": "name1",
                  },
                ],
              },
            ]
        `);
    });

    it('should return valid a single Service and any case search result', () => {
        const services: ServiceDto[] = [{ name: 'name1', lastAck: 1, activePods: 1 }];

        const data: Container[] = [{ namespace: 'namespace1', services }];

        const search = 'NAME1';

        const result = filterTreeData(data, search);

        expect(result).toMatchInlineSnapshot(`
            [
              {
                "namespace": "namespace1",
                "services": [
                  {
                    "activePods": 1,
                    "lastAck": 1,
                    "name": "name1",
                  },
                ],
              },
            ]
        `);
    });

    it('should return valid an array of Service search result', () => {
        const services: ServiceDto[] = [
            { name: 'name1', lastAck: 1, activePods: 1 },
            { name: 'name2', lastAck: 2, activePods: 2 },
            { name: 'name3', lastAck: 3, activePods: 3 },
        ];

        const data: Container[] = [{ namespace: 'namespace1', services }];

        const search = 'name1';

        const result = filterTreeData(data, search);

        expect(result).toMatchInlineSnapshot(`
            [
              {
                "namespace": "namespace1",
                "services": [
                  {
                    "activePods": 1,
                    "lastAck": 1,
                    "name": "name1",
                  },
                ],
              },
            ]
        `);
    });

    it('should return valid an array of Service and Containers search result', () => {
        const services: ServiceDto[] = [
            { name: 'name1', lastAck: 1, activePods: 1 },
            { name: 'name2', lastAck: 2, activePods: 2 },
            { name: 'name3', lastAck: 3, activePods: 3 },
        ];

        const data: Container[] = [
            { namespace: 'namespace1', services },
            { namespace: 'namespace2', services },
            { namespace: 'namespace3', services },
        ];

        const search = 'name1';

        const result = filterTreeData(data, search);

        expect(result).toMatchInlineSnapshot(`
            [
              {
                "namespace": "namespace1",
                "services": [
                  {
                    "activePods": 1,
                    "lastAck": 1,
                    "name": "name1",
                  },
                ],
              },
              {
                "namespace": "namespace2",
                "services": [
                  {
                    "activePods": 1,
                    "lastAck": 1,
                    "name": "name1",
                  },
                ],
              },
              {
                "namespace": "namespace3",
                "services": [
                  {
                    "activePods": 1,
                    "lastAck": 1,
                    "name": "name1",
                  },
                ],
              },
            ]
        `);
    });

    it('should return valid an empty Service search result', () => {
        const data: Container[] = [];

        const search = 'name1';

        const result = filterTreeData(data, search);

        expect(result).toMatchInlineSnapshot(`[]`);
    });
});
