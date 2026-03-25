import { callsTreeStub } from './__fixtures__/calls-tree-data.stub';
import { getExpandedRowsBySearch } from './search-elements';

describe('get expanded rows', () => {
    test('for empty search', () => {
        const result = getExpandedRowsBySearch('')(callsTreeStub.children);
        expect(result).toEqual([]);
    });
    test('for search only root', () => {
        const result = getExpandedRowsBySearch('JobRunShell')(callsTreeStub.children);
        expect(result).toEqual([]);
    });
    test('for search in children', () => {
        const result = getExpandedRowsBySearch('netcracker')(callsTreeStub.children);
        expect(result.sort()).toEqual(['0', '1', '2'].sort());
    });
    test('for not case sensitive search', () => {
        const result = getExpandedRowsBySearch('scheduler')(callsTreeStub.children);
        expect(result.sort()).toEqual(['0', '1', '2', '3'].sort());
    });
    test('for search with regexp symbols', () => {
        const result = getExpandedRowsBySearch('Lock(String, JobStoreSupport$TransactionCallback)')(
            callsTreeStub.children
        );
        expect(result.sort()).toEqual(['0'].sort());
    });
    test('for search in numeric fields', () => {
        const result = getExpandedRowsBySearch('25.9')(callsTreeStub.children);
        expect(result.sort()).toEqual(['0', '1'].sort());
    });
    test('for unexist search', () => {
        const result = getExpandedRowsBySearch('Some unexist text')(callsTreeStub.children);
        expect(result.sort()).toEqual([].sort());
    });
});
