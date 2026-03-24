import { callsTreeStub } from '@app/features/cdt/calls-tree/call-tree-entity/utils/__fixtures__/calls-tree-data.stub';
import { findTreeNode } from '@app/features/cdt/calls-tree/call-tree-entity/utils/calls-tree-operations';

describe('find by id', () => {
    //
    test.each([['1'], ['2'], ['3'], ['9'], ['0'], ['10']])('find by id=%s', id => {
        const result = findTreeNode(callsTreeStub.children, id);
        expect(result).toBeDefined();
        expect(result?.id).toBe(id);
    });

    test('should return undefined when our id does not exists', () => {
        const result = findTreeNode(callsTreeStub.children, '666');
        expect(result).toBeUndefined();
    });
});
