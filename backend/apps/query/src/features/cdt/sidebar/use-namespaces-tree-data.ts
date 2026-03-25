import { filterTreeData, createTreeFromContainersDto } from '@app/features/cdt/sidebar/utils/sidebar-tree-utils';
import { useGetNamespacesQuery } from '@app/store/cdt-openapi';

export function useNamespacesTreeData(search = '') {
    return useGetNamespacesQuery(undefined, {
        selectFromResult: ({ data = [], ...rest }) => {
            const filteredData = filterTreeData(data, search);
            return {
                ...rest,
                data,
                treeData: createTreeFromContainersDto(filteredData),
                filteredData: filteredData,
                rootNodes: data?.map(it => it.namespace) ?? [],
            };
        },
    });
}
