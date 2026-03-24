import HtmlEllipsis from '@app/components/html-ellipsis/html-ellipsis';
import type { Container } from '@app/store/cdt-openapi';
import type { DataNode } from 'antd/lib/tree';
import type { Key } from 'react';

export const SERVICE_NAME_SEPARATOR = '|';

export const parseTreeKey = (key: Key): [string, string | undefined] => {
    return key.toString().split(SERVICE_NAME_SEPARATOR) as [string, string | undefined];
};
export const createTreeFromContainersDto = (items: Container[]): DataNode[] => {
    return items.map(container => {
        const node: DataNode = {
            key: container.namespace,
            checkable: true,
            title: <HtmlEllipsis text={container.namespace} />,
            children: container.services.map(service => ({
                key: `${container.namespace}${SERVICE_NAME_SEPARATOR}${service.name}`,
                title: <HtmlEllipsis text={service.name} />,
                checkable: true,
            })),
        };
        return node;
    });
};

export function filterTreeData(data: Container[], searchQuery: string): Container[] {
    return data.reduce((acc, container) => {
        const filteredChild = container.services?.filter(it =>
            it.name?.toLowerCase().includes(searchQuery.toLowerCase())
        );
        const match = container.namespace?.includes(searchQuery) || (filteredChild && filteredChild.length > 0);
        if (match) {
            acc.push({
                namespace: container.namespace,
                services: filteredChild,
            });
        }
        return acc;
    }, [] as Container[]);
}
