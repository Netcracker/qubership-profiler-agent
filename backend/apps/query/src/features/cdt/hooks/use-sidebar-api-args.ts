import { useCheckKeys } from '../sidebar/use-checked-keys';
import { useNamespacesTreeData } from '@app/features/cdt/sidebar/use-namespaces-tree-data';
import { parseTreeKey } from '@app/features/cdt/sidebar/utils/sidebar-tree-utils';
import { type ServiceListItem } from '@app/store/cdt-openapi';
import { useMemo } from 'react';

export function useSidebarApiArgs() {
    const checkedKeys = useCheckKeys();
    const { data } = useNamespacesTreeData();

    const selectedServices = useMemo(() => {
        const _checkedKeys = Array.isArray(checkedKeys) ? checkedKeys : checkedKeys.checked;
        return _checkedKeys.reduce((acc, checkedKey) => {
            const [namespace, service] = parseTreeKey(checkedKey);
            // not namespace
            if (!service && namespace && !acc.find(item => item.namespace == namespace && item.service != undefined)) {
                const container = data.find(container => container.namespace === namespace);
                if (container) {
                    const services = container.services;
                    if (services && Array.isArray(services)) {
                        services.forEach(s => {
                            acc.push({
                                namespace: container.namespace,
                                service: s.name,
                            });
                        });
                    }
                }
            } else if (service && !acc.find(item => item.service == service)) {
                const container = data.find(container => container.services.find(it => it.name === service));
                if (container) {
                    acc.push({
                        namespace: container.namespace,
                        service: service,
                    });
                }
            }
            return acc;
        }, [] as ServiceListItem[]);
    }, [checkedKeys, data]);
    return [selectedServices] as const;
}
