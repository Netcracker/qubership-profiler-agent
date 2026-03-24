import type { CallsTreeData } from '@app/features/cdt/calls-tree/calls-tree-context';
import { type DashboardEntity } from '@app/store/slices/calls-tree-context-slices';

export const staticVersionPlaceholder = '-- Placeholder for static version --';
export const staticCallsTreePlaceholder = '-- Placeholder for json calls-tree data --';
export const staticInitialLayoutPlaceholder = '-- Placeholder for initial layout --';

export function getStaticVersion(): string {
    return staticVersionPlaceholder;
}

export function getStaticCallsTreeData(): CallsTreeData {
    const jsonData = staticCallsTreePlaceholder;
    return {
        data: JSON.parse(jsonData),
        isFetching: false,
        isError: false,
    };
}

export function getStaticInitialLayout(): DashboardEntity[] {
    const jsonData = staticInitialLayoutPlaceholder;
    return JSON.parse(jsonData);
}
