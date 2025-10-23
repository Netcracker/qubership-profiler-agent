import { useCallsStore, useCallsStoreSelector } from '@app/features/cdt/calls/calls-store';
import { CALLS_COLUMNS_KEYS, useSortedCallsColumns } from '@app/features/cdt/calls/hooks/use-calls-columns';
import { type PropertiesItemModel, reorderItems } from '@netcracker/cse-ui-components';
import { PropertiesList } from '@netcracker/cse-ui-components/components/properties-list/properties-list';
import { ReactComponent as SettingsOutline20Icon } from '@netcracker/ux-assets/icons/settings/settings-outline-20.svg';
import { UxButton, UxIcon, UxPopover } from '@netcracker/ux-react';
import { type Key, type ReactNode, memo, useCallback } from 'react';

const ColumnsPopover = () => {
    const [columnsOrder, set] = useCallsStore(s => s.columnsOrder);
    const _columnsOrder = columnsOrder.length ? columnsOrder : CALLS_COLUMNS_KEYS;
    const hiddenColumns = useCallsStoreSelector(s => s.hiddenColumns);
    const sortedColumns = useSortedCallsColumns();

    const handleReorder = useCallback(
        (fromIndex: number, targetIndex: number) => {
            const newOrder = reorderItems(_columnsOrder, fromIndex, targetIndex);
            set({ columnsOrder: Array.from(newOrder) });
        },
        [_columnsOrder, set]
    );
    const handleToggle = useCallback(
        (item: PropertiesItemModel<unknown>) => {
            if (hiddenColumns.includes(item.value)) {
                set({ hiddenColumns: hiddenColumns.filter(c => c !== item.value) });
            } else {
                set({ hiddenColumns: [...hiddenColumns, item.value] });
            }
        },
        [hiddenColumns, set]
    );
    return (
        <UxPopover
            placement={'bottomRight'}
            title={<span>Properties</span>}
            arrow={false}
            overlayStyle={{ paddingTop: 0 }}
            content={
                <>
                    <PropertiesList
                        onReorder={handleReorder}
                        items={sortedColumns.map(col => ({
                            label: col.title as ReactNode,
                            value: col.key as Key,
                            data: col,
                            hidden: hiddenColumns.includes(col.key),
                        }))}
                        onToggle={handleToggle}
                    />
                </>
            }
        >
            <UxButton type="light" leftIcon={<UxIcon component={SettingsOutline20Icon} />} />
        </UxPopover>
    );
};

export default memo(ColumnsPopover);
