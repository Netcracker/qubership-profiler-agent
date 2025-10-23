import { asyncConfirm } from '@app/components/confirm';
import { getDownloadDumpUrl, useDeleteDumpByIdMutation, type HeapDumpInfo } from '@app/store/cdt-openapi';
import { ReactComponent as DeleteOutline16Icon } from '@netcracker/ux-assets/icons/delete/delete-outline-16.svg';
import { ReactComponent as Download16Icon } from '@netcracker/ux-assets/icons/download/download-16.svg';
import { UxButton, UxIcon } from '@netcracker/ux-react';
import { memo, useCallback } from 'react';
import classNames from './heap-dumps-table.module.scss';

const iconStyle = { fontSize: 16 };
function TrowActions(heapInfo: HeapDumpInfo) {
    const { dumpId, pod } = heapInfo;
    const [deleteDump] = useDeleteDumpByIdMutation();
    const handleDeleteDump = useCallback(async () => {
        await asyncConfirm(
            {
                key: 'delete-dump',
                okButtonProps: {
                    color: 'red',
                },
                title: 'Delete Heap Dump?',
                header: 'delete-dump'
            },
            () =>
                deleteDump({
                    dumpId: dumpId,
                    dumpType: 'heap',
                    podId: pod,
                })
        );
    }, [deleteDump, dumpId, pod]);

    return (
        <div className="flex g-4">
            <UxButton
                className={classNames.pigeonButtons}
                href={getDownloadDumpUrl({ dumpId: heapInfo.dumpId, dumpType: 'heap', podId: heapInfo.pod })}
                target="_blank"
                type="light"
                size="small"
                leftIcon={<UxIcon style={iconStyle} component={Download16Icon} />}
            />
            <UxButton
                className={classNames.pigeonButtons}
                onClick={handleDeleteDump}
                type="light"
                size="small"
                leftIcon={<UxIcon style={iconStyle} component={DeleteOutline16Icon} />}
            />
        </div>
    );
}

export default memo(TrowActions);
