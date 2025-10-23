import { type CallsTreeInfo } from '@app/store/cdt-openapi';
import { usePopupVisibleState } from '@netcracker/cse-ui-components';
import { ReactComponent as ParamsIconSvg } from '@netcracker/ux-assets/icons/grid-four/grid-four-outline-16.svg';
import { UxButton, UxIcon, UxPopupNew, UxTableNew, type UxTableNewRow } from '@netcracker/ux-react';
import { type FC } from 'react';
import { useCallsTreeData } from '../../../calls-tree-context';
import classNames from '../../content-controls.module.scss';
import { columnsFactory, type TableData } from '../../params-table/columns';
import { createParamsData } from '../../utils/calls-tree-operations';

interface ParamsButtonModel {
    row: UxTableNewRow<CallsTreeInfo>;
}

const ParamsButton: FC<ParamsButtonModel> = ({ row }) => {
    const [visible, close, open] = usePopupVisibleState();

    const { isFetching } = useCallsTreeData();

    return (
        <div className={classNames.toolControls}>
            <UxButton type="light" onClick={open}>
                {<UxIcon style={{ fontSize: 16, color: '#0068FF' }} component={ParamsIconSvg} />}
            </UxButton>
            <UxPopupNew
                visible={visible}
                header={row.original.info.title}
                size="large"
                footer={<UxButton onClick={close}>Close</UxButton>}
                content={
                    <UxTableNew<TableData>
                        columns={columnsFactory()}
                        data={createParamsData(row.original) as TableData[]}
                        className="ux-table"
                        treeData={true}
                        loading={isFetching}
                    />
                }
                onOk={close}
                onCancel={close}
                onClose={close}
            />
        </div>
    );
};

export default ParamsButton;
