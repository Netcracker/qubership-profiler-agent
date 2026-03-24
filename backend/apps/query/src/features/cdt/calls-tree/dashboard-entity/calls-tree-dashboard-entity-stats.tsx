import { ContentCard } from '@netcracker/cse-ui-components';
import cn from 'classnames';
import classNames from './calls-tree-dashboard-entity.module.scss';
import DefaultEntityActions from './calls-tree-dashboard-entity-default-actions';
import type { FC } from 'react';
import { useAppDispatch, useAppSelector } from '@app/store/hooks';
import { callsTreeContextDataAction, selectStatsState } from '@app/store/slices/calls-tree-context-slices';
import { ReactComponent as CancelIcon } from '@netcracker/ux-assets/icons/cancel/cancel-16.svg';
import CallStatsTable from '../call-tree-entity/call-stats-table/call-stats-table';
import StatsTable from '../call-tree-entity/common-stats-table/stats-table';
import { UxButton, UxIcon } from '@netcracker/ux-react';

const StatsEntity: FC = () => {
    const { selectedRowTitle } = useAppSelector(selectStatsState);

    const dispatch = useAppDispatch();

    const handleClose = () => {
        dispatch(callsTreeContextDataAction.unselectRow())
    };

    return (
        <ContentCard
            className={cn(classNames.entity, 'draggable-handle', 'card-with-table')}
            title={selectedRowTitle ? selectedRowTitle : 'Statistic'}
            titleClassName="ux-typography-13px-medium"
            extra={
                <div className="draggable-cancel">
                    {selectedRowTitle ? (
                        <UxButton onClick={handleClose} type="light">
                            {<UxIcon style={{ fontSize: 18 }} component={CancelIcon} />}
                        </UxButton>
                    ) : (
                        <DefaultEntityActions i="stats" />
                    )}
                </div>
            }
        >
            <div className={cn(classNames.content, 'draggable-cancel')}>
                {selectedRowTitle ? <CallStatsTable /> : <StatsTable />}
            </div>
        </ContentCard>
    );
};

export default StatsEntity;
