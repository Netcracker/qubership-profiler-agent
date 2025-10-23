import { ContentCard } from '@netcracker/cse-ui-components';
import cn from 'classnames';
import classNames from './calls-tree-dashboard-entity.module.scss';
import type { FC } from 'react';
import CallTreeTableTools from '../call-tree-entity/calls-tree-table/calls-tree-tools';
import CallsTreeTable from '../call-tree-entity/calls-tree-table/calls-tree-table';

const CallsTreeEntity: FC = () => {
    return (
        <ContentCard
            className={cn(classNames.entity, 'draggable-handle', 'card-with-table')}
            title="Call Tree"
            titleClassName='ux-typography-13px-medium'
            extra={
                <div className="draggable-cancel">
                    <CallTreeTableTools />
                </div>
            }
        >
            <div className={cn(classNames.content, 'draggable-cancel')}>
                <CallsTreeTable />
            </div>
        </ContentCard>
    );
};

export default CallsTreeEntity;
