import { ContentCard } from '@netcracker/cse-ui-components';
import cn from 'classnames';
import classNames from './calls-tree-dashboard-entity.module.scss';
import DefaultEntityActions from './calls-tree-dashboard-entity-default-actions';
import type { FC } from 'react';

const FrameGraphEntity: FC = () => {
    return (
        <ContentCard
            className={cn(classNames.entity, 'draggable-handle')}
            title="Frame Graph"
            titleClassName='ux-typography-13px-medium'
            extra={
                <div className="draggable-cancel">
                    <DefaultEntityActions i="frame-graph" />
                </div>
            }
        >
            <div className={cn(classNames.content, 'draggable-cancel')}>TODO</div>
        </ContentCard>
    );
};

export default FrameGraphEntity;
