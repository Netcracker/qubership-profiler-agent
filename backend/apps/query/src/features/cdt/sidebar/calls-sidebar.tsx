import { type FC } from 'react';
import { ReactComponent as FlowTree20Icon } from '@netcracker/ux-assets/icons/flow-tree/flow-tree-20.svg';
import { ReactComponent as ChevronCollapse20Icon } from '@netcracker/ux-assets/icons/chevron-collapse/chevron-collapse-20.svg';
import classNames from './calls-sidebar.module.scss';
import { UxButton, UxIcon, UxMenu, UxTooltip } from '@netcracker/ux-react';
import { useAppDispatch, useAppSelector } from '@app/store/hooks';
import { appDataActions, selectSiderCollapsed } from '@app/store/slices/app-state.slice';
import clsx from 'clsx';
import NamespacesTree from '@app/features/cdt/sidebar/namespaces-tree';

const CallsSideBar: FC = () => {
    const collapsed = useAppSelector(selectSiderCollapsed);
    const dispatch = useAppDispatch();
    return (
        <aside className={clsx(classNames.sidebar, collapsed && classNames.collapsed)}>
            <h3 className="ux-typography-13px-semibold">Namespaces</h3>
            {collapsed && (
                <UxMenu mode="vertical" selectedKeys={['tree']}>
                    <UxMenu.Item
                        key="tree"
                        onClick={() => dispatch(appDataActions.toggleSiderCollapsed())}
                        icon={
                            <UxTooltip title="Namespaces" placement="right">
                                <UxIcon component={FlowTree20Icon} />{' '}
                            </UxTooltip>
                        }
                    />
                </UxMenu>
            )}
            {!collapsed && <NamespacesTree />}
            <div className={classNames.footer}>
                <UxButton
                    onClick={() => dispatch(appDataActions.toggleSiderCollapsed())}
                    type="text"
                    color="blue"
                    leftIcon={<UxIcon component={ChevronCollapse20Icon} />}
                >
                    Collapse
                </UxButton>
            </div>
        </aside>
    );
};

export default CallsSideBar;
