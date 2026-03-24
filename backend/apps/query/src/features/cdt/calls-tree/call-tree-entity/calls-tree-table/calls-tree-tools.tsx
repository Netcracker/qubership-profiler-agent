import { ESC_CALL_TREE_QUERY_PARAMS } from '@app/constants/query-params';
import { ReactComponent as ActionsIcon } from '@netcracker/ux-assets/icons/actions/actions-20.svg';
import { ReactComponent as BookmarkIcon } from '@netcracker/ux-assets/icons/bookmark/bookmark-outline-16.svg';
import { ReactComponent as DeleteIcon } from '@netcracker/ux-assets/icons/delete/delete-outline-16.svg';
import { UxButton, UxDropdownNew, UxIcon, UxInput, type UxDropdownNewItem } from '@netcracker/ux-react';
import { useState, type FC, useCallback } from 'react';
import { useSearchParams } from 'react-router-dom';
import classNames from '../content-controls.module.scss';
import ColumnsPopover from './columns-popover';
import { useDisableWidgetFunction } from '../../hooks/use-widgets';
import { useDebounceCallback } from '@react-hook/debounce';

const CallsTreeTableSearch: FC = () => {
    const [urlParams, setUrlParams] = useSearchParams();
    const [callsTreeQuery, setCallsTreeQuery] = useState(
        urlParams.get(ESC_CALL_TREE_QUERY_PARAMS.callsTreeQuery) || ''
    );
    const applyCallsTreeQuery = useDebounceCallback(() => {
        setUrlParams(params => {
            if (callsTreeQuery) {
                params.set(ESC_CALL_TREE_QUERY_PARAMS.callsTreeQuery, callsTreeQuery?.toString());
            } else {
                params.delete(ESC_CALL_TREE_QUERY_PARAMS.callsTreeQuery);
            }
            return params;
        });
    }, 500);

    const onChangeSearch = useCallback(
        (searchQuery: string) => {
            setCallsTreeQuery(searchQuery);
            applyCallsTreeQuery();
        },
        [applyCallsTreeQuery]
    );

    return (
        <UxInput.Search
            className={classNames.search}
            value={callsTreeQuery}
            placeholder="Search"
            size="small"
            outlined
            onChange={e => onChangeSearch(e.target.value)}
        />
    );
};

const CallTreeTableTools: FC = () => {
    const disableWidget = useDisableWidgetFunction();

    const DROPDOWN_ITEMS: UxDropdownNewItem[] = [
        {
            id: 'labelsManagement',
            text: 'Labels Managemenet',
            leftIcon: <UxIcon style={{ fontSize: 16 }} component={BookmarkIcon} />,
        },
        {
            id: 'remove',
            text: 'Remove',
            className: 'amarant-label',
            leftIcon: <UxIcon style={{ fontSize: 16 }} component={DeleteIcon} />,
        },
    ];

    function handleClick(item: UxDropdownNewItem) {
        switch (item.id) {
            case 'labelsManagement':
                console.log('Labels managemenet choosen');
                break;
            case 'remove':
                disableWidget('calls-tree');
                break;
        }
    }

    return (
        <div className={classNames.toolControls}>
            <CallsTreeTableSearch />
            <ColumnsPopover />
            <UxDropdownNew items={DROPDOWN_ITEMS} onItemClick={handleClick}>
                <UxButton type="light" size="medium" leftIcon={<UxIcon component={ActionsIcon} />} />
            </UxDropdownNew>
        </div>
    );
};

export default CallTreeTableTools;
