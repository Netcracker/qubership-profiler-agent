/* eslint-disable react/no-unescaped-entities */
import ColumnsPopover from '@app/features/cdt/calls/columns-popover';
import {useAppDispatch} from '@app/store/hooks';
import {contextDataAction} from '@app/store/slices/context-slices';
import {UxInput, UxRadio, UxTooltip} from '@netcracker/ux-react';
import {memo, useMemo} from 'react';
import classNames from './content-controls.module.scss';
import {ESC_QUERY_PARAMS} from "@app/constants/query-params";
import {useSearchParams} from "react-router-dom";

const ContentControls = () => {
    const dispatch = useAppDispatch();
    const [urlParams, setUrlParams] = useSearchParams();
    const query =  urlParams.get(ESC_QUERY_PARAMS.callsQuery) || '';
    const durationFrom = useMemo(() => {
        const duration = urlParams.get(ESC_QUERY_PARAMS.callsDuration);
        return duration ? +duration : 5000;
    }, [urlParams]);

    function onChangeSearch(e: React.ChangeEvent<HTMLInputElement>) {
        if (e) {
            const value = e.target.value;
            setUrlParams(params => {
                if (!value) {
                    params.delete(ESC_QUERY_PARAMS.callsQuery);
                } else {
                    params.set(ESC_QUERY_PARAMS.callsQuery, e.target.value.toString());
                }
                return params;
            });
        }
    }

    return (
        <div className={classNames.contentControls}>
            <UxRadio.Group
                size="small"
                value={durationFrom}
                onChange={e => {
                    dispatch(contextDataAction.toFirstPage());
                    setUrlParams(s => {
                        s.set(ESC_QUERY_PARAMS.callsDuration, e.target.value.toString());
                        return s;
                    });
                }
                }
            >
                <UxRadio.Button value={0}>All</UxRadio.Button>
                <UxRadio.Button value={10}>{'>10ms'}</UxRadio.Button>
                <UxRadio.Button value={100}>{'>100ms'}</UxRadio.Button>
                <UxRadio.Button value={3000}>{'>3sec'}</UxRadio.Button>
                <UxRadio.Button value={5000}>{'>5sec'}</UxRadio.Button>
            </UxRadio.Group>
            <UxTooltip
                title="Search tips"
                className={classNames.searchTips}
                overlayStyle={{minWidth: '500px'}}
                description={
                    <div className={classNames.codeExample} style={{padding: '5px', paddingLeft: '15px'}}>
                        <ul>
                            <li>
                                <code>+clust1 user5 admin</code> lists all <code>(user5 OR admin)</code> made to
                                {' '}<code>clust1</code> node
                            </li>
                            <li>
                                <code>'test page' -clust2</code> matching phrase{' '}<code>'test page'</code> except <code>clust2</code>
                            </li>
                            <li>
                                <code>+GET -health user5 administrator</code>
                                <br/>lists http <code>GET</code> requests of <code>(user5 or admin)</code>{' '} except <code>/health</code>
                            </li>
                            <li>
                                <code>+$http.method=GET -$web.url=health $user=user5 user=admin</code>
                                <br/> the same, but explicitly sets parameters for searching
                            </li>
                        </ul>
                    </div>
                }
            >
                <UxInput.Search
                    value={query}
                    onChange={e => {
                        dispatch(contextDataAction.setSearchParamsApplied(false))
                        onChangeSearch(e)
                    }}
                    size="small"
                    onPressEnter={() => dispatch(contextDataAction.setSearchParamsApplied(true))}
                    outlined
                    placeholder="Query"
                    hint="Use + for mandatory, - to exclude, or 'exact phrase' filtering"
                />
            </UxTooltip>
            <ColumnsPopover/>
        </div>
    );
};

export default memo(ContentControls);
