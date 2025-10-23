import { userLocale } from '@app/common/user-locale';
import { ESC_QUERY_PARAMS } from '@app/constants/query-params';
import { defaultRange, defaultSelectedRange, fastRanges } from '@app/features/cdt/controls/fast-ranges';
import { IsoDatePicker } from '@netcracker/cse-ui-components';
import { SummaryCard } from '@netcracker/cse-ui-components/components/summary-card/summary-card';
import { UxButton, UxRadio, UxTabs } from '@netcracker/ux-react';
import type { RadioChangeEvent } from 'antd';
import sub from 'date-fns/sub';
import { useCallback, useMemo, useState } from 'react';
import { useMatch, useNavigate, useSearchParams } from 'react-router-dom';
import classNames from './controls-card.module.scss';
import { contextDataAction, useSearchParamsApplied } from '@app/store/slices/context-slices';
import clsx from 'clsx';
import { useAppDispatch } from '@app/store/hooks';
import { useSidebarApiArgs } from '@app/features/cdt/hooks/use-sidebar-api-args';
import type { Moment } from 'moment';

const ControlsCard = () => {
    const dispatch = useAppDispatch();
    const searchParamsApplied = useSearchParamsApplied();
    const navigate = useNavigate();
    const [search, setSearchParams] = useSearchParams();
    const [selectedRange, setSelectedRange] = useState<string | undefined>(defaultSelectedRange.label);
    const match = useMatch('/:activeKey');
    const [selectedServices] = useSidebarApiArgs();

    const handleChangeFastRange = useCallback(
        (e: RadioChangeEvent) => {
            dispatch(contextDataAction.setSearchParamsApplied(false));
            setSelectedRange(e.target.value);
            const range = fastRanges.find(range => range.label === e.target.value);
            if (range) {
                setSearchParams(prev => {
                    prev.set(
                        ESC_QUERY_PARAMS.dateFrom,
                        sub(new Date(), { [range.unit]: range.value })
                            .getTime()
                            .toString()
                    );
                    prev.set(ESC_QUERY_PARAMS.dateTo, new Date().getTime().toString());
                    return prev;
                });
            }
        },
        [dispatch, setSearchParams]
    );

    const handleChangePicker = useCallback(
        (field: keyof typeof ESC_QUERY_PARAMS) => (value?: string) => {
            dispatch(contextDataAction.setSearchParamsApplied(false));
            setSearchParams(prev => {
                if (value) prev.set(field, `${new Date(value).getTime()}`);
                return prev;
            });
        },
        [dispatch, setSearchParams]
    );

    // TODO: get rid of
    const [from, to] = useMemo(() => {
        const dateFrom = search.get('dateFrom');
        const dateTo = search.get('dateTo');
        if (dateFrom && dateTo) {
            return [new Date(+dateFrom), new Date(+dateTo)];
        }
        return [new Date(+defaultRange.dateFrom), new Date(+defaultRange.dateTo)];
    }, [search]);

    const handleChangeTab = useCallback(
        (activeKey: string): void =>
            navigate({
                pathname: activeKey,
                search: search.toString(),
            }),
        [navigate, search]
    );
    const formatPickerValue = useCallback((v: Moment) => (v ? v.toDate().toLocaleString(userLocale, {
        hour12: false
    }) : ''), []);
    const disabledToDates = useCallback(
        (d: Moment) => (from?.getTime() ? d.toDate().getTime() < from?.getTime() : false),
        [from]
    );
    return (
        <SummaryCard
            title={<></>}
            className={classNames.card}
            content={
                <div className={classNames.controls} role="toolbar">
                    <IsoDatePicker
                        label="From"
                        time
                        value={from?.toISOString()}
                        onChange={handleChangePicker(ESC_QUERY_PARAMS.dateFrom)}
                        format={formatPickerValue}
                    />
                    <IsoDatePicker
                        label="To"
                        time
                        disabledDate={disabledToDates}
                        value={to?.toISOString()}
                        onChange={handleChangePicker(ESC_QUERY_PARAMS.dateTo)}
                        format={formatPickerValue}
                    />
                    <UxRadio.Group value={selectedRange} onChange={handleChangeFastRange}>
                        {fastRanges.map(range => (
                            <UxRadio.Button key={range.label} value={range.label}>
                                {range.label}
                            </UxRadio.Button>
                        ))}
                    </UxRadio.Group>
                    <UxButton
                        onClick={() => dispatch(contextDataAction.setSearchParamsApplied(true))}
                        size="large"
                        disabled={selectedServices.length === 0}
                        className={clsx(classNames.applyButton, { [classNames.pulse]: !searchParamsApplied })}
                    >
                        Apply
                    </UxButton>
                </div>
            }
            footer={
                <UxTabs activeKey={match?.params.activeKey} className="tabs-in-footer" onChange={handleChangeTab}>
                    <UxTabs.TabPane tab="Calls" key={'calls'} />
                    <UxTabs.TabPane tab="Pods Info" key={'pods-info'} />
                    <UxTabs.TabPane tab="Heap Dumps" key={'heap-dumps'} />
                </UxTabs>
            }
        ></SummaryCard>
    );
};

export default ControlsCard;
