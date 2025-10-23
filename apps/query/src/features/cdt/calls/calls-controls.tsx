import { useCallsStore } from '@app/features/cdt/calls/calls-store';

import { useSearchParamsApplied } from "@app/store/slices/context-slices";
import useCallsFetchArg from "@app/features/cdt/calls/use-calls-fetch-arg";
import { useGetCallsByConditionQuery } from '@app/store/cdt-openapi';
import { createExportUrl, createCallUrl } from '@app/features/cdt/calls/create-call-url';

import { ReactComponent as Download20Icon } from '@netcracker/ux-assets/icons/download/download-20.svg';
import { ReactComponent as OpenIn20Icon } from '@netcracker/ux-assets/icons/open-in/open-in-20.svg';
import { UxButton, UxIcon, UxTooltip } from '@netcracker/ux-react';

const CallsControls = () => {
    const [selectedCalls, set] = useCallsStore(s => s.selectedCalls);
    const openCallsDisabled = !selectedCalls || selectedCalls?.length === 0;
    const searchParamsApplied = useSearchParamsApplied();
    const [callRequest, { shouldSkip, notReady }] = useCallsFetchArg();
    console.log(callRequest.filters.duration);


    const [graphCollapsed, setGraphCollapsed] = useCallsStore(s => s.graphCollapsed);

    // const [callRequest, { shouldSkip, notReady }] = useCallsFetchArg();
    // const { isFetching, data, isError, error, refetch } = useGetCallsByConditionQuery(callRequest, {
    //     skip: shouldSkip,
    // });
    // const showGraphDisabled = !data?.calls || data.calls.length == 0;

    // console.log("calls");
    // console.log(data?.calls?.length);
    // console.log(showGraphDisabled);

    const handleHideGraph = () => {
        setGraphCollapsed({graphCollapsed: !graphCollapsed})
    };

    return (
        <>

            <UxTooltip
                title={!openCallsDisabled ? `Show calls statistics as graph` : undefined}
                placement="bottomLeft"
            >
                <UxButton
                    onClick={handleHideGraph}
                    // disabled={showGraphDisabled}
                >
                    { graphCollapsed ? "Show Graph" : "Hide Graph" }
                </UxButton>
            </UxTooltip>

            <UxTooltip
                title={!openCallsDisabled ? `Download calls as CSV file ` + callRequest.filters.duration : undefined}
                placement="bottomLeft"
            >
                <UxButton
                    href={createExportUrl(callRequest)}
                    target="_blank"
                    type="light"
                    // disabled={!searchParamsApplied}
                    leftIcon={<UxIcon component={Download20Icon} />}
                />
            </UxTooltip>

            <UxTooltip
                title={!openCallsDisabled ? `Open selected items (${selectedCalls.length})` : undefined}
                placement="bottomLeft"
            >
                <UxButton
                    href={!openCallsDisabled ? createCallUrl(selectedCalls) : undefined}
                    disabled={openCallsDisabled}
                    type="light"
                    leftIcon={<UxIcon component={OpenIn20Icon} />}
                />
            </UxTooltip>
        </>
    );
};

export default CallsControls;
