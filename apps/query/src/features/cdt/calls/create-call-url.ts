import type {CallInfo, CallsLoadRequest} from '@app/store/cdt-openapi';

export function createExportUrl(callRequest: CallsLoadRequest): string {
    const params: Record<string, string> = {
        'timeFrom': "" + callRequest.filters.timeRange?.from,
        'timeTo': "" + callRequest.filters.timeRange?.to,
        'durationFrom': "" + callRequest.filters.duration?.from,
        'query': "" + callRequest.filters.query,
        'services': JSON.stringify(callRequest.filters.services),
    };
    const hashParams = new URLSearchParams(params).toString();
    // const url = `/cdt/v2/calls/export/csv?${hashParams}`;
    const url = `/cdt/v2/calls/export/excel?${hashParams}`;
    // console.log(url)
    return url;
}

export function createCallUrl(_callsInfo: CallInfo[] | CallInfo): string {
    const callsInfo = Array.isArray(_callsInfo) ? _callsInfo : [_callsInfo];
    const timestamps = callsInfo.map(it => it.ts);
    const minimalTs = callsInfo.length ? Math.min(...timestamps) : 0;
    const maximumTs = callsInfo.length ? Math.max(...timestamps) : 0;
    const maximumDuration = callsInfo.length ? Math.max(...callsInfo.map(it => it.duration ?? 0)) : 0;

    const params: Record<string, string> = {
        'params-trim-size': '15000',
        s: `${minimalTs}`,
        e: `${maximumTs + maximumDuration}`,
    };

    let iKey = '';
    for (let i = 0; i < callsInfo.length; i++) {
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        const callInfo = callsInfo[i]!;
        const podNumber = i;

        iKey += `&i=${podNumber}_${callInfo.traceId}`;

        const fKey = `f[_${podNumber}]`;
        params[fKey] = `${callInfo.pod.pod}_${callInfo.pod.startTime}`;
    }

    const hashParams = new URLSearchParams(params).toString();
    // const decodedParams = decodeURIComponent(hashParams);
    const url = `/esc/tree.html#${hashParams}${iKey}`;

    return url;
}
