export const ESC_QUERY_PARAMS = {
    dateFrom: 'dateFrom',
    dateTo: 'dateTo',
    //checkedKeys: 'checkedKeys',
    halfCheckedKeys: 'halfCheckedKeys',
    callsQuery: 'callsQuery',
    callsDuration: 'callsDuration',
    podsQuery: 'podsQuery',
    services: 'services'
} as const;

export const ESC_CALL_TREE_QUERY_PARAMS = {
    podName: 'podName',
    ts: 'ts',
    callsTreeQuery: 'callsQuery'
} as const;
