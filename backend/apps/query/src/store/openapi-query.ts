import { baseQueryWithAuth } from '@app/store/base-query';
import { createApi, fetchBaseQuery } from '@reduxjs/toolkit/query/react';

export const openApi = createApi({
    reducerPath: 'openapi',
    baseQuery: baseQueryWithAuth,
    tagTypes: ['HeapDumps'],
    endpoints: () => ({}),
});
