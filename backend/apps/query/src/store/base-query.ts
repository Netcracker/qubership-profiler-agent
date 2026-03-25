import { extractErrorMessageFromBeError } from '@app/common/errors/error-utils';
import { isInvalidTokenError } from '@app/common/guards/errors';
import { userLocale } from '@app/common/user-locale';
import { API_BASE_URL } from '@app/constants/app.constants';
import { uxNotificationHelper } from '@netcracker/ux-react';
import {
    type BaseQueryFn,
    type FetchArgs,
    type FetchBaseQueryError,
    createApi,
    fetchBaseQuery,
} from '@reduxjs/toolkit/query/react';

export function getAccessToken() {
    try {
        const token = sessionStorage.getItem('token');
        if (token) {
            const authData = JSON.parse(token);
            return authData.accessToken ?? authData.access_token;
        }
        return null;
    } catch (e) {
        return null;
    }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function getAuthTokenValue(idpAccessToken: any) {
    if (
        typeof idpAccessToken === 'object' &&
        idpAccessToken !== null &&
        ('tokenType' in idpAccessToken || 'token_type' in idpAccessToken)
    ) {
        return `${idpAccessToken.tokenType ?? idpAccessToken.token_type} ${
            idpAccessToken.accessToken ?? idpAccessToken.access_token
        }`;
    }
    return '';
}

export function getAuthHeader(): string | null {
    try {
        const token = sessionStorage.getItem('token');
        if (token) {
            const authData = JSON.parse(token);
            return getAuthTokenValue(authData);
        }
        return null;
    } catch (e) {
        return null;
    }
}

export function getTenantId() {
    return sessionStorage.getItem('tenantId');
}

const baseQuery = fetchBaseQuery({
    baseUrl: API_BASE_URL,
    // That's like an axios interceptor to provide auth header for each request
    prepareHeaders(headers) {
        const authHeader = getAuthHeader();
        if (authHeader) {
            headers.set('Authorization', authHeader);
        }
        return headers;
    },
});

export const baseQueryWithAuth: BaseQueryFn<string | FetchArgs, unknown, FetchBaseQueryError> = async (
    args,
    api,
    extraOptions
) => {
    const baseApiResult = await baseQuery(args, api, extraOptions);
    if (baseApiResult.error) {
        if (isInvalidTokenError(baseApiResult.error)) {
            uxNotificationHelper.error({
                title: 'Invalid Token',
                description: 'You need to login again.',
                key: 'invalid_token',
            });
            // Here should be your logout logic
            // clearSessionToken();
            // window.location.replace(window.location.toString());
        }
        const beErrorMessage = extractErrorMessageFromBeError(baseApiResult.error);
        uxNotificationHelper.error({
            title: 'API Error',
            description: beErrorMessage?.message ?? 'Unknown Error',
            time: new Date().toLocaleString(userLocale),
            key: 'unknown_error',
        });
    }
    return baseApiResult;
};

export const baseApi = createApi({
    baseQuery: baseQueryWithAuth,
    endpoints: () => ({}),
    tagTypes: ['Todos'],
});
