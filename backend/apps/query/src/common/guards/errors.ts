import type { BackendErrorPayload } from '@app/models/be-errors';
import type { FetchBaseQueryError } from '@reduxjs/toolkit/query';

export type TimeoutError = {
    /**
     * * `"TIMEOUT_ERROR"`:
     *   Request timed out
     **/
    status: 'TIMEOUT_ERROR';
    data?: undefined;
    error: string;
};

export type HttpError = {
    /**
     * * `number`:
     *   HTTP status code
     */
    status: number;
    data: unknown;
};

export type HttpErrorWithBeCorrectPaylod = {
    status: number;
    data: BackendErrorPayload;
};
export function isTimeoutError(error: unknown): error is TimeoutError {
    return !!error && typeof error === 'object' && 'status' in error && error.status === 'TIMEOUT_ERROR';
}

export function isErrorWithBackendErrorPayload(error: unknown): error is HttpErrorWithBeCorrectPaylod {
    if (
        !!error &&
        typeof error === 'object' &&
        'data' in error &&
        'status' in error &&
        typeof error.status === 'number'
    ) {
        return isBackendErrorPayload(error.data);
    }
    return false;
}

export function isBackendErrorPayload(data: unknown): data is BackendErrorPayload {
    return !!data && typeof data === 'object' && 'errorCode' in data && 'status' in data;
}

export function isInvalidTokenError(error: FetchBaseQueryError) {
    return error.status === 401 || (error.status === 'PARSING_ERROR' && error.originalStatus === 401);
}
