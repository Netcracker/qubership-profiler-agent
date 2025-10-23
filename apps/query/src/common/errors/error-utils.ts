import { isErrorWithBackendErrorPayload } from '@app/common/guards/errors';

export function extractErrorMessageFromBeError(error: unknown) {
    if (isErrorWithBackendErrorPayload(error)) {
        return {
            message: error.data.userMessage,
        };
    }
}
