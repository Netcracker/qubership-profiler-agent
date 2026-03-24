export type BackendErrorPayload = {
    errorCode: number;
    stackTrace: string;
    status: string;
    time: string;
    userMessage?: string;
};
