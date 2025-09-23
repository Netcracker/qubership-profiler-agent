package com.netcracker.profiler.agent;

public class OperationIdTracker {
    private final static ThreadLocal<String> OPERATION_ID = new InheritableThreadLocal<String>();

    /**
     * Returns ID of a high level operation (aka TransactionID, aka TID).
     * That is identifier for the whole business action.
     * @return returns ID of a high level operation, {@code null} when no operation is in progress
     */
    public static String getOperationId() {
        return OPERATION_ID.get();
    }

    /**
     * Assigns ID for a current business operation, or {@code null} when no operation is in progress.
     * The ID might be propagated to spawned threads and/or RPC calls.
     */
    public static void setOperationId(String operationId) {
        OPERATION_ID.set(operationId);

        if (operationId == null) {
            return;
        }
        final CallInfo callInfo = Profiler.getState().callInfo;
        if (operationId.equals(callInfo.getEndToEndId())) {
            return;
        }

        callInfo.setEndToEndId(operationId);
    }
}
