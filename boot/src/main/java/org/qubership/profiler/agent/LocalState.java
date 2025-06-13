package org.qubership.profiler.agent;

public class LocalState {
    private static final ESCLogger logger = ESCLogger.getLogger(LocalState.class.getName());

    // This is required to ensure JMX is initialized before usage of LocalState
    private static final ThreadJMXProvider THREAD_INFO_PROVIDER = ThreadJMXProviderFactory.INSTANCE;

    public final Thread thread = Thread.currentThread();
    public final int dumperIncarnation = ProfilerData.dumperIncarnation;
    public LocalBuffer buffer;
    // Contains a thread-local amount of heap consumed by the current thread to reduce global contention on
    // ProfilerData.reserveLargeEventVolume
    private long largeEventsVolume;
    public int sp;
    public CallInfo callInfo = new CallInfo(this);
    public final String shortThreadName = getShortThreadName(thread.getName());

    /**
     * Contains stack trace of current thread (method ids) paired with time when the method started
     * Basically it is methodId | TimerCache.timerSHL32
     */
    public long[] stackTrace = ProfilerData.MINIMAL_LOGGED_DURATION == 0 ? null : new long[ProfilerData.INITIAL_STACK_LENGTH];
    /**
     * Points to the next item in stackTrace array that should be flushed.
     * There is nothing to flush when flushedSp==sp
     */
    private int flushedSp;

    public boolean isSystem;

    public long cpuTime;
    public long waitTime;
    public long memoryUsed;
    public long fileRead, fileWritten;
    public long netRead, netWritten;
    public long transactions;

    public int nextCpuStamp = TimerCache.timer + ProfilerData.THREAD_CPU_MINIMAL_CALL_DURATION;
    public int nextWaitStamp = TimerCache.timer + ProfilerData.THREAD_WAIT_MINIMAL_CALL_DURATION;
    public int nextMemoryStamp = TimerCache.timer + ProfilerData.THREAD_MEMORY_MINIMAL_CALL_DURATION;

    //This is block a self call of adding information to LocalBuffer when we use OutputStreams. If this variable is true, profiler SHOULD NOT USE his own profiling methods
    private boolean isTriggeredAddingInformationToLocalBuffer = false;

    /*
     * Used to store additional information in Dumper to avoid Thread->State map
     */
    public Object additional;

    public void enter(int methodId) {
        long methodAndTime = methodId | TimerCache.timerSHL32;
        enter(methodAndTime);
    }

    //in case of reactive calls we need to report how much time passed post-factum
    public void enter(int methodId, long millisecondsPassed) {
        long methodAndTime;
        long startTimeToReportSHL32;
        if(millisecondsPassed > 0) {
            callInfo.additionalReportedTime += (int)millisecondsPassed;
            startTimeToReportSHL32 = (TimerCache.timer - millisecondsPassed) << 32;
        } else {
            startTimeToReportSHL32 = TimerCache.timerSHL32;
        }
        methodAndTime = methodId | startTimeToReportSHL32;
        enter(methodAndTime);
    }

    public void enter(long methodAndTime) {
        if (ProfilerData.MINIMAL_LOGGED_DURATION == 0)
            logEnterImmediately(methodAndTime);
        else
            logEnterLazy(methodAndTime);
    }

    public void exit() {
        if (ProfilerData.MINIMAL_LOGGED_DURATION == 0) {
            logExitImmediately();
        } else {
            logExitLazy();
        }
    }

    public void reactorExit() {
        if (ProfilerData.MINIMAL_LOGGED_DURATION == 0)
            logExitImmediately();
        else
            logExitLazy();
    }

    public void event(Object value, int id) {
        if (ProfilerData.MINIMAL_LOGGED_DURATION == 0)
            logEventImmediately(value, id);
        else
            logEventLazy(value, id);
    }

    /**
     * Attempts to reserve a specified volume for large event data collection.
     * If the total thread-local volume exceeds a predefined limit, it tries to
     * borrow from a global counter to accommodate the requested volume.
     *
     * @param volume the amount of volume to reserve for large event data
     * @return true if the reservation succeeds (either using thread-local space
     *         or borrowing from the global counter), false otherwise
     */
    public boolean reserveLargeEventVolume(long volume) {
        long largeEventsVolume = this.largeEventsVolume + volume;
        if (largeEventsVolume < ProfilerData.LARGE_EVENT_TLAB_BYTES) {
            // We allow a certain amount of heap used by each thread to reduce contention on the global counter
            this.largeEventsVolume = largeEventsVolume;
            return true;
        }
        // Thread-local counter exceeded its limit, try to borrow from the global one
        if (ProfilerData.reserveLargeEventVolume(largeEventsVolume)) {
            // Borrow succeeds, we can continue collecting more data in the current thread
            this.largeEventsVolume = 0;
            return true;
        }
        // Borrow fails, so we collected too much data. Subtract and try again later
        return false;
    }

    /*
     * The following methods are used when org.qubership.profiler.Profiler.minimal_logged_duration
     * is set to 0 (default is 1).
     * Basically that means, profiler should log each and every method invocation
     */

    private void logEnterImmediately(int methodId) {
        sp++;
        buffer.initEnter(methodId);
    }

    private void logEnterImmediately(long methodAndTime) {
        if(isTriggeredAddingInformationToLocalBuffer){
            return;
        } else {
            isTriggeredAddingInformationToLocalBuffer = true;
        }

        try {
            sp++;
            buffer.initEnter(methodAndTime);
        } finally {
            isTriggeredAddingInformationToLocalBuffer = false;
        }

    }

    private void logExitImmediately() {
        if(isTriggeredAddingInformationToLocalBuffer){
            return;
        } else {
            isTriggeredAddingInformationToLocalBuffer = true;
        }

        try {
            int sp = this.sp;
            sp--;
            this.sp = sp;
            if (sp == 0) {
                callFinished();
                return;
            }
            if(sp < 0){
                throw new ProfilerAgentException("SP is below zero. this should never happen: " + sp);
            }
            buffer.initExit();
        } finally {
            isTriggeredAddingInformationToLocalBuffer = false;
        }
    }

    private void logEventImmediately(Object value, int id) {
        if(isTriggeredAddingInformationToLocalBuffer){
            return;
        } else {
            isTriggeredAddingInformationToLocalBuffer = true;
        }
        try {
            buffer.event(value, id);
        } finally {
            isTriggeredAddingInformationToLocalBuffer = false;
        }
    }

    /*
     * The following methods are used when org.qubership.profiler.Profiler.minimal_logged_duration
     * is greater than 0.
     * Basically that means, profiler should log only those invocations that exceed 0ms or contain some
     * captured arguments inside
     */

    private void logEnterLazy(int methodId) {
        long methodAndTime = methodId | TimerCache.timerSHL32;
        logEnterLazy(methodAndTime);
    }

    private void logEnterLazy(long methodAndTime) {
        if(isTriggeredAddingInformationToLocalBuffer){
            return;
        } else {
            isTriggeredAddingInformationToLocalBuffer = true;
        }

        try{
            int sp = this.sp;
            this.sp = sp + 1;
            long[] stackTrace = this.stackTrace;
            if (sp >=0 && sp < stackTrace.length) {
                stackTrace[sp] = methodAndTime;
            } else {
                growStackTrace(methodAndTime);
            }
        } finally {
            isTriggeredAddingInformationToLocalBuffer = false;
        }
    }

    private void growStackTrace(int methodId) {
        growStackTrace(methodId | TimerCache.timerSHL32);
    }

    private void growStackTrace(long methodAndTime) {
        long[] newStack = new long[stackTrace.length * 2];

        System.arraycopy(stackTrace, 0, newStack, 0, stackTrace.length);
        newStack[sp - 1] = methodAndTime;
        stackTrace = newStack;
    }

    private void logExitLazy() {
        if(isTriggeredAddingInformationToLocalBuffer){
            return;
        } else {
            isTriggeredAddingInformationToLocalBuffer = true;
        }

        try {
            // If we already logged paired enter call, just log exit
            int sp = this.sp;
            if (flushedSp == sp) {
                logExitFromStack();
                return;
            }

            // Just ignore if duration fits in minimal logged duration
            sp--;
            this.sp = sp;
            if (sp == 0) {
                callFinishedLazy();
                return;
            }
            if(sp < 0){
                throw new ProfilerAgentException("SP is below zero. this should never happen: " + sp);
            }
            int start = (int) (stackTrace[sp] >>> 32);
            int timer = TimerCache.timer;
            if ((timer < start + ProfilerData.MINIMAL_LOGGED_DURATION))
                return;

            // Duration did not fit, log call along with necessary enters
            logStackTrace();
            buffer.initTimedEnter(stackTrace[sp]);
            buffer.initExit();
        } finally {
            isTriggeredAddingInformationToLocalBuffer = false;
        }
    }

    private void logStackTrace() {
        for (; flushedSp < sp; flushedSp++) {
            buffer.initTimedEnter(stackTrace[flushedSp]);
        }
    }

    private void logExitFromStack() {
        int sp = this.sp;
        sp--;
        this.sp = sp;

        if (sp == 0) {
            callFinishedLazy();
            return;
        }
        if(sp < 0){
            throw new ProfilerAgentException("SP is below zero. this should never happen: " + sp);
        }

        flushedSp = sp;
        buffer.initExit();
    }

    private void logEventLazy(Object value, int id) {
        if(isTriggeredAddingInformationToLocalBuffer){
            return;
        } else {
            isTriggeredAddingInformationToLocalBuffer = true;
        }
        try {
            logStackTrace();
            buffer.event(value, id);
        } finally {
            isTriggeredAddingInformationToLocalBuffer = false;
        }
    }

    private void callFinishedLazy() {
        if (flushedSp == 0) { // if we never logged enter event
            int start = (int) (stackTrace[0] >>> 32);
            int timer = TimerCache.timer;
            if (timer < start + ProfilerData.MINIMAL_LOGGED_DURATION) {
                CallInfo prev = this.callInfo;
                prev.finishTime = timer;
                callInfo = new CallInfo(this);
                // There is no CallInfo chaining here since current callInfo never appeared in LocalBuffer
                // thus dumper has no reference to it to follow next links
                // Dumper does not traverse next link
                // prev.next = callInfo;
                createNewMass();
                return;
            }
            buffer.initTimedEnter(stackTrace[0]);
        }
        flushedSp = 0;
        callFinished();
        createNewMass();
    }

    private void createNewMass() {
        if (stackTrace.length > ProfilerData.MAX_STACK_LENGTH)
            stackTrace = new long[ProfilerData.INITIAL_STACK_LENGTH];
    }

    private void callFinished() {
        if (buffer.corrupted) {
            //callInfo.corrupted = true;
//            reset the buffer so it only contains corrupted callInfo after this method.
            buffer.init(null);
            buffer.corrupted = false;
            if(logger.isFineEnabled()) {
                logger.fine("ESCAGENTCORRUPTEDBUFFER: resetting corrupted state to false for active buffer of thread " + thread.getName());
            }
            additional = null;
            callInfo = null; //to mark callInfo as isFirstInThread
            callInfo = new CallInfo(this);
            return;
        }
        THREAD_INFO_PROVIDER.updateThreadCounters(this);
        CallInfo callInfo = this.callInfo;
        callInfo.cpuTime = cpuTime;
        callInfo.waitTime = waitTime;
        callInfo.memoryUsed = memoryUsed;
        callInfo.fileRead = fileRead;
        callInfo.fileWritten = fileWritten;
        callInfo.netRead = netRead;
        callInfo.netWritten = netWritten;
        callInfo.transactions = transactions;
        callInfo.finishTime = TimerCache.timer;
        buffer.event(callInfo, ProfilerData.PARAM_CALL_INFO);
        CallInfo prev = this.callInfo;
        this.callInfo = new CallInfo(this);
        prev.next = this.callInfo;
    }

    public void markSystem() {
        isSystem = true;
    }

    private static String getShortThreadName(String fullName) {
        int executeThread = fullName.indexOf("ExecuteThread: ");
        if (executeThread >= 0)
            fullName = fullName.substring(executeThread + 16);
        else if ((fullName.endsWith("_MisfireHandler") || fullName.endsWith("_ClusterManager"))
                && fullName.length() > 25) {
            fullName = fullName.substring(fullName.length() - 25);
        } else if (fullName.startsWith("QuartzScheduler")) {
            fullName = "Quartz" + fullName.substring("QuartzScheduler".length());
        } else if (fullName.startsWith("DataFlow")) {
            fullName = "DF" + fullName.substring("DataFlow".length());
        }
        return fullName;
    }

    @Override
    public String toString() {
        return "LocalState{" + System.identityHashCode(this)+ " " +
                "thread=" + thread +
                ", dumperIncarnation=" + dumperIncarnation +
                ", sp=" + sp +
                ", callInfo=" + callInfo +
                ", shortThreadName='" + shortThreadName + '\'' +
                ", flushedSp=" + flushedSp +
                ", isSystem=" + isSystem +
                ", cpuTime=" + cpuTime +
                ", waitTime=" + waitTime +
                ", memoryUsed=" + memoryUsed +
                ", fileRead=" + fileRead +
                ", fileWritten=" + fileWritten +
                ", netRead=" + netRead +
                ", netWritten=" + netWritten +
                ", transactions=" + transactions +
                ", nextCpuStamp=" + nextCpuStamp +
                ", nextWaitStamp=" + nextWaitStamp +
                ", nextMemoryStamp=" + nextMemoryStamp +
                ", additional=" + additional +
                ", stackTrace=" + StringUtils.arrayToString(new StringBuilder(), stackTrace) +
                ", buffer=" + buffer +
                '}';
    }
}
