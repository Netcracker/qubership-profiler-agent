package org.qubership.profiler.agent;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class LocalBuffer {
    public final static int SIZE = Integer.getInteger(LocalBuffer.class.getName() + ".SIZE", 4096);
    private final static AtomicLongFieldUpdater<LocalBuffer> START_TIME_UPDATER = AtomicLongFieldUpdater.newUpdater(LocalBuffer.class, "startTime");
    volatile public LocalState state;
    public LocalBuffer prevBuffer;

    public final long[] data = new long[SIZE];
    public final Object[] value = new Object[SIZE];
    // volatile since we want atomic values as it can be updated by both Dumper and mutator threads
    public volatile long startTime;
    // volatile as it is updated by mutator (log...) and read by Dumper thread
    public volatile int count;
    // volatile as it might be updated by both Dumper (stealData) and mutator (buffer.reset()) threads
    public volatile int first;
    public boolean corrupted;
    // Contains the total amount of heap consumed by the large events stored in the buffer
    private long largeEventsVolume;

    public LocalBuffer() {
        init(null);
    }

    public void increaseStartTime(long value) {
        START_TIME_UPDATER.addAndGet(this, value);
    }

    public void init(LocalBuffer prevBuffer) {
        startTime = TimerCache.now;
        count = 0;
        first = 0;
        resetLargeEventsVolume();
        this.prevBuffer = prevBuffer;
    }

    public void event(Object contents,
                      int tagId) {
        int r = count;
        long[] data = this.data;
        if (r >= 0 && r < data.length) {
            data[r] = tagId | TimerCache.timerSHL32;
            if (contents instanceof CharSequence) {
                contents = truncateCharSequence(contents);
            }
            value[r] = contents;
            count = r + 1;
        } else {
            LocalState state = this.state;
            Profiler.exchangeBuffer(this);
            state.buffer.event(contents, tagId);
        }
    }

    private Object truncateCharSequence(Object contents) {
        CharSequence s = (CharSequence) contents;
        int length = s.length();
        if (length < ProfilerData.LARGE_EVENT_THRESHOLD) {
            return contents;
        }
        LocalState state = this.state;
        if (state.reserveLargeEventVolume(length)) {
            largeEventsVolume += length;
            return contents;
        }

        // We exceed the total amount of heap used by the events, truncate the value
        contents = s.subSequence(0, ProfilerData.TRUNCATED_EVENTS_THRESHOLD) + "... truncated from " + length + " chars";
        return contents;
    }

//    public LocalState getLocalState(){
//        if(this.state != null ){
//            return state;
//        }
//        if(!Thread.currentThread().isAlive()){
//            System.err.println("Thread reported that it is not alive. which is wierd. local state null will be returned. Thread: " + Thread.currentThread().getName());
//            return null;
//        }
//        System.out.println("Local state is null for thread  " + Thread.currentThread().getName());
//        return null;
//    }

    public void initEnter(int methodId) {
        initTimedEnter(methodId | TimerCache.timerSHL32);
    }

    public void initEnter(long methodAndTime) {
        initTimedEnter(methodAndTime);
    }

    public void initTimedEnter(long methodAndTime) {
        int r = count;
        long[] data = this.data;
        if (r >= 0 && r < data.length) {
            data[r] = methodAndTime;
            count = r + 1;
        } else {
            Profiler.exchangeBuffer(this, methodAndTime);
        }
    }

    public void initExit() {
        initTimedExit(TimerCache.timerSHL32);
    }

    public void initTimedExit(long time) {
        int r = count;
        long[] data = this.data;
        if (r >= 0 && r < data.length) {
            data[r] = time;
            count = r + 1;
        } else {
            Profiler.exchangeBuffer(this, time);
        }
    }

    public void reset() {
        if (this.first < this.count && this.first >= 0)
            Arrays.fill(this.value, this.first, this.count, null);
        this.first = 0;
        this.count = 0;
        this.prevBuffer = this; // Ensure Dumper will not think this is the first buffer in thread
        this.corrupted = false;
        resetLargeEventsVolume();
    }

    private void resetLargeEventsVolume() {
        long volume = largeEventsVolume;
        if (volume <= 0) {
            return;
        }
        // Release the memory to the global pool so other threads can borrow it
        largeEventsVolume = 0;
        ProfilerData.reserveLargeEventVolume(-volume);
    }

    @Override
    public String toString() {
        return "LocalBuffer{" + System.identityHashCode(this) + " " +
                "corrupted=" + corrupted +
                ", state=" + System.identityHashCode(state) +
                ", count=" + count +
                ", first=" + first +
                ", startTime=" + startTime +
                ", data=" + StringUtils.arrayToString(new StringBuilder(), Arrays.copyOf(data, 20)) +
                ", value=" + StringUtils.arrayToString(new StringBuilder(), Arrays.copyOf(value, 20)) +
                ", prevBuffer=" + System.identityHashCode(prevBuffer) +
                '}';
    }

    public void command(byte commandId, Object result) {
        count = -1;
        data[0] = commandId;
        value[0] = result;
    }
}
