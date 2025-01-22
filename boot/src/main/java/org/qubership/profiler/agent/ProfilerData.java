package org.qubership.profiler.agent;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class ProfilerData {
    private static final ESCLogger logger = ESCLogger.getLogger(ProfilerData.class.getName());

    public static final int INITIAL_BUFFERS = Integer.getInteger(Profiler.class.getName() + ".INITIAL_BUFFERS", 200);
    public static final int MIN_BUFFERS = Integer.getInteger(Profiler.class.getName() + ".MIN_BUFFERS", INITIAL_BUFFERS / 2);
    public static final int MAX_SCALE_ATTEMPTS = Integer.getInteger(Profiler.class.getName() + ".MAX_SCALE_ATTEMPTS", MIN_BUFFERS * 4);
    public static final int DATA_SENDER_QUEUE_SIZE = Integer.getInteger(Profiler.class.getName() + ".DATA_SENDER.queue_size", 1000);
    public static final int METRICS_OUTPUT_VERSION = Integer.getInteger(Profiler.class.getName() + ".METRICS_OUTPUT_VERSION", 2);
    public static final int INMEMORY_SUSPEND_LOG_SIZE = Integer.getInteger(Profiler.class.getName() + ".INMEMORY_SUSPEND_LOG_SIZE", 50000);
    public static final boolean INMEMORY_SUSPEND_LOG = Boolean.parseBoolean(PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".INMEMORY_SUSPEND_LOG", "true"));
    public static final boolean THREAD_CPU = Boolean.parseBoolean(PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".THREAD_CPU_MONITORING", "true"));
    public static final boolean THREAD_WAIT = Boolean.parseBoolean(PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".THREAD_WAIT_MONITORING", "false"));
    public static final boolean THREAD_MEMORY = Boolean.parseBoolean(PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".THREAD_MEMORY_MONITORING", "true"));

    public static final int THREAD_CPU_MINIMAL_CALL_DURATION = PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".THREAD_CPU_MONITORING.minimal_duration", 100);
    public static final int THREAD_WAIT_MINIMAL_CALL_DURATION = PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".THREAD_WAIT_MONITORING.minimal_duration", 100);
    public static final int THREAD_MEMORY_MINIMAL_CALL_DURATION = PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".THREAD_MEMORY_MONITORING.minimal_duration", 100);

    public static final int MINIMAL_LOGGED_DURATION = PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".minimal_logged_duration", 1);
    public static final int INITIAL_STACK_LENGTH = PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".initial_stack_length", 50);
    public static final int MAX_STACK_LENGTH = PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".max_stack_length", 1000);

    public static final int MAX_BUFFERS = PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".MAX_BUFFERS", Math.max(INITIAL_BUFFERS * 2, 4096));
    public static final boolean BLOCK_WHEN_DIRTY_BUFFERS_QUEUE_IS_FULL = Boolean.parseBoolean(PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".BLOCK_WHEN_DIRTY_BUFFERS_QUEUE_IS_FULL", "false"));
    public static final int PARAMS_TRIM_SIZE = PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".PARAMS_TRIM_SIZE", 50000);

    public static final boolean LOG_ORACLE_SID_FOR_EACH_SQL = PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".LOG_ORACLE_SID_FOR_EACH_SQL", false);

    public static final boolean LOG_JMS_TEXT = PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".LOG_JMS_TEXT", true);

    public static final boolean ADD_TRY_CATCH_BLOCKS = PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".ADD_TRY_CATCH_BLOCKS", true);
    public static final boolean ADD_PLAIN_TRY_CATCH_BLOCKS = PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".ADD_PLAIN_TRY_CATCH_BLOCKS", true);
    public static final boolean ADD_INDY_TRY_CATCH_BLOCKS = PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".ADD_INDY_TRY_CATCH_BLOCKS", true);
    public static final boolean DISABLE_CALL_EXPORT = PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".DISABLE_CALL_EXPORT", false);
    public static final boolean WRITE_CALL_RANGES = PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".WRITE_CALL_RANGES", true);
    public static final boolean WRITE_CALLS_DICTIONARY = PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".WRITE_CALLS_DICTIONARY", true);

    public static final String SERVER_NAME = ServerNameResolverAgent.SERVER_NAME;

    public static final ThreadLocal<LocalState> localState = new ThreadLocal<LocalState>() {
        @Override
        protected LocalState initialValue() {
            LocalState state = activeThreads.get(Thread.currentThread());
            if (state == null) {
                state = new LocalState();
                LocalBuffer buffer = getEmptyBuffer(state);
                buffer.init(null);
                state.buffer = buffer;
                activeThreads.put(state.thread, state);
                }
            return state;
        }
    };

    public final static ConcurrentMap<Thread, LocalState> activeThreads = new ConcurrentHashMap<Thread, LocalState>();
    public final static BlockingQueue<LocalBuffer> dirtyBuffers = new ArrayBlockingQueue<LocalBuffer>(MAX_BUFFERS);
    public final static BlockingQueue<LocalBuffer> emptyBuffers = new ArrayBlockingQueue<LocalBuffer>(MAX_BUFFERS);
    final static MethodDictionary dictionary = new MethodDictionary(10000);
    public final static int PARAM_CALL_INFO = resolveTag("call.info") | DumperConstants.DATA_TAG_RECORD;
    public final static int PARAM_CALL_RED = resolveTag("call.red") | DumperConstants.DATA_TAG_RECORD;
    public final static int PARAM_CALL_IDLE = resolveTag("call.idle") | DumperConstants.DATA_TAG_RECORD;
    public final static int PARAM_EXCEPTION = resolveTag("exception") | DumperConstants.DATA_TAG_RECORD;
    public final static int PARAM_PLUGIN_EXCEPTION = resolveTag("profiler.plugin.exception") | DumperConstants.DATA_TAG_RECORD;
    public final static int PARAM_SQL = resolveTag("sql") | DumperConstants.DATA_TAG_RECORD;
    public final static int PARAM_ORACLE_SID = resolveTag("oracle.sid") | DumperConstants.DATA_TAG_RECORD;
    public static boolean dumperDead = false;
    //no need for volatile here since it only enables WARN logging for DirtyBuffer-related stuff
    public static boolean warnBufferQueueOverflow = Boolean.parseBoolean(PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".WARN_BUFFER_QUEUE_OVERFLOW", "false"));;
    public static int dumperIncarnation = 0;

    public static ProfilerPluginLogger pluginLogger;

    public static volatile Map<String, ProfilerProperty> properties = Collections.emptyMap();

    static {
        logger.fine("Qubership Profiler Initializing " + INITIAL_BUFFERS + " empty buffers of capacity " + LocalBuffer.SIZE);

        for (int i = 0; i < INITIAL_BUFFERS; i++) {
            final LocalBuffer buffer = new LocalBuffer();
            emptyBuffers.add(buffer);
        }
    }

    public static int resolveTag(String tag) {
        return dictionary.resolve(tag);
    }

    public static String resolveMethodId(int methodId) {
        return dictionary.resolve(methodId);
    }

    public static List<String> getTags() {
        return Collections.unmodifiableList(dictionary.getTags());
    }

    public static boolean addDirtyBuffer(LocalBuffer buffer, boolean force) {
        if(buffer.corrupted){
            logger.corruptedBufferWarning("ESCAGENTCORRUPTEDBUFFER: Attempt to add corrupted buffer to dirty buffers from thread " + Thread.currentThread().getName());

            return false;
        }
        if(buffer.state == null){
            logger.corruptedBufferWarning("ESCAGENTCORRUPTEDBUFFER:  Attempt to add dirty buffer with empty state to dirty buffers queue by thread " + Thread.currentThread().getName());

            return false;
        }

        //if this is a command buffer, wait until there is a spotr in queue. Otherwise it is allowed to discard the buffer
        if(force) {
            try {
                dirtyBuffers.put(buffer);
            } catch (InterruptedException e) {
                logger.corruptedBufferWarning("ESCAGENTCORRUPTEDBUFFER: Interrupted while forcibly adding a dirty buffer");

                Thread.currentThread().interrupt();
                return false;
            }
            return true;
        } else {
            boolean taken = dirtyBuffers.offer(buffer);
            if(!taken) {
                logger.reportDirtyBufferOverflow(Thread.currentThread().getName());
            }
            return taken;
        }
    }

    public static void addDirtyBufferIfPossible(LocalBuffer buffer) {
        if (buffer.corrupted) {
            logger.corruptedBufferWarning("ESCAGENTCORRUPTEDBUFFER: Can't add corrupted buffer if possible "+ buffer);
            return;
        }
        if(buffer.state == null) {
            logger.fine("Attempt to add dirty buffer with empty state to dirty buffers if possible queue by thread " + Thread.currentThread().getName());

            return ;
        }
        boolean added = false;
        boolean dumperDead = ProfilerData.dumperDead;
        boolean interrupted = false;
        int dirtySize = dirtyBuffers.size();
        if (dumperDead) {
            if (dirtySize < MAX_BUFFERS) {
                added = dirtyBuffers.offer(buffer);
            }
        } else {
            for (int i = 0; i < 9; i++) {
                try {
                    added = dirtyBuffers.offer(buffer, 1, TimeUnit.SECONDS);
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                    /* log below */
                }
            }
        }
        if (!added) {
            String message = "[Qubership Profiler] ESCAGENTCORRUPTEDBUFFER: Unable to add dirty buffer " + (dumperDead ? "" : "(timeout)")
                    + buffer.toString()
                    + ". Action: check logs/execution-statistics-collector.log to see why Dumper failed. " +
                    "Number of dirty buffers is " + dirtySize + ". Thread interrupts detected while recycling buffer: " + interrupted;
            logger.corruptedBufferWarning(message);
            boolean addedEmpty = emptyBuffers.offer(new LocalBuffer());
            int emptySize = emptyBuffers.size();
            Thread thread = Thread.currentThread();
            String threadName = thread.getName() + "@" + thread.getId() + "@" + thread.hashCode();
            logger.corruptedBufferWarning("[Qubershiop Profiler] ESCAGENTCORRUPTEDBUFFER: " + (addedEmpty ? "Added" : "Not added") +
                            " new buffer to empty queue." +
                            " dirtySize: " + dirtySize + ", emptySize: " + emptySize +
                            ", thread: " + threadName
            );
        }
        if (interrupted) { // reinterrupt, so subsequent code might notice the interrupt
            // Technically it is not required in Thread.exit case, however it is here for completeness
            Thread.currentThread().interrupt();
        }
    }

    static void addEmptyBuffer(LocalBuffer buffer) {
        emptyBuffers.offer(buffer);
    }


    public static LocalBuffer getEmptyBuffer(LocalState state) {
        LocalBuffer buffer = emptyBuffers.poll();
        if (buffer == null) {
            buffer = new LocalBuffer();
        }
        buffer.state = state;
        return buffer;
    }

    public static void clearThreadsInfo() {
        for (LocalState state : activeThreads.values())
            state.additional = null;
    }

}
