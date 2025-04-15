package org.qubership.profiler.agent;

import static java.util.logging.Level.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.text.MessageFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class ESCLogger {
    private static final String MESSAGE_FORMAT_STRING_DEFAULT = "{3,date,YYYY-MM-dd HH:mm:ss,SSS}  {1} [thread={2}] [class={0}]- {4}{5}";
    private static final String MESSAGE_FORMAT_STRING = PropertyFacadeBoot.getProperty(ESCLogger.class.getName() + ".format", MESSAGE_FORMAT_STRING_DEFAULT);
    private static final MessageFormat MESSAGE_FORMAT = new MessageFormat(MESSAGE_FORMAT_STRING);
    public static Level ESC_LOG_LEVEL;

    private String name;
    private final Level logLevel;
    private static boolean javaUtilLoggingEnabled;
    //for testing purposes to calibrate load testing
    private static Runnable corruptedBufferCallback;
    private static AtomicInteger numCorruptions = new AtomicInteger(0);

    static {
        String logLevelProperty = PropertyFacadeBoot.getPropertyOrEnvVariable("ESC_LOG_LEVEL");
        if(logLevelProperty == null ) {
            ESC_LOG_LEVEL = WARNING;
        } else {
            if("debug".equalsIgnoreCase(logLevelProperty.trim())){
                ESC_LOG_LEVEL = FINE;
            } else {
                try {
                    ESC_LOG_LEVEL = Level.parse(logLevelProperty);
                } catch (Throwable t) {
                    ESC_LOG_LEVEL = WARNING;
                }
            }
        }
    }

    public ESCLogger(String name) {
        this(name, ESC_LOG_LEVEL);
    }

    public ESCLogger(String name, Level logLevel) {
        this.name = name;
        this.logLevel = logLevel;
    }

    static void enableJavaUtilLogging() {
        System.out.println("Enabling java.util.logging");
        javaUtilLoggingEnabled = true;
    }

    public static boolean isJavaUtilLoggingEnabled(){
        return javaUtilLoggingEnabled;
    }

    public static ESCLogger getLogger(Class clazz) {
        return getLogger(clazz.getName());
    }

    public static ESCLogger getLogger(String className) {
        return new ESCLogger(className);
    }

    public static ESCLogger getLogger(Class clazz, Level logLevel) {
        return getLogger(clazz.getName(), logLevel);
    }

    public static ESCLogger getLogger(String className, Level logLevel) {
        return new ESCLogger(className, logLevel);
    }

    public void severe(String message) {
        log(SEVERE, message, null);
    }

    public void severe(String message, Throwable t) {
        log(SEVERE, message, t);
    }

    public void info(String message) {
        log(INFO, message, null);
    }

    public void fine(String message) {
        log(FINE, message, null);
    }

    public void warning(String message) {
        log(WARNING, message, null);
    }

    /**
     * custom level that is DEBUG when dumper has never been initialized
     * and WARN when dumper has been initialized at least once. Meaning that collector is present in the environment
     * @param message
     */
    public void corruptedBufferWarning(String message) {
        corruptedBufferWarning(message, null);
    }

    public void reportDirtyBufferOverflow(String threadName) {
        numCorruptions.incrementAndGet();
    }

    public void printDirtyBufferWarningInCaseOfOverflow(){
        int numOverflows = numCorruptions.getAndSet(0);
        if(numOverflows > 0) {
            corruptedBufferWarning("ESCAGENTCORRUPTEDBUFFER: " + numOverflows + " buffers could not be sent to remote collector and have been discarded");
        }
    }

    public void corruptedBufferWarning(String message, Throwable t) {
        if(ProfilerData.warnBufferQueueOverflow) {
            log(WARNING, message, t);
        } else {
            log(FINE, message, t);
        }
        if(corruptedBufferCallback != null) {
            corruptedBufferCallback.run();
        }
    }

    public static void setCorruptedBufferCallback(Runnable callback) {
        corruptedBufferCallback = callback;
    }

    public boolean isFineEnabled(){
        return FINE.intValue() >= logLevel.intValue();
    }

    public void log(Level level, String message, Throwable t) {
        if(level.intValue() < logLevel.intValue()) {
            return;
        }

        Object[] arguments = new Object[6];
        arguments[0] = name;
        arguments[1] = level;
        arguments[2] = Thread.currentThread().getName();
        arguments[3] = new Date();
        arguments[4] = message;

        if (t != null) {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            t.printStackTrace(new PrintStream(byteArrayOutputStream));

            arguments[5] = "\n" + byteArrayOutputStream.toString();
        } else {
            arguments[5] = "";
        }

        String toPrint;
        // MessageFormat instances are not thread-safe and ESCLogger shouldn't be used in
        // performance sensitive parts of the code, so we will just use synchronization
        synchronized (MESSAGE_FORMAT) {
            toPrint = MESSAGE_FORMAT.format(arguments);
        }

        if(SEVERE.equals(level) || WARNING.equals(level)) {
            System.err.println(toPrint);
        } else {
            System.out.println(toPrint);
        }
    }
}
