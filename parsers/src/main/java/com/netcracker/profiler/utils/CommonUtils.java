package com.netcracker.profiler.utils;

import static com.netcracker.profiler.util.ProfilerConstants.CALL_HEADER_MAGIC;

import com.netcracker.profiler.chart.UnaryFunction;
import com.netcracker.profiler.dump.DataInputStreamEx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Date;
import java.util.zip.ZipException;

public class CommonUtils {
    public static final Logger log = LoggerFactory.getLogger(CommonUtils.class);

    public static long getCallsStartTimestamp(File file) {
        try (DataInputStreamEx calls = DataInputStreamEx.openDataInputStream(file, 16)) {
            if (calls == null) {
                return System.currentTimeMillis();
            }
            long time = calls.readLong();
            if ((int) (time >>> 32) == CALL_HEADER_MAGIC) {
                time = calls.readLong();
            }
            if (log.isTraceEnabled()) {
                log.trace("Timestamp of {} is {} ({})", file.getAbsolutePath(), new Date(time), time);
            }
            return time;
        } catch (EOFException e) {
            return System.currentTimeMillis();
        } catch (ZipException e) {
            if(!"invalid stored block lengths".equals(e.getMessage())) {
                //it's ok to get ZipException(invalid stored block lengths) when reading current stream
                throw new RuntimeException(e);
            }
            return System.currentTimeMillis();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    };

    public static long getTraceStartTimestamp(File file) {
        try (DataInputStreamEx trace = DataInputStreamEx.openDataInputStream(file, 24)) {
            if (trace == null) {
                return System.currentTimeMillis();
            }
            trace.readLong(); // serverStart
            trace.readLong(); // threadId
            long realTime = trace.readLong();
            if (log.isTraceEnabled()) {
                log.trace("Timestamp of {} is {} ({})", file.getAbsolutePath(), new Date(realTime), realTime);
            }
            return realTime;
        } catch (EOFException e) {
            return System.currentTimeMillis();
        } catch (ZipException e) {
            if(!"invalid stored block lengths".equals(e.getMessage())) {
                //it's ok to get ZipException(invalid stored block lengths) when reading current stream
                throw new RuntimeException(e);
            }
            return System.currentTimeMillis();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    };

    /**
     * Taken from http://en.wikipedia.org/wiki/Binary_search_algorithm
     * #Deferred_detection_of_equality
     * Adapted to find upper bound.
     */
    public static <T, K> int upperBound(T[] a, K key, int imin, int imax,
                                        UnaryFunction<T, K> keySelector, Comparator<K> comparator) {
        int initialMax = imax;
        // continually narrow search until just one element remains
        while (imin < imax) {
            int imid = (imin + imax + 1) >>> 1;

            // code must guarantee the interval is reduced at each iteration
            assert imid > imin
                    : "search interval should be reduced min=" + imin
                    + ", mid=" + imid + ", max=" + imax;
            // note: 0 <= imin < imax implies imid will always be less than imax

            // reduce the search
            if (comparator.compare(keySelector.evaluate(a[imid]), key) > 0) {
                // change max index to search lower subarray
                imax = imid - 1;
            } else {
                imin = imid;
            }
        }
        // At exit of while:
        //   if a[] is empty, then imax < imin
        //   otherwise imax == imin

        // deferred test for equality
        if (imax != imin) {
            return -1;
        }

        int cmp = comparator.compare(keySelector.evaluate(a[imin]), key);
        if (cmp == 0) {
            // Detected exact match, just return it
            return imin;
        }
        if (cmp > 0) {
            // We were asked the key that is less than all the values in array
            return imin - 1;
        }
        // If imin != initialMax we return imin since a[imin-1] < key < a[imin]
        // If imin == initialMax we return initialMax+11 since
        // the resulting window might be empty
        // For instance, range between 99 following and 100 following
        // Use if-else to ensure code coverage is reported for each return
        if (imin == initialMax) {
            return initialMax + 1;
        } else {
            return imin;
        }
    }

}
