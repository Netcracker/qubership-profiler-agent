package com.netcracker.profiler.utils;

import com.netcracker.profiler.chart.UnaryFunction;

import java.util.Comparator;

public class CommonUtils {

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
