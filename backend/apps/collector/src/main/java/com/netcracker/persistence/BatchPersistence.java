package com.netcracker.persistence;

import com.netcracker.common.models.Sizeable;
import com.netcracker.persistence.op.Operation;
import com.netcracker.persistence.op.UniOperation;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public interface BatchPersistence {

    <T extends Sizeable> void saveInBatches(List<T> toInsert, Function<T, Operation> saver);

    void execute(Operation op);

    void execute(List<Operation> batch);

    void flushIndexes();

    // execute async UniOperations
    default <T extends Operation> void executeAsync(List<T> batch) {
        if (batch == null || batch.isEmpty()) {
            return;
        }

        var list = new ArrayList<Uni<Boolean>>(batch.size());
        for (var op : batch) {
            if (op instanceof UniOperation uni) {
                list.add(uni.op());
            }
        }
        executeUniAsync(list);
    }

    default void executeUniAsync(List<Uni<Boolean>> list) {
        if (!list.isEmpty()) {
            var start = System.currentTimeMillis();
            Uni.join().all(list).
                    andCollectFailures().
                    onSubscription().invoke(() -> {
                        long ts = System.currentTimeMillis() - start;
                        if (ts > 500) {
                            Log.debugf("Done? %d operations in  %d ms", list.size(), ts);
                        } else if (ts > 10) {
                            Log.tracef("Done? %d operations in  %d ms", list.size(), ts);
                        }
                    }).
                    await().atMost(maxBatchDuration());
            long ts = System.currentTimeMillis() - start;
            if (ts > 500) {
                Log.debugf("got %d operations in  %d ms", list.size(), ts);
            } else if (ts > 10) {
                Log.tracef("got %d operations in  %d ms", list.size(), ts);
            }
        }
    }

    default Duration maxBatchDuration() {
        return Duration.of(2, ChronoUnit.MINUTES);
    }
}
