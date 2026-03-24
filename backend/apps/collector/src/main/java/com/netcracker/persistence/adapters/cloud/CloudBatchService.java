package com.netcracker.persistence.adapters.cloud;

import com.netcracker.common.PersistenceType;
import com.netcracker.common.models.Sizeable;
import com.netcracker.persistence.BatchPersistence;
import com.netcracker.persistence.op.Operation;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;
import java.util.function.Function;

@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@ApplicationScoped
public class CloudBatchService implements BatchPersistence {

    @Override
    public <T extends Sizeable> void saveInBatches(List<T> toInsert, Function<T, Operation> saver) {
        // FIXME: Now, instead of batches, records are saved one by one (future)
        toInsert.forEach(saver::apply);
    }

    @Override
    public void execute(Operation op) {

    }

    @Override
    public void execute(List<Operation> batch) {

    }

    @Override
    public void flushIndexes() {

    }
}
