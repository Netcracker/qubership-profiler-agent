package com.netcracker.persistence.adapters.cloud;
import com.netcracker.common.PersistenceType;
import com.netcracker.common.models.CallsModel;
import com.netcracker.persistence.CallsPersistence;
import com.netcracker.persistence.adapters.cloud.cdt.CloudCallsEntity;
import com.netcracker.persistence.adapters.cloud.dao.CloudCallsDao;
import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@ApplicationScoped
public class CloudCallsService implements CallsPersistence {

    @Inject
    CloudCallsDao cloudCallsDao;

    @Override
    public void insert(CallsModel toSave) {
        cloudCallsDao.insert(CloudCallsEntity.prepare(toSave));
        cloudCallsDao.commit();
    }
}
