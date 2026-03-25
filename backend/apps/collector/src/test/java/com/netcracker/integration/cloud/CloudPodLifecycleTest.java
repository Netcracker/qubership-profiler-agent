package com.netcracker.integration.cloud;

import com.netcracker.cdt.collector.PodLifecycleTest;
import com.netcracker.common.PersistenceType;
import com.netcracker.integration.Profiles;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

@QuarkusTest
@TestProfile(Profiles.CloudTest.class)
@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
public class CloudPodLifecycleTest extends PodLifecycleTest {
}
