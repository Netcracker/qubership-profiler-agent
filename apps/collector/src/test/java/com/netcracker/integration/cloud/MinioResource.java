package com.netcracker.integration.cloud;

import io.quarkus.logging.Log;
import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.Base58;
import org.testcontainers.utility.DockerImageName;

import com.netcracker.common.PersistenceType;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

public class MinioResource implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {
    private GenericContainer<?> container;
    private Optional<String> containerNetworkId;

    public void init(Map<String, String> initArgs) {

        DockerImageName dockerImage = DockerImageName
                .parse("minio/minio:RELEASE.2024-11-07T00-52-20Z-cpuv1")
                .asCompatibleSubstituteFor("minio");
        Map<String, String> env = Map.of(
                "MINIO_ROOT_USER", "test",
                "MINIO_ROOT_PASSWORD", "test12345"
        );

        container = new GenericContainer<>(dockerImage)
                .withEnv(env)
                .withNetwork(Network.newNetwork())
                .withNetworkAliases("minio-" + Base58.randomString(6))
                .withCommand("server", "/data")
                .withLogConsumer(frame -> {
                    Log.debugf("[minio|%s] %s", frame.getType(), frame.getUtf8StringWithoutLineEnding());
                });
        container.addExposedPort(9000);
        container.setWaitStrategy(new HttpWaitStrategy()
                .forPort(9000)
                .forPath("/minio/health/ready")
                .withStartupTimeout(Duration.ofMinutes(5)));
    }

    @Override
    public void setIntegrationTestContext(DevServicesContext context) {
        containerNetworkId = context.containerNetworkId();
    }

    @Override
    public Map<String, String> start() {
        containerNetworkId.ifPresent(container::withNetworkMode);
        container.start();

        return ImmutableMap.of(
                "service.persistence", PersistenceType.CLOUD,
                "quarkus.minio.url", "http://" + container.getHost() + ":" + container.getMappedPort(9000),
                "quarkus.minio.access-key", "test",
                "quarkus.minio.secret-key", "test12345"
        );
    }

    @Override
    public void stop() {
        container.stop();
    }
}
