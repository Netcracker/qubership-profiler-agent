package com.netcracker.integration.cloud;

import io.quarkus.logging.Log;
import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.ext.ScriptUtils;
import org.testcontainers.jdbc.JdbcDatabaseDelegate;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

public class PostgresResource implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {

    public static final String INIT_SCRIPT = "persistence/cloud/init_db.sql";

    private Optional<String> containerNetworkId;
    private JdbcDatabaseContainer<?> container;

    public void init(Map<String, String> initArgs) {
        try (var container = new PostgreSQLContainer<>("postgres:latest")) {
            container
                .withUsername("postgres")
                .withPassword("postgres")
                .withStartupTimeout(Duration.ofMinutes(4))
                .withLogConsumer(frame -> {
                    Log.debugf("[pg|%s] %s", frame.getType(), frame.getUtf8StringWithoutLineEnding());
                });
            container.addExposedPort(5432);
            this.container = container;
        }
    }

    @Override
    public void setIntegrationTestContext(DevServicesContext context) {
        containerNetworkId = context.containerNetworkId();
    }

    @Override
    public Map<String, String> start() {

        // Apply the network to the container
        containerNetworkId.ifPresent(container::withNetworkMode);

        // Start container before retrieving its URL or other properties
        container.start();

        // Create database
        cleanBeforeTests();

        String jdbcUrl = container.getJdbcUrl();
        if (containerNetworkId.isPresent()) {
            // Replace hostname + port in the provided JDBC URL with the hostname of the Docker container
            // running PostgreSQL and the listening port.
            jdbcUrl = fixJdbcUrl(jdbcUrl);
        }

        Log.warnf("quarkus.datasource.jdbc.url: " + jdbcUrl);
        Log.debugf("quarkus.datasource.username: " + container.getUsername());
        Log.debugf("quarkus.datasource.password: " + container.getPassword());

        // Return a map containing the configuration the application needs to use the service
        return ImmutableMap.of(
                "service.persistence", "cloud",
                "quarkus.datasource.username", container.getUsername(),
                "quarkus.datasource.password", container.getPassword(),
                "quarkus.datasource.jdbc.url", jdbcUrl,
                "quarkus.datasource.jdbc.driver", "org.postgresql.Driver");
    }

    private String fixJdbcUrl(String jdbcUrl) {
        // Part of the JDBC URL to replace
        String hostPort = container.getHost() + ':' + container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT);

        // Host/IP on the container network plus the unmapped port
        String networkHostPort =
                container.getCurrentContainerInfo().getConfig().getHostName()
                        + ':'
                        + PostgreSQLContainer.POSTGRESQL_PORT;

        Log.warnf("PG port container (from %d): actual %s", PostgreSQLContainer.POSTGRESQL_PORT, networkHostPort);
        return jdbcUrl.replace(hostPort, networkHostPort);
    }

    private void cleanBeforeTests() {
        var delegate = new JdbcDatabaseDelegate(container, "");
        ScriptUtils.runInitScript(delegate, INIT_SCRIPT);
        int i = 0;
    }

    @Override
    public void stop() {
        if (container == null) {
            return;
        }
        if (!container.isShouldBeReused()) {
            container.stop();
        }
    }
}
