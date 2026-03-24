package com.netcracker.persistence.adapters.cloud;

import com.netcracker.common.PersistenceType;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@Singleton
public class CloudPostgresConfiguration {

    @ConfigProperty(name = "quarkus.datasource.jdbc.url")
    String url;

    @ConfigProperty(name = "quarkus.datasource.username")
    String username;

    @ConfigProperty(name = "quarkus.datasource.password")
    String password;

    @ApplicationScoped
    public Connection createConnection() {

        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);

        // TODO: think about reconnection
        try {
            Connection connection = DriverManager.getConnection(url, props);
            // TODO: use it only for calls and traces
            connection.setAutoCommit(false); // All LargeObject API calls must be within a transaction block
            return connection;
        } catch (SQLException e) {
            Log.errorf("error during postgres connection creating: %s", e.getMessage());
            System.exit(-1);
            return null;
        }

    }

}
