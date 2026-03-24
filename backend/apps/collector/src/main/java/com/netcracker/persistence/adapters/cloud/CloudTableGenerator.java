package com.netcracker.persistence.adapters.cloud;

import com.netcracker.common.PersistenceType;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;
import java.util.List;

@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@ApplicationScoped
public class CloudTableGenerator {

    private static final int FIVE_MINUTES_IN_SECONDS = 300; // 5 * 60

    @Inject
    Connection connection;

    @PostConstruct
    void init() {
        var curr5min = getTimestampTruncatedToFiveMinutes();
        var prev5min = curr5min - FIVE_MINUTES_IN_SECONDS;
        var next5min = getTimestampTruncatedToFiveMinutes() + FIVE_MINUTES_IN_SECONDS;
    }



    private void createTable(String sql) {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.executeUpdate();
        } catch (SQLException e) {
            Log.errorf("error during create table: %s", e.getMessage());
        }
        try {
            connection.commit();
        } catch (SQLException e) {
            Log.errorf("error during commit transaction for dump: %s", e.getMessage());
        }
    }

    public static long getTimestampTruncatedToFiveMinutes() {
        return Instant.now().truncatedTo(new TemporalUnit() {
            @Override
            public Duration getDuration() {
                return Duration.ofSeconds(FIVE_MINUTES_IN_SECONDS);
            }

            @Override
            public boolean isDurationEstimated() {
                return false;
            }

            @Override
            public boolean isDateBased() {
                return false;
            }

            @Override
            public boolean isTimeBased() {
                return false;
            }

            @Override
            public <R extends Temporal> R addTo(R temporal, long amount) {
                return null;
            }

            @Override
            public long between(Temporal temporal1Inclusive, Temporal temporal2Exclusive) {
                return 0;
            }
        }).getEpochSecond();
    }

}
