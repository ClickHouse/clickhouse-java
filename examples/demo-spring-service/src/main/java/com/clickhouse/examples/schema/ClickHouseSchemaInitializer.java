package com.clickhouse.examples.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Creates the ClickHouse tables the service needs, if they do not already exist.
 *
 * <p>Runs during bean initialization, before the application starts accepting traffic, so
 * the first signal insert and metric export both find their tables in place.
 */
@Component
public class ClickHouseSchemaInitializer implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseSchemaInitializer.class);

    private static final String SIGNALS_DDL = """
            CREATE TABLE IF NOT EXISTS iot_signals (
                signal_id   UUID DEFAULT generateUUIDv4(),
                device_id   String,
                location_id UUID,
                signal_type LowCardinality(String),
                value       Float64,
                unit        String,
                event_time  DateTime64(3),
                received_at DateTime64(3) DEFAULT now64(3)
            ) ENGINE = MergeTree
            ORDER BY (location_id, event_time, signal_type)
            """;

    private static final String LOCATION_MIGRATION_DDL = """
            ALTER TABLE iot_signals
            ADD COLUMN IF NOT EXISTS location_id UUID AFTER device_id
            """;

    private static final String METRICS_DDL = """
            CREATE TABLE IF NOT EXISTS otel_metrics (
                name        String,
                description String,
                unit        String,
                type        LowCardinality(String),
                value       Float64,
                attributes  Map(String, String),
                start_time  DateTime64(9),
                time        DateTime64(9)
            ) ENGINE = MergeTree
            ORDER BY (name, time)
            """;

    private final JdbcTemplate jdbc;

    public ClickHouseSchemaInitializer(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void afterPropertiesSet() {
        jdbc.execute(SIGNALS_DDL);
        jdbc.execute(LOCATION_MIGRATION_DDL);
        jdbc.execute(METRICS_DDL);
        log.info("ClickHouse schema ready (tables: iot_signals, otel_metrics)");
    }
}
