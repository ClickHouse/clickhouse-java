package com.clickhouse.r2dbc.connection;

import java.util.concurrent.atomic.AtomicReference;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;
import io.r2dbc.spi.ConnectionMetadata;

public class ClickHouseConnectionMetadata implements ConnectionMetadata {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseConnectionMetadata.class);

    final ClickHouseClient client;
    final ClickHouseNode server;

    private final AtomicReference<String> serverVersion = new AtomicReference<>("");

    ClickHouseConnectionMetadata(ClickHouseClient client, ClickHouseNode server) {
        this.client = client;
        this.server = server;
    }

    @Override
    public String getDatabaseProductName() {
        return "Clickhouse";
    }

    /**
     * Blocking operation. Queries server version by calling "SELECT version()" statement.
     *
     * @return non-null server version
     */
    @Override
    public String getDatabaseVersion() {
        String version = serverVersion.get();
        if (version.isEmpty()) {
            // blocking here
            try (ClickHouseResponse resp = client.read(server).query("SELECT version()").executeAndWait()) {
                version = resp.firstRecord().getValue(0).asString();
                if (!serverVersion.compareAndSet("", version)) {
                    return serverVersion.get();
                }
            } catch (Exception e) {
                log.error("While fetching server version, error occured.", e);
            }
        }
        return version;
    }
}
