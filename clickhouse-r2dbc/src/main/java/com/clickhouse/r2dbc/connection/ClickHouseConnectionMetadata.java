package com.clickhouse.r2dbc.connection;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;
import io.r2dbc.spi.ConnectionMetadata;

public class ClickHouseConnectionMetadata implements ConnectionMetadata {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseConnectionMetadata.class);

    final ClickHouseClient client;
    final ClickHouseNode server;

    private final String serverVersion = null;

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
     * @return server version
     */
    @Override
    public String getDatabaseVersion() {
        if (serverVersion != null) {
            return serverVersion;
        }
        try {
            // blocking here
            ClickHouseResponse resp = client.connect(server).query("SELECT version()").executeAndWait();
            return resp.records().iterator().next().getValue(0).asString();
        } catch (Exception e) {
            log.error("While fetching server version, error occured.", e);
            return null;
        }
    }
}
