package com.clickhouse.client;

/**
 * This defines protocols can be used to connect to ClickHouse.
 */
public enum ClickHouseProtocol {
    /**
     * Protocol detection is needed when establishing connection.
     */
    ANY(8123),
    /**
     * HTTP/HTTPS.
     */
    HTTP(8123),
    /**
     * Native TCP.
     */
    NATIVE(9000),
    /**
     * MySQL interface.
     */
    MYSQL(9004),
    /**
     * PostgreSQL interface.
     */
    POSTGRESQL(9005),
    /**
     * Inter-server HTTP/HTTPS.
     */
    INTERSERVER(9009),
    /**
     * GRPC interface.
     */
    GRPC(9100);

    private final int defaultPort;

    ClickHouseProtocol(int defaultPort) {
        this.defaultPort = defaultPort;
    }

    /**
     * Get default port used by the protocol.
     *
     * @return default port used by the protocol
     */
    public int getDefaultPort() {
        return this.defaultPort;
    }
}
