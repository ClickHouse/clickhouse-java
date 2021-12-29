package com.clickhouse.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This defines protocols can be used to connect to ClickHouse.
 */
public enum ClickHouseProtocol {
    /**
     * Protocol detection is needed when establishing connection.
     */
    ANY(8123),
    /**
     * HTTP/HTTPS interface.
     */
    HTTP(8123, "http", "https"),
    /**
     * Native interface.
     */
    TCP(9000, "native", "tcp"),
    /**
     * MySQL interface.
     */
    MYSQL(9004, "mysql"),
    /**
     * PostgreSQL interface.
     */
    POSTGRESQL(9005, "postgres", "postgresql", "pgsql"),
    /**
     * Inter-server.
     */
    // INTERSERVER(9009),
    /**
     * GRPC interface.
     */
    GRPC(9100, "grpc");

    /**
     * Gets most suitable protocol according to given URI scheme.
     *
     * @param scheme case insensitive URI scheme
     * @return suitable protocol, {@link #ANY} if not found
     */
    public static ClickHouseProtocol fromUriScheme(String scheme) {
        ClickHouseProtocol protocol = ANY;

        for (ClickHouseProtocol p : values()) {
            for (String s : p.getUriSchemes()) {
                if (s.equalsIgnoreCase(scheme)) {
                    protocol = p;
                    break;
                }
            }
        }

        return protocol;
    }

    private final int defaultPort;
    private final List<String> schemes;

    ClickHouseProtocol(int defaultPort, String... schemes) {
        this.defaultPort = defaultPort;

        int len = schemes != null ? schemes.length : 0;
        if (len > 0) {
            List<String> list = new ArrayList<>(len);
            for (String scheme : schemes) {
                if (scheme != null) {
                    list.add(scheme);
                }
            }
            this.schemes = Collections.unmodifiableList(list);
        } else {
            this.schemes = Collections.emptyList();
        }
    }

    /**
     * Get default port used by the protocol.
     *
     * @return default port used by the protocol
     */
    public int getDefaultPort() {
        return this.defaultPort;
    }

    /**
     * Gets supported URI schemes.
     *
     * @return URI schemes
     */
    public List<String> getUriSchemes() {
        return this.schemes;
    }
}
