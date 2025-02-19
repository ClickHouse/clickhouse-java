package com.clickhouse.client;

@Deprecated
public class UnsupportedProtocolException extends IllegalStateException {
    private final ClickHouseProtocol protocol;

    public UnsupportedProtocolException(ClickHouseProtocol protocol, String errorMessage) {
        super(errorMessage);

        this.protocol = protocol == null ? ClickHouseProtocol.ANY : protocol;
    }

    public ClickHouseProtocol getProtocol() {
        return protocol;
    }
}