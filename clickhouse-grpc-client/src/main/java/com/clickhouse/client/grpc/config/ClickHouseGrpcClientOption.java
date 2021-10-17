package com.clickhouse.client.grpc.config;

import java.io.Serializable;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.config.ClickHouseConfigOption;

/**
 * gRPC client options.
 */
public enum ClickHouseGrpcClientOption implements ClickHouseConfigOption {
    /**
     * Flow control window.
     */
    FLOW_CONTROL_WINDOW("flow_control_window", 0,
            "Size of flow control window in byte, 0 or negative number are same as default"),
    /**
     * Maximum message size.
     */
    MAX_INBOUND_MESSAGE_SIZE("max_inbound_message_size", 4 * 1024 * 1024,
            "The maximum message size allowed to be received."),
    /**
     * Maximum size of metadata.
     */
    MAX_INBOUND_METADATA_SIZE("max_inbound_metadata_size", 8192,
            "The maximum size of metadata allowed to be received. This enforces HTTP/2 SETTINGS_MAX_HEADER_LIST_SIZE, the maximum size of header list that client is prepared to accept."),
    /**
     * Whether to use Okhttp instead of Netty.
     */
    USE_OKHTTP("use_okhttp", false,
            "Whether to use lightweight transport based on Okhttp instead of Netty. In many cases Netty is faster than Okhttp."),
    /**
     * Whether to use full stream decompression.
     */
    USE_FULL_STREAM_DECOMPRESSION("use_full_stream_decompression", false,
            "Whether to use full stream decompression for better compression ratio or not.");

    private final String key;
    private final Serializable defaultValue;
    private final Class<?> clazz;
    private final String description;

    <T extends Serializable> ClickHouseGrpcClientOption(String key, T defaultValue, String description) {
        this.key = ClickHouseChecker.nonNull(key, "key");
        this.defaultValue = ClickHouseChecker.nonNull(defaultValue, "defaultValue");
        this.clazz = defaultValue.getClass();
        this.description = ClickHouseChecker.nonNull(description, "description");
    }

    @Override
    public Serializable getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public Class<?> getValueType() {
        return clazz;
    }
}
