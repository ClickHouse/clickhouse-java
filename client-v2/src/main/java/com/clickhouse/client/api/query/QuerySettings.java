package com.clickhouse.client.api.query;


import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

import java.util.Map;

@Builder
@Getter
public class QuerySettings {

    private Map<String, Object> rawSettings;

    public enum CompressionMethod {
        LZ4,
        ZSTD,
        NONE
    }

    private CompressionMethod compressionMethod;

    private Integer readTimeout;

    private String queryID;
}
