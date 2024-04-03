package com.clickhouse.client.api;

import com.clickhouse.data.ClickHouseFormat;

public class InsertSettings {
    private final String deduplicationToken;
    private final String queryId;
    private final ClickHouseFormat format;

    private InsertSettings(Builder builder) {
        this.deduplicationToken = builder.deduplicationToken;
        this.queryId = builder.queryId;
        this.format = builder.format;
    }

    public String getDeduplicationToken() {
        return deduplicationToken;
    }

    public String getQueryId() {
        return queryId;
    }

    public ClickHouseFormat getFormat() {
        return format;
    }

    public static class Builder {
        private String deduplicationToken = "";
        private String queryId = "";
        private ClickHouseFormat format = ClickHouseFormat.RowBinary;

        public Builder() {}

        public Builder addDeduplicationToken(String deduplicationToken) {
            this.deduplicationToken = deduplicationToken;
            return this;
        }

        public Builder addQueryId(String queryId) {
            this.queryId = queryId;
            return this;
        }

        public Builder addFormat(ClickHouseFormat format) {
            this.format = format;
            return this;
        }

        public InsertSettings build() {
            return new InsertSettings(this);
        }
    }
}
