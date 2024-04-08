package com.clickhouse.client.api;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseFormat;

import java.util.HashMap;
import java.util.Map;

public class InsertSettings {
    Map<String, Object> rawSettings = new HashMap<>();

    private InsertSettings(Builder builder) {
        //rawSettings.put(ClickHouseClientOption.DEDUPE_TOKEN.getKey(), builder.deduplicationToken);
        //rawSettings.put(ClickHouseClientOption.QUERY_ID.getKey(), builder.queryId);
        rawSettings.put(ClickHouseClientOption.FORMAT.getKey(), builder.format);
    }

    public Object getSetting(String option) {
        return rawSettings.get(option);
    }

    public void setSetting(String option, Object value) {
        rawSettings.put(option, value);
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
