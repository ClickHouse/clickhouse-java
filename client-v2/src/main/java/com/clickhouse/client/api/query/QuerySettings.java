package com.clickhouse.client.api.query;


import com.clickhouse.client.config.ClickHouseClientOption;
import lombok.Getter;

import java.util.Map;

@Getter
public class QuerySettings {

    private Map<String, Object> rawSettings;

    public String getCompressAlgorithm() {
        return (String) rawSettings.get(ClickHouseClientOption.COMPRESS_ALGORITHM.getKey());
    }

    public String getFormat() {
        return (String) rawSettings.get(ClickHouseClientOption.FORMAT.getKey());
    }

    public String getQueryID() {
        return (String) rawSettings.get("query_id");
    }

    public static class Builder {
        private QuerySettings settings = new QuerySettings();

        public Builder compressAlgorithm(String algortihm) {
            settings.rawSettings.put(ClickHouseClientOption.COMPRESS_ALGORITHM.getKey(), algortihm);
            return this;
        }
        public Builder format(String format) {
            settings.rawSettings.put(ClickHouseClientOption.FORMAT.getKey(), format);
            return this;
        }
        public Builder queryID(String queryID) {
            settings.rawSettings.put("query_id", queryID);
            return this;
        }
        public Builder addSetting(String key, Object value) {
            settings.rawSettings.put(key, value);
            return this;
        }
        public QuerySettings build() {
            return settings;
        }
    }
}
