package com.clickhouse.client.api.query;

public class QuerySettings<TValue> {

    private String key;
    private TValue value;

    public QuerySettings(String key, TValue defaultValue) {
        this.key = key;
        this.value = defaultValue;
    }

    public String getKey() {
        return key;
    }

    public TValue getValue() {
        return value;
    }


    public static class Compression extends QuerySettings<Compression.Method> {

        public enum Method {
            LZ4,
            ZSTD,
            NONE
        }

        public Compression(Method method) {
            super("compression", method);
        }
    }

    public static class ReadTimeout extends QuerySettings<Integer> {
        public ReadTimeout(int timeout) {
            super("read_timeout", timeout);
        }
    }

    public static class QueryID extends QuerySettings<String> {
        public QueryID(String queryID) {
            super("query_id", queryID);
        }
    }
}
