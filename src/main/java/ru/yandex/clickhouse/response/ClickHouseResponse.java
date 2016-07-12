package ru.yandex.clickhouse.response;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.List;

/**
 * Object for jackson for ClickHouse response
 */
public class ClickHouseResponse {
    private List<Meta> meta;
    @JsonDeserialize(contentUsing = ArrayToStringDeserializer.class)
    private List<List<String>> data;
    @JsonDeserialize(using = ArrayToStringDeserializer.class)
    private List<String> totals;
    private Extremes extremes;
    private int rows;
    private int rows_before_limit_at_least;


    public static class Extremes {
        @JsonDeserialize(using = ArrayToStringDeserializer.class)
        private List<String> min;
        @JsonDeserialize(using = ArrayToStringDeserializer.class)
        private List<String> max;

        public List<String> getMin() {
            return min;
        }

        public void setMin(List<String> min) {
            this.min = min;
        }

        public List<String> getMax() {
            return max;
        }

        public void setMax(List<String> max) {
            this.max = max;
        }
    }

    public static class Meta {
        private String name;
        private String type;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        @Override
        public String toString() {
            return "Meta{" +
                    "name='" + name + '\'' +
                    ", type='" + type + '\'' +
                    '}';
        }
    }

    public Extremes getExtremes() {
        return extremes;
    }

    public void setExtremes(Extremes extremes) {
        this.extremes = extremes;
    }

    public List<Meta> getMeta() {
        return meta;
    }

    public void setMeta(List<Meta> meta) {
        this.meta = meta;
    }

    public List<List<String>> getData() {
        return data;
    }

    public void setData(List<List<String>> data) {
        this.data = data;
    }

    public int getRows() {
        return rows;
    }

    public void setRows(int rows) {
        this.rows = rows;
    }

    public int getRows_before_limit_at_least() {
        return rows_before_limit_at_least;
    }

    public void setRows_before_limit_at_least(int rows_before_limit_at_least) {
        this.rows_before_limit_at_least = rows_before_limit_at_least;
    }

    public List<String> getTotals() {
        return totals;
    }

    public void setTotals(List<String> totals) {
        this.totals = totals;
    }

    @Override
    public String toString() {
        return "ClickHouseResponse{" +
                "meta=" + meta +
                ", data=" + data +
                ", rows=" + rows +
                ", rows_before_limit_at_least=" + rows_before_limit_at_least +
                '}';
    }
}
