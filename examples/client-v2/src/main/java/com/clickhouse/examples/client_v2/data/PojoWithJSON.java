package com.clickhouse.examples.client_v2.data;

import java.util.Objects;

public class PojoWithJSON {

    // This field is a string representation of a JSON object
    private String eventPayload;

    public String getEventPayload() {
        return eventPayload;
    }

    public void setEventPayload(String eventPayload) {
        this.eventPayload = eventPayload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PojoWithJSON that = (PojoWithJSON) o;
        return Objects.equals(eventPayload, that.eventPayload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventPayload);
    }

    @Override
    public String toString() {
        return "PojoWithJSON{" +
                "eventPayload='" + eventPayload + '\'' +
                '}';
    }

    public static String createTable(String tableName) {
        return "CREATE TABLE " + tableName + " (eventPayload JSON) ENGINE = MergeTree() ORDER BY tuple()";
    }
}
