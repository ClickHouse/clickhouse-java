package com.clickhouse.client.query;

public class NoGettersPOJO {

    private int p1;

    private int p2;

    public NoGettersPOJO(int p1, int p2) {
        this.p1 = p1;
        this.p2 = p2;
    }

    public int sum() {
        return p1 + p2;
    }

    public static String generateTableCreateSQL(String tableName) {
        return "CREATE TABLE " + tableName + " (p1 Int32, p2 Int32) ENGINE = MergeTree() ORDER BY ()";
    }
}
