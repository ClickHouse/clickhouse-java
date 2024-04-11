package com.clickhouse.client.api.query;


/**
 * QueryStatement class is responsible for constructing SQL query statements.
 *
 */
public class QueryStatement {

    private final String query;

    public QueryStatement(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }
}
