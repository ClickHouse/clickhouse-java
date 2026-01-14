package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;

import java.util.concurrent.CompletableFuture;

/**
 * Interface between high-level logic and low-level network protocol.
 * There is no much difference between protocols from high-level perspective but each
 * protocol implementation has own specific even within same technology and that defines line of separation.
 * For example, HTTP protocol implemented with Apache HTTP Client and HttpClient from JDK differs in the way how
 * headers are passed, how connection pool works, how response should be handled. So making a base class with sparse
 * methods will lead to a more tangled and complex code. Implementations may always inherit each other for example
 * secure http client may be based on top of basic http client to keep TLS related code in separate class. Any sort of
 * compositions also possible.
 */
public interface ProtocolClient {

    /**
     * Constructs a protocol specific request, executes with retries and returns query response object
     * filled with information needed to get data.
     * </br>
     * Note: each protocol implementation uses some library so headers and parameters handling is protocol client
     * responsibility.
     * @param statement - string with SQL statement
     * @param settings - query settings
     * @return - future that will return QueryResponse object
     */
    CompletableFuture<QueryResponse> doQueryRequest(String statement, QuerySettings settings);

    /**
     * Constructs a protocol specific request, executes with retries and returns insert response object
     * filled with result of the operation. Implementation may decide to close output stream to release
     * connection.
     * @param insertStatement - string with SQL insert statement
     * @param settings - insert settings
     * @return
     */
    CompletableFuture<InsertResponse> doInsertRequest(String insertStatement, InsertSettings settings);
}
