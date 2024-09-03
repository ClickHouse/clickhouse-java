package com.clickhouse.client.api;

public enum ConnectionReuseStrategy {

    /**
     * Reuse recently freed connection and returned to a pool
     */
    LIFO,

    /**
     * Reuse mostly all connections
     */
    FIFO
    ;
}
