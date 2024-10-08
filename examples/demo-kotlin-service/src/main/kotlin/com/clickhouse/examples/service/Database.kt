package com.clickhouse.examples.service

import com.clickhouse.client.api.Client
import com.clickhouse.client.api.query.GenericRecord

object Database {

    private var client: Client? = null

    fun connect(dbUrl: String, dbUser: String, dbPassword: String) {
        println("Connecting to ClickHouse using $dbUrl...")
        client = Client.Builder()
            .addEndpoint(dbUrl)
            .setUsername(dbUser)
            .setPassword(dbPassword)
            .useNewImplementation(true)
            .build()
    }

    fun query(query: String): List<GenericRecord>? {
        return client!!.queryAll(query)
    }
}