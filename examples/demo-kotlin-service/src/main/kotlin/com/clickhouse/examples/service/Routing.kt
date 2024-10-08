package com.clickhouse.examples.service

import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*

fun Application.configureRouting() {
    routing {
        get("/numbers") {
            val limit = call.request.queryParameters["limit"]?.toIntOrNull() ?: 10
            val numbers = Database.query("SELECT toUInt32(number) FROM system.numbers LIMIT $limit")!!
                .map { it.getLong(1) }

            call.respond(numbers)
        }
    }
}
