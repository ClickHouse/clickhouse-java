package com.clickhouse.examples.service

import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.plugins.contentnegotiation.*

fun main(args: Array<String>) {
    io.ktor.server.netty.EngineMain.main(args)
}

fun Application.configureDatabase(env: ApplicationEnvironment) {
    Database.connect(
        env.config.property("ktor.database.url").getString(),
        env.config.property("ktor.database.user").getString(),
        env.config.property("ktor.database.password").getString()
    )
}

fun Application.module() {
    install(ContentNegotiation) {
        json()
    }
    configureRouting()
    configureDatabase(environment)

}
