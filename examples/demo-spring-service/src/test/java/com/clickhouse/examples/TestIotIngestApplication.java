package com.clickhouse.examples;

import org.springframework.boot.SpringApplication;

/**
 * Local development launcher: starts the application with a Testcontainers-managed ClickHouse.
 *
 * <p>Run with {@code ./mvnw spring-boot:test-run} (or from your IDE). A ClickHouse container
 * is started automatically and the app connects to it; stop the process to tear it down.
 */
public class TestIotIngestApplication {

    public static void main(String[] args) {
        SpringApplication.from(IotIngestApplication::main)
                .with(TestcontainersConfiguration.class)
                .run(args);
    }
}
