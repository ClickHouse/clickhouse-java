package com.clickhouse.examples;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Entry point for the IoT signal ingestion service.
 *
 * <p>The service exposes an authenticated HTTP endpoint that accepts IoT signals,
 * persists them to ClickHouse, and records OpenTelemetry metrics that are themselves
 * exported to ClickHouse.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class IotIngestApplication {

    public static void main(String[] args) {
        SpringApplication.run(IotIngestApplication.class, args);
    }
}
