package com.clickhouse.examples;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Spins up a real ClickHouse instance in a container for local development and integration
 * tests. {@link ServiceConnection} wires the container's JDBC coordinates straight into
 * Spring's {@code DataSource}, so no manual configuration is needed.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    @Bean
    @ServiceConnection
    ClickHouseContainer clickHouseContainer() {
        return new ClickHouseContainer(DockerImageName.parse("clickhouse/clickhouse-server:24.3"));
    }
}
