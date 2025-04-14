package com.clickhouse.demo_service;


import com.clickhouse.client.api.Client;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableJpaRepositories
@EnableTransactionManagement
public class DbConfiguration {

    @Bean
    public Client chDirectClient(MeterRegistry meterRegistry, @Value("${db.url}") String dbUrl, @Value("${db.user}") String dbUser,
                                 @Value("${db.pass}") String dbPassword) {
        return new Client.Builder()
                .addEndpoint(dbUrl)
                .setUsername(dbUser)
                .setPassword(dbPassword)

                // sets the maximum number of connections to the server at a time
                // this is important for services handling many concurrent requests to ClickHouse
                .setMaxConnections(100)
                .setLZ4UncompressedBufferSize(1058576)
                .setSocketRcvbuf(500_000)
                .setSocketTcpNodelay(true)
                .setSocketSndbuf(500_000)
                .setClientNetworkBufferSize(500_000)
                .allowBinaryReaderToReuseBuffers(true) // using buffer pool for binary reader
                .registerClientMetrics(meterRegistry, "clickhouse-client-metrics")
                .build();
    }

    // JPA configured via application.properties
}
