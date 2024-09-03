package com.clickhouse.demo_service;


import com.clickhouse.client.api.Client;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DbConfiguration {

    @Bean
    public Client chDirectClient(@Value("${db.url}") String dbUrl, @Value("${db.user}") String dbUser,
                                 @Value("${db.pass}") String dbPassword) {
        return new Client.Builder()
                .addEndpoint(dbUrl)
                .setUsername(dbUser)
                .setPassword(dbPassword)
                .useNewImplementation(true) // using new transport layer implementation
                .setLZ4UncompressedBufferSize(1050000) // increase a LZ4 buffer size
                .setMaxConnections(50)
                .build();
    }
}
