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
                .compressServerResponse(false)
                .useNewImplementation(true) // using new transport layer implementation

                // sets the maximum number of connections to the server at a time
                // this is important for services handling many concurrent requests to ClickHouse
                .setOption(ClickHouseHttpOption.MAX_OPEN_CONNECTIONS.getKey(), "2000")
                .build();
    }
}
