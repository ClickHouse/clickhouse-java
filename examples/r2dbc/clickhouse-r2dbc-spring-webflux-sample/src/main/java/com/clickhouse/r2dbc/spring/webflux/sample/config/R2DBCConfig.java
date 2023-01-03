package com.clickhouse.r2dbc.spring.webflux.sample.config;

import io.r2dbc.spi.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static io.r2dbc.spi.ConnectionFactories.get;
import static java.lang.String.format;

@Configuration
public class R2DBCConfig {

    @Value("${clickhouse.host:localhost}")
    private String host;

    @Value("${clickhouse.port:8123}")
    private String port;

    @Value("${clickhouse.database:clickdb}")
    private String database;

    @Value("${clickhouse.user:default}")
    private String user;

    @Value("${clickhouse.password:''}")
    private String password;

    @Bean
    public ConnectionFactory connectionFactory() {
        return get(format("r2dbc:clickhouse:http://%s:%s@%s:%d/%s", user, password,
                host, Integer.parseInt(port), database));
    }
}
