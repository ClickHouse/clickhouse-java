package com.clickhouse.demo_service;

import org.apache.catalina.connector.Connector;
import org.springframework.boot.web.embedded.tomcat.TomcatConnectorCustomizer;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TomcatConfig implements TomcatConnectorCustomizer {
    @Override
    public void customize(Connector connector) {
        connector.setProperty("maxThreads", "2");
    }
}
