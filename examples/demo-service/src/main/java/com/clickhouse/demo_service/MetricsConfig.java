package com.clickhouse.demo_service;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class MetricsConfig {

    // Define a simple config for the logging registry
    @Bean
    public LoggingRegistryConfig loggingRegistryConfig() {
        return new LoggingRegistryConfig() {
            @Override
            public String get(String key) {
                // Only "step" is required. Returning null for others means defaults will be used.
                if ("step".equals(key)) {
                    return "35s"; // Publish metrics every 5 seconds
                }
                return null;
            }

            @Override
            public Duration step() {
                return Duration.ofSeconds(5);
            }
        };
    }

    // Create the LoggingMeterRegistry bean.
    @Bean
    public LoggingMeterRegistry loggingMeterRegistry(LoggingRegistryConfig config) {
        LoggingMeterRegistry registry = new LoggingMeterRegistry(config, Clock.SYSTEM);
        // Start the registry’s internal scheduler so that metrics are published periodically.
        registry.start();
        return registry;
    }
}
