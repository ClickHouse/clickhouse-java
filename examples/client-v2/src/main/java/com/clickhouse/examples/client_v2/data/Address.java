package com.clickhouse.examples.client_v2.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Nested POJO to test ClickHouse Tuple serialization with custom objects
 */
@AllArgsConstructor
@Data
@NoArgsConstructor
public class Address {
    private String street;
    private String city;
}
