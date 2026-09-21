package com.clickhouse.examples.config;

import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.Dialect;

/**
 * Minimal Hibernate dialect for the JPA operations used by this service.
 *
 * <p>ClickHouse has no official Hibernate dialect. The base Hibernate 6 SQL rendering is
 * sufficient here because the application only performs inserts and aggregate reads, while
 * table creation remains under {@code ClickHouseSchemaInitializer}.
 */
public class ClickHouseDialect extends Dialect {

    public ClickHouseDialect() {
        super(DatabaseVersion.make(24, 3));
    }
}
