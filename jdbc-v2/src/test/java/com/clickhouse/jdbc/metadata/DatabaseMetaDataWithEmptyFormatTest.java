package com.clickhouse.jdbc.metadata;

import com.clickhouse.client.api.ClientConfigProperties;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Runs the whole {@link DatabaseMetaDataTest} suite over connections configured with an empty
 * {@code format} property. Such a connection omits the {@code X-ClickHouse-Format} request header,
 * so the server answers with its {@code default_format} ({@code TabSeparated}) unless a statement
 * asks for something else. {@code DatabaseMetaData} must keep working because it pins
 * {@code RowBinaryWithNamesAndTypes} on every statement it creates.
 */
@Test(groups = { "integration" })
public class DatabaseMetaDataWithEmptyFormatTest extends DatabaseMetaDataTest {

    @Override
    public Connection getJdbcConnection(Properties properties) throws SQLException {
        Properties props = properties == null ? new Properties() : (Properties) properties.clone();
        props.setProperty(ClientConfigProperties.INPUT_OUTPUT_FORMAT.getKey(), "");
        return super.getJdbcConnection(props);
    }

    @Override
    public void testAllTableEnginesFromSystemTableEnginesAreMapped() {
        throw new SkipException("Reads through a plain Statement, which cannot consume TabSeparated");
    }
}
