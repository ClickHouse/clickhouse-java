package com.clickhouse.jdbc.metadata;

import com.clickhouse.client.api.ClientConfigProperties;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Runs the whole {@link ShowStatementDatabaseMetaDataTest} suite over connections configured with an empty
 * {@code format} property, the same as {@link DatabaseMetaDataWithEmptyFormatTest} does for the system table queries.
 */
@Test(groups = { "integration" })
public class ShowStatementDatabaseMetaDataWithEmptyFormatTest extends ShowStatementDatabaseMetaDataTest {

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

    @Override
    @Test(groups = { "integration" }, dataProvider = "columnNameLikePatterns")
    public void testGetColumnsMatchesColumnNamePatternLikeServer(String pattern, String columnName, boolean expected) {
        throw new SkipException("Reads through a plain PreparedStatement, which cannot consume TabSeparated");
    }
}
