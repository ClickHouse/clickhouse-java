package com.clickhouse.jdbc;

import com.clickhouse.jdbc.internal.DriverProperties;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

@Test(groups = {"integration"})
public class WriterStatementImplTest extends JdbcIntegrationTest {
    

    @Test(groups = {"integration"})
    public void testTargetTypeMethodThrowException() throws SQLException {

        Properties properties = new Properties();
        properties.setProperty(DriverProperties.BETA_ROW_BINARY_WRITER.getKey(), "true");
        try (Connection connection = getJdbcConnection(properties);
             PreparedStatement stmt = connection.prepareStatement("INSERT INTO system.numbers VALUES (?, ?)")) {
            Assert.assertTrue(stmt instanceof WriterStatementImpl);

            Assert.expectThrows(SQLException.class, () -> stmt.setObject(1, "", JDBCType.VARCHAR.getVendorTypeNumber()));
            Assert.expectThrows(SQLException.class, () -> stmt.setObject(1, "", JDBCType.VARCHAR));
            Assert.expectThrows(SQLException.class, () -> stmt.setObject(1, "", JDBCType.DECIMAL.getVendorTypeNumber(), 3));
            Assert.expectThrows(SQLException.class, () -> stmt.setObject(1, "", JDBCType.DECIMAL, 3));
        }
    }
}
