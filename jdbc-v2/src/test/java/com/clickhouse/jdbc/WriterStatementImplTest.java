package com.clickhouse.jdbc;

import com.clickhouse.jdbc.internal.SqlParserFacade;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.EnumSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

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

    @DataProvider
    Object[][] testBatchInsertWithRowBinary_dp() {

        Object[][] template = new Object[][]{
                {"INSERT  INTO \n `%s` \nVALUES (?, ?, abs(?), ?)", PreparedStatementImpl.class, null}, // only string possible (because of abs(?))
                {"INSERT  INTO\n `%s` \nVALUES (?, ?, ?, ?)", WriterStatementImpl.class, null}, // row binary writer
                {" INSERT INTO %s (ts, v1, v2, v3) VALUES (?, ?, ?, ?)", WriterStatementImpl.class, null}, // only string supported now
                {"INSERT INTO %s SELECT ?, ?, ?, ?", PreparedStatementImpl.class, null}, // only string possible (because of SELECT)
        };

        Set<SqlParserFacade.SQLParser> parsers = EnumSet.of(SqlParserFacade.SQLParser.ANTLR4_LIGHT, SqlParserFacade.SQLParser.JAVACC);
        Object[][] dataset = new Object[template.length * parsers.size()][];

        int i = 0;
        for (SqlParserFacade.SQLParser p : parsers) {
            for (Object[] t : template) {
                Object[] test = new Object[t.length];
                System.arraycopy(t, 0, test, 0, t.length);
                test[t.length - 1] = p;
                dataset[i++] = test;
            }
        }

        return dataset;
    }

    @Test(dataProvider = "testBatchInsertWithRowBinary_dp")
    void testBatchInsertWithRowBinary(String sql, Class implClass, SqlParserFacade.SQLParser parser) throws Exception {
        String table = "test_batch";
        long seed = System.currentTimeMillis();
        Random rnd = new Random(seed);
        System.out.println("testBatchInsert seed" + seed);
        Properties properties = new Properties();
        properties.put(DriverProperties.BETA_ROW_BINARY_WRITER.getKey(), "true");
        try (Connection conn = getJdbcConnection(properties)) {

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS " + table +
                        " ( ts DateTime, v1 Int32, v2 Float32, v3 Int32) Engine MergeTree ORDER BY ()");
            }

            final int nBatches = 10;
            try (PreparedStatement stmt = conn.prepareStatement(String.format(sql, table))) {
                Assert.assertEquals(stmt.getClass(), implClass);
                for (int bI = 0; bI < nBatches; bI++) {
                    stmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
                    stmt.setInt(2, rnd.nextInt());
                    stmt.setFloat(3, rnd.nextFloat());
                    stmt.setInt(4, rnd.nextInt());
                    stmt.addBatch();
                }

                int[] result = stmt.executeBatch();
                for (int r : result) {
                    Assert.assertTrue(r == 1 || r == PreparedStatement.SUCCESS_NO_INFO);
                }
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM " + table);) {

                int count = 0;
                while (rs.next()) {
                    Timestamp ts = rs.getTimestamp(1);
                    assertNotNull(ts);
                    assertTrue(rs.getInt(2) != 0);
                    assertTrue(rs.getFloat(3) != 0.0f);
                    assertTrue(rs.getInt(4) != 0);
                    count++;
                }
                assertEquals(count, nBatches);

                stmt.execute("TRUNCATE " + table);
            }
        }
    }
}
