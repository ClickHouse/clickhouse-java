package com.clickhouse.jdbc;


import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

/**
 * Integration test for prepared statement. Testing SQL with prepared statement is main focus of this test.
 * Any tests that relate to schema, data type, tricky SQL comes here.
 *
 */
@Test(groups = {"integration"}, enabled = false)
public class PreparedStatementSQLTest extends BaseSQLTests {

    @Test(groups = {"integration"}, dataProvider = "testSQLStatements")
    public void testQuery(Map<String, TestDataset> tables, SQLTestCase testCase) throws Exception {

        try (Connection connection = getJdbcConnection()) {
            setupTables(tables, connection);

            try (PreparedStatement stmt = connection.prepareStatement(testCase.getQuery());
                 ResultSet rs = stmt.executeQuery()) {

                int checkCount = 0;
                checkCount += rsMetadataChecks(rs, testCase, tables);
                checkCount += dataCheck(rs, testCase, tables);
                Assert.assertEquals(checkCount, testCase.getChecks().size(), "Check count does not match");
                Assert.assertTrue(checkCount > 0, "Test without checks");
            }
        }
    }


    @DataProvider(name = "testSQLStatements")
    public static Object[][] testSQLStatementsDP() throws Exception {
        return loadTestData("datasets.yaml", "PreparedStatementSQLTests.yaml");
    }

}
