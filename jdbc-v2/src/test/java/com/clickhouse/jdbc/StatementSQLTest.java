package com.clickhouse.jdbc;


import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Integration test for prepared statement. Testing SQL with prepared statement is main focus of this test.
 * Any tests that relate to schema, data type, tricky SQL comes here.
 *
 */
@Test(groups = {"integration"})
public class StatementSQLTest extends BaseSQLTests {

    private AtomicBoolean isTablesSetup = new AtomicBoolean(false);


    @Test(groups = {"integration"}, dataProvider = "testSQLStatements")
    public void testQuery(Map<String, TestDataset> tables, SQLTestCase testCase) throws Exception {

        try (Connection connection = getJdbcConnection()) {
            setupTables(tables, connection);

            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(testCase.getQuery())) {

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
        return loadTestData("datasets.yaml", "StatementSQLTests.yaml");
    }

    /**
     * Test which SQL keywords can be used as table names (unquoted).
     * This test reads keywords from sql-keywords.txt and attempts to create
     * a table with each keyword as the name, then select from it.
     */
    @Test(groups = {"integration"}, enabled = false)
    public void testKeywordsAsTableNames() throws Exception {
        List<String> keywords = loadKeywordsFromResource("sql-keywords.txt");
        List<String> allowedKeywords = new ArrayList<>();
        List<String> disallowedKeywords = new ArrayList<>();

        try (Connection connection = getJdbcConnection()) {
            try (Statement stmt = connection.createStatement()) {
                for (String keyword : keywords) {
                    String tableName = keyword; // Use keyword directly without quoting
                    String createSql = "CREATE TABLE IF NOT EXISTS " + tableName + " (id Int32) ENGINE = Memory";
                    String selectSql = "SELECT * FROM " + tableName;
                    String dropSql = "DROP TABLE IF EXISTS " + tableName;

                    try {
                        stmt.execute(createSql);
                        try (ResultSet rs = stmt.executeQuery(selectSql)) {
                            // Just consume the result set
                            while (rs.next()) {
                                // no-op
                            }
                        }
                        stmt.execute(dropSql);
                        allowedKeywords.add(keyword);
                    } catch (Exception e) {
                        disallowedKeywords.add(keyword);
                        // Try to drop in case table was created but select failed
                        try {
                            stmt.execute(dropSql);
                        } catch (Exception ignored) {
                            // Ignore cleanup errors
                        }
                    }
                }
            }
        }

        System.out.println("=== Keywords ALLOWED as table names (" + allowedKeywords.size() + ") ===");
        for (String kw : allowedKeywords) {
            System.out.println(kw);
        }

        System.out.println("\n=== Keywords NOT ALLOWED as table names (" + disallowedKeywords.size() + ") ===");
        for (String kw : disallowedKeywords) {
            System.out.println(kw);
        }

        // The test passes regardless - we're just collecting information
        // If you want to assert something specific, add it here
        Assert.assertTrue(allowedKeywords.size() + disallowedKeywords.size() == keywords.size(),
                "All keywords should be categorized");
    }

    private List<String> loadKeywordsFromResource(String resourceName) throws Exception {
        List<String> keywords = new ArrayList<>();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourceName)) {
            if (is == null) {
                throw new RuntimeException("Resource not found: " + resourceName);
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    // Skip empty lines and comments
                    if (!line.isEmpty() && !line.startsWith("#")) {
                        keywords.add(line);
                    }
                }
            }
        }
        return keywords;
    }

}
