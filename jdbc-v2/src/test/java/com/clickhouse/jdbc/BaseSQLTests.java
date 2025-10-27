package com.clickhouse.jdbc;

import com.clickhouse.data.ClickHouseColumn;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.testng.Assert.fail;

@Test(groups = {"integration"})
public class BaseSQLTests extends JdbcIntegrationTest {

    private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Test(groups = {"integration"}, dataProvider = "testSQLQueryWithResultSetDP", enabled = false)
    public void testSQLQueryWithResultSet(Map<String, TestDataset> tables, SQLTestCase testCase) throws Exception {

        try (Connection connection = getJdbcConnection()) {
            setupTables(tables, connection);

            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(testCase.query)) {
                int checkCount = 0;
                checkCount += rsMetadataChecks(resultSet, testCase, tables);
                checkCount += dataCheck(resultSet, testCase, tables);
                Assert.assertEquals(checkCount, testCase.checks.size(), "Check count does not match");
                Assert.assertTrue(checkCount > 0, "Test without checks");
            }
        }
    }

    public static final ArrayList<TestResultCheckModel> DEFAULT_CHECKS = new ArrayList<>();
    static {
        DEFAULT_CHECKS.add(new TestResultCheckModel("row_count", "${events.rowCount}"));
        DEFAULT_CHECKS.add(new TestResultCheckModel("column_count", "${events.columnsCount}"));
        DEFAULT_CHECKS.add(new TestResultCheckModel("column_names", "${events.columnNames}"));
        DEFAULT_CHECKS.add(new TestResultCheckModel("column_types", "${events.columnTypes}"));
    }

    @DataProvider(name = "testSQLQueryWithResultSetDP")
    public static Object[][] testSQLQueryWithResultSetDP() throws Exception {
        return loadTestData("datasets.yaml", "SQLTests.yaml");
    }

    protected static Object[][] loadTestData(String datasetsSource, String sqlTestsSource) throws Exception {
        ClassLoader classLoader = BaseSQLTests.class.getClassLoader();
        try (InputStream datasetsInput = classLoader.getResourceAsStream(datasetsSource);
             InputStream tests = classLoader.getResourceAsStream(sqlTestsSource)) {

            // Parse resource files
            TestDataset[] datasets = yamlMapper.readValue(datasetsInput, TestDataset[].class);
            Map<String, TestDataset> datasetMap = Arrays.stream(datasets).collect(
                    Collectors.toMap(testDataset -> "datasets/" + testDataset.getName(), Function.identity()));
            SQLTestCase[] testCases = yamlMapper.readValue(tests, SQLTestCase[].class);

            // Create test data for each test case
            Object[][] testData = new Object[testCases.length][];
            for (int i = 0; i < testCases.length; i++) {
                SQLTestCase testCase = testCases[i];
                if (testCase.getChecks() == null || testCase.getChecks().isEmpty()) {
                    testCase.setChecks(DEFAULT_CHECKS);
                }
                Map<String, TestDataset> testDatasetMap = new HashMap<>();
                for (Map.Entry<String, String> entry : testCase.getTables().entrySet()) {
                    TestDataset testDataset = datasetMap.get(entry.getValue());
                    if (testDataset == null) {
                        fail("Dataset " + entry.getValue() + " not found");
                    }
                    testDatasetMap.put(entry.getKey(), testDataset);
                }
                testData[i] = new Object[]{testDatasetMap, testCase};
            }

            return testData;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void setupTables(Map<String, TestDataset> tables, Connection connection) throws Exception {
        for (Map.Entry<String, TestDataset> entry : tables.entrySet()) {
            String tableName = entry.getKey();
            TestDataset dataset = entry.getValue();

            try (Statement statement = connection.createStatement()) {

                // Drop table if exists
                statement.execute("DROP TABLE IF EXISTS " + tableName);

                // Create table
                StringBuilder createTableQuery = new StringBuilder();
                createTableQuery.append("CREATE TABLE ").append(tableName).append(" (");

                String[] columns = dataset.getColumns();
                for (int i = 0; i < columns.length; i++) {
                    if (i > 0) {
                        createTableQuery.append(", ");
                    }
                    createTableQuery.append(columns[i]);
                }
                createTableQuery.append(") ENGINE = MergeTree() ORDER BY tuple()");

                statement.execute(createTableQuery.toString());

                // Insert data
                if (dataset.getData() != null && dataset.getData().length > 0) {
                    StringBuilder insertQuery = new StringBuilder();
                    insertQuery.append("INSERT INTO ").append(tableName).append(" VALUES ");

                    String[][] data = dataset.getData();
                    for (int i = 0; i < data.length; i++) {
                        insertQuery.append("(");
                        for (int j = 0; j < data[i].length; j++) {
                            insertQuery.append('\'').append(data[i][j]).append('\'').append(',');
                        }
                        insertQuery.setLength(insertQuery.length() - 1);
                        insertQuery.append("),");
                    }
                    insertQuery.setLength(insertQuery.length() - 1);

                    statement.execute(insertQuery.toString());
                }
            }
        }
    }

    public int rsMetadataChecks(ResultSet rs, SQLTestCase testCase, Map<String, TestDataset> tables) throws Exception {
        int checkCount = 0;
        for (TestResultCheckModel check : testCase.checks) {
            ResultSetCheck resultSetCheck = RESULT_SET_METADATA_CHECKS.get(check.getName());
            if (resultSetCheck != null) {
                resultSetCheck.check(rs, check, tables);
                checkCount++;
            }
        }
        return checkCount;
    }

    protected int dataCheck(ResultSet rs, SQLTestCase testCase, Map<String, TestDataset> tables) throws Exception {

        List<Pair<TestResultCheckModel, DataCheck>> checks = testCase.getChecks().stream().filter(cm -> DATA_CHECKS.containsKey(cm.getName()))
                .map(cm -> Pair.of(cm, DATA_CHECKS.get(cm.getName()))).collect(Collectors.toList());


        while (rs.next()) {
            for (Pair<TestResultCheckModel, DataCheck> check : checks) {
                check.getRight().check(rs, check.getLeft(), tables);
            }
        }
        return checks.size();
    }


    @Data
    @NoArgsConstructor
    public static final class TestDataset {
        private String name;
        private String[] columns;
        private String[][] data;

        private ClickHouseColumn[] clickHouseColumns;
        private String[] columnTypes;
        private String[] columnNames;

        @Override
        public String toString() {
            return name;
        }

        public void setColumns(String[] columns) {
            this.columns = columns;
            this.clickHouseColumns = new ClickHouseColumn[columns.length];
            this.columnTypes = new String[columns.length];
            this.columnNames = new String[columns.length];
            for (int i = 0; i < columns.length; i++) {
                this.clickHouseColumns[i] = ClickHouseColumn.parse(columns[i]).get(0);
                this.columnTypes[i] = this.clickHouseColumns[i].getOriginalTypeName();
                this.columnNames[i] = this.clickHouseColumns[i].getColumnName();
            }
        }

    }

    @Data
    @NoArgsConstructor
    public static final class SQLTestCase {
        private String name;
        private String query;
        private Map<String, String> tables;
        @JsonIgnore
        private Map<String, String> datasets;
        private List<TestResultCheckModel> checks;

        @Override
        public String toString() {
            return name;
        }

        public void setTables(Map<String, String> tables) {
            this.tables = tables;
            this.datasets = tables.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class TestResultCheckModel {
        private String name;
        private Object expected;

        @Override
        public String toString() {
            return name + "(" + expected + ")";
        }
    }

    public static final Map<String, DataCheck> DATA_CHECKS = new HashMap<>();

    static {
        DATA_CHECKS.put("row_count", new RowCountCheck());
    }

    public interface DataCheck {
        void check(ResultSet rs, TestResultCheckModel check, Map<String, TestDataset> testDataset) throws Exception;
    }

    public static final class RowCountCheck implements DataCheck {

        @Override
        public void check(ResultSet rs, TestResultCheckModel check, Map<String, TestDataset> testDataset) throws Exception {
            if (rs.isLast()) {
                int rowCount = rs.getRow();
                Assert.assertEquals(rowCount, getIntegerProperty(check.getExpected(), testDataset));
            }
        }
    }


    public static final Map<String, ResultSetCheck> RESULT_SET_METADATA_CHECKS = new HashMap<>();

    static {
        RESULT_SET_METADATA_CHECKS.put("column_count", new CheckResultSetColumnCount());
        RESULT_SET_METADATA_CHECKS.put("column_names", new CheckResultSetColumnNames());
        RESULT_SET_METADATA_CHECKS.put("column_types", new CheckResultSetColumnTypes());
    }

    public interface ResultSetCheck {
        void check(ResultSet rs, TestResultCheckModel check, Map<String, TestDataset> testDataset) throws Exception;
    }


    private static Object getDataProperty(Object propertyOrValue, Map<String, TestDataset> testDatasetMap) {
        if (propertyOrValue instanceof String) {
            String property = (String) propertyOrValue;
            if (property.startsWith("${") && property.endsWith("}")) {
                String propertyName = property.substring(2, property.length() - 1);
                String[] parts = propertyName.split("\\.");
                TestDataset dataset = testDatasetMap.get(parts[0]);

                switch (parts[1]) {
                    case "columnsCount":
                        return dataset.getColumns().length;
                    case "columnNames":
                        return dataset.getColumnNames();
                    case "columnTypes":
                        return dataset.getColumnTypes();
                    case "rowCount":
                        return dataset.data.length;
                }


                return null;
            }
        }

        return propertyOrValue;
    }

    private static Integer getIntegerProperty(Object propertyOrValue, Map<String, TestDataset> testDatasetMap) {
        Object property = getDataProperty(propertyOrValue, testDatasetMap);
        if (property == null) {
            return null;
        }
        if (property instanceof Number) {
            return ((Number) property).intValue();
        } else if (property instanceof String) {
            return Integer.parseInt((String) property);
        }
        throw new IllegalArgumentException("Property " + property + " is not a number");
    }

    private static Object getArrayProperty(Object propertyOrValue, Map<String, TestDataset> testDatasetMap) {
        Object property = getDataProperty(propertyOrValue, testDatasetMap);
        if (property == null) {
            return null;

        }
        if (property instanceof List<?>) {
            return ((List<?>) property).toArray();
        } else if (property.getClass().isArray()) {
            return property;
        }
        throw new IllegalArgumentException("Property " + property + " is not an array");
    }

    public static final class CheckResultSetColumnCount implements ResultSetCheck {
        @Override
        public void check(ResultSet rs, TestResultCheckModel check, Map<String, TestDataset> tables) throws Exception {
            Integer expected = getIntegerProperty(check.getExpected(), tables);
            int actual = rs.getMetaData().getColumnCount();
            Assert.assertEquals(actual, expected, "Column count does not match");
        }
    }

    public static final class CheckResultSetColumnNames implements ResultSetCheck {
        @Override
        public void check(ResultSet rs, TestResultCheckModel check, Map<String, TestDataset> tables) throws Exception {
            ResultSetMetaData metaData = rs.getMetaData();
            Object[] expected = (Object[]) getArrayProperty(check.getExpected(), tables);
            Object[] actual = new String[metaData.getColumnCount()];
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                actual[i] = metaData.getColumnName(i + 1);
            }
            Assert.assertEquals(actual, expected, "Column names do not match");
        }
    }


    public static final class CheckResultSetColumnTypes implements ResultSetCheck {
        @Override
        public void check(ResultSet rs, TestResultCheckModel check, Map<String, TestDataset> tables) throws Exception {
            Object[] expected = (Object[]) getArrayProperty(check.getExpected(), tables);
            Object[] actual = new String[expected.length];
            for (int i = 0; i < expected.length; i++) {
                actual[i] = rs.getMetaData().getColumnTypeName(i + 1);
            }
            Assert.assertEquals(actual, expected, "Column types do not match");
        }
    }
}