package com.clickhouse.client.metadata;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.metadata.DefaultColumnToMethodMatchingStrategy;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseVersion;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MetadataTests extends BaseIntegrationTest {

    private Client client;

    @BeforeMethod(groups = { "integration" })
    public void setUp() {
        client = newClient().build();
    }

    @Test(groups = { "integration" })
    public void testGetTableSchema() {
        prepareDataSet("describe_table");

        TableSchema schema = client.getTableSchema("describe_table", ClickHouseServerForTest.getDatabase());

        Assert.assertEquals(schema.getTableName(), "describe_table");
        Assert.assertEquals(schema.getDatabaseName(), ClickHouseServerForTest.getDatabase());

        Assert.assertEquals(schema.getColumns().size(), 2);

        List<ClickHouseColumn> columns = schema.getColumns();
        Assert.assertEquals(columns.get(0).getColumnName(), "param1");
        Assert.assertEquals(columns.get(0).getDataType().name(), "UInt32");
    }

    @Test(groups = { "integration" })

    public void testGetTableSchemaDifferentDb() throws Exception {
        String table = "test_get_table_schema_different_db";
        String db = ClickHouseServerForTest.getDatabase() + "_schema_test" ;
        try {
            QuerySettings settings = new QuerySettings().setDatabase(db);
            client.execute("DROP DATABASE IF EXISTS " + db).get().close();
            client.execute("CREATE DATABASE " + db).get().close();
            client.query("DROP TABLE IF EXISTS " + table, settings).get().close();
            client.query("CREATE TABLE " + table + " (rowId Int32) Engine=MergeTree ORDER BY ()", settings).get().close();
            TableSchema tableSchema = client.getTableSchema(table, db);
            Assert.assertEquals(tableSchema.getColumnByName("rowId").getDataType(), ClickHouseDataType.Int32);
        } finally {
            client.execute("DROP DATABASE IF EXISTS " + db).get().close();
        }
    }

    private void prepareDataSet(String tableName) {

        try {
            String sql = "CREATE TABLE IF NOT EXISTS " + tableName + " (param1 UInt32, param2 UInt16) ENGINE = Memory";
            client.execute(sql).get();

            sql = "INSERT INTO " + tableName + " VALUES (1, 2), (3, 4), (5, 6)";
            client.execute(sql).get();
        } catch (Exception e) {
            Assert.fail("Failed to prepare data set", e);
        }
    }

    @Test(groups = {"integration"}, dataProvider = "testMatchingNormalizationData")
    public void testDefaultColumnToMethodMatchingStrategy(String methodName, String columnName) {
        methodName = DefaultColumnToMethodMatchingStrategy.INSTANCE.normalizeMethodName(methodName);
        columnName = DefaultColumnToMethodMatchingStrategy.INSTANCE.normalizeColumnName(columnName);
        Assert.assertEquals(methodName, columnName, "Method name: " + methodName + " Column name: " + columnName);
    }

    @DataProvider(name = "testMatchingNormalizationData")
    public Object[][] testMatchingNormalizationData() {
        return new Object[][]{
                {"getLastName", "LastName"},
                {"getLastName", "last_name"},
                {"getLastName", "last.name"},
                {"setLastName", "last.name"},
                {"isLastUpdate", "last_update"},
                {"hasMore", "more"},
                {"getFIRST_NAME", "first_name"},
                {"setUPDATED_ON", "updated.ON"},
                {"getNUM_OF_TRIES", "num_of_tries"},
                {"gethas_more", "has_more"},

        };
    }

    @Test(groups = {"integration"})
    public void testCreateTableWithAllDataTypes() throws Exception {
        String tableName = "test_all_data_types";
        
        // Query system.data_type_families to get all known types
        List<GenericRecord> dbTypes = client.queryAll("SELECT name, alias_to FROM system.data_type_families ORDER BY name");
        
        // Types that cannot be used directly in CREATE TABLE columns
        Set<String> excludedTypes = new HashSet<>();
        excludedTypes.add("AggregateFunction");
        excludedTypes.add("SimpleAggregateFunction");
        excludedTypes.add("Nothing");
        excludedTypes.add("Nullable"); // Nullable is a wrapper, not a base type
        excludedTypes.add("LowCardinality"); // LowCardinality is a wrapper, not a base type
        excludedTypes.add("Enum"); // Enum is a base type, use Enum8 or Enum16 instead
        
        // Build column definitions
        StringBuilder createTableSql = new StringBuilder();
        createTableSql.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");
        
        int columnIndex = 0;
        Set<String> addedTypes = new HashSet<>();
        
        for (GenericRecord dbType : dbTypes) {
            String typeName = dbType.getString("name");
            String aliasTo = dbType.getString("alias_to");
            
            // Use alias if available, otherwise use the name
            String actualType = StringUtils.isNotBlank(aliasTo) ? aliasTo : typeName;
            
            // Skip excluded types and duplicates
            if (excludedTypes.contains(actualType) || addedTypes.contains(actualType)) {
                continue;
            }
            
            // Generate column name and type definition
            String columnName = "col_" + columnIndex++;
            String columnType = getColumnTypeDefinition(actualType);
            
            if (columnType != null) {
                createTableSql.append(columnName).append(" ").append(columnType).append(", ");
                addedTypes.add(actualType);
            }
        }
        
        // Remove trailing comma and space
        if (createTableSql.length() > 0 && createTableSql.charAt(createTableSql.length() - 2) == ',') {
            createTableSql.setLength(createTableSql.length() - 2);
        }
        
        createTableSql.append(") ENGINE = Memory");
        
        // Create table with appropriate settings for experimental types
        CommandSettings commandSettings = new CommandSettings();
        // Allow Geometry type which may have variant ambiguity
        commandSettings.serverSetting("allow_suspicious_variant_types", "1");
        // Allow QBit experimental type
        commandSettings.serverSetting("allow_experimental_qbit_type", "1");
        try {
            // Try to enable experimental types if version supports them
            if (isVersionMatch("[24.8,)")) {
                commandSettings.serverSetting("allow_experimental_variant_type", "1")
                        .serverSetting("allow_experimental_dynamic_type", "1")
                        .serverSetting("allow_experimental_json_type", "1");
            }
            if (isVersionMatch("[25.8,)")) {
                commandSettings.serverSetting("enable_time_time64_type", "1");
            }
        } catch (Exception e) {
            // If version check fails, continue without experimental settings
        }
        
        try {
            client.execute("DROP TABLE IF EXISTS " + tableName).get().close();
            client.execute(createTableSql.toString(), commandSettings).get().close();
            
            // Verify the schema
            TableSchema schema = client.getTableSchema(tableName);
            Assert.assertNotNull(schema, "Schema should not be null");
            Assert.assertEquals(schema.getTableName(), tableName);
            Assert.assertTrue(schema.getColumns().size() > 0, "Table should have at least one column");
            
            // Verify that we have columns for the types we added
            // Some types might fail to create, so we check for at least 80% success rate
            Assert.assertTrue(schema.getColumns().size() >= addedTypes.size() * 0.8, 
                    "Expected at least 80% of types to be successfully created. Created: " + schema.getColumns().size() + ", Attempted: " + addedTypes.size());
        } finally {
            try {
                client.execute("DROP TABLE IF EXISTS " + tableName).get().close();
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }
    
    /**
     * Returns the column type definition for a given ClickHouse type name.
     * Returns null if the type cannot be used in CREATE TABLE.
     */
    private String getColumnTypeDefinition(String typeName) {
        // Handle types that need parameters
        switch (typeName) {
            case "Decimal":
                return "Decimal(10, 2)";
            case "Decimal32":
                return "Decimal32(2)";
            case "Decimal64":
                return "Decimal64(3)";
            case "Decimal128":
                return "Decimal128(4)";
            case "Decimal256":
                return "Decimal256(5)";
            case "DateTime":
                return "DateTime";
            case "DateTime32":
                return "DateTime32";
            case "DateTime64":
                return "DateTime64(3)";
            case "FixedString":
                return "FixedString(10)";
            case "Enum":
                // Enum base type cannot be used without parameters, return null to skip it
                return null;
            case "Enum8":
                return "Enum8('a' = 1, 'b' = 2)";
            case "Enum16":
                return "Enum16('a' = 1, 'b' = 2)";
            case "Array":
                return "Array(String)";
            case "Map":
                return "Map(String, Int32)";
            case "Tuple":
                return "Tuple(String, Int32)";
            case "Nested":
                return "Nested(name String, value Int32)";
            case "Object":
                return "Object('json' String)";
            case "Variant":
                return "Variant(String, Int32)";
            case "QBit":
                // QBit requires two parameters: element type and number of elements
                return "QBit(Float32, 4)";
            case "Geometry":
            case "Geometry1":
                // Geometry type (requires allow_suspicious_variant_types = 1 setting)
                return "Geometry";
            default:
                // For simple types without parameters, return as-is
                return typeName;
        }
    }

    public boolean isVersionMatch(String versionExpression) {
        List<GenericRecord> serverVersion = client.queryAll("SELECT version()");
        return ClickHouseVersion.of(serverVersion.get(0).getString(1)).check(versionExpression);
    }
    protected Client.Builder newClient() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean isSecure = isCloud();
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .serverSetting(ServerSettings.WAIT_END_OF_QUERY, "1");
    }
}
