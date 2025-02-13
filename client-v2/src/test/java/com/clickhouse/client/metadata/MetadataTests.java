package com.clickhouse.client.metadata;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.metadata.DefaultColumnToMethodMatchingStrategy;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;

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

    protected Client.Builder newClient() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean isSecure = isCloud();
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .serverSetting(ServerSettings.WAIT_END_OF_QUERY, "1")
                .useNewImplementation(System.getProperty("client.tests.useNewImplementation", "true").equals("true"));
    }
}
