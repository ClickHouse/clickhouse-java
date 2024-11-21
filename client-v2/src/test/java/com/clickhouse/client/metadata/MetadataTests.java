package com.clickhouse.client.metadata;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.metadata.DefaultColumnToMethodMatchingStrategy;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
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
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        client = new Client.Builder()
                .addEndpoint(node.getBaseUri() + "default")
                .setUsername("default")
                .setPassword("")
                .build();
    }

    @Test(groups = { "integration" })
    public void testGetTableSchema() {
        prepareDataSet("describe_table");

        TableSchema schema = client.getTableSchema("describe_table", "default");

        Assert.assertEquals(schema.getTableName(), "describe_table");
        Assert.assertEquals(schema.getDatabaseName(), "default");

        Assert.assertEquals(schema.getColumns().size(), 2);

        List<ClickHouseColumn> columns = schema.getColumns();
        Assert.assertEquals(columns.get(0).getColumnName(), "param1");
        Assert.assertEquals(columns.get(0).getDataType().name(), "UInt32");
    }

    private void prepareDataSet(String tableName) {

        try {
            String sql = "CREATE TABLE IF NOT EXISTS default." + tableName + " (param1 UInt32, param2 UInt16) ENGINE = Memory";
            client.execute(sql).get();

            sql = "INSERT INTO default." + tableName + " VALUES (1, 2), (3, 4), (5, 6)";
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
}
