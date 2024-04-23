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
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
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
                .addUsername("default")
                .addPassword("")
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

        try (ClickHouseClient client = ClickHouseClient.builder().config(new ClickHouseConfig())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build()) {

            ClickHouseRequest request = client.read(getServer(ClickHouseProtocol.HTTP))
                    .query("CREATE TABLE IF NOT EXISTS default." + tableName + " (param1 UInt32, param2 UInt16) ENGINE = Memory");
            request.executeAndWait();

            request = client.write(getServer(ClickHouseProtocol.HTTP))
                    .query("INSERT INTO default." + tableName + " VALUES (1, 2), (3, 4), (5, 6)");
            request.executeAndWait();


            if (false) {
                // debug check
                ClickHouseResponse response = client.read(getServer(ClickHouseProtocol.HTTP))
                        .query("SELECT * FROM default." + tableName + " ORDER BY param1 ASC")
                        .executeAndWait();

                Iterator<ClickHouseRecord> iter = response.records().iterator();
                while (iter.hasNext()) {
                    ClickHouseRecord r = iter.next();
                    if (r.size() > 0) {
                        System.out.println(r.getValue(0).asString());
                    }
                }
            }
        } catch (Exception e) {
            Assert.fail("Failed to prepare data set", e);
        }
    }
}
