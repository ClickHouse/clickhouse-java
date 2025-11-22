package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.query.GenericRecord;
import lombok.Data;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Random;

@Test(groups = {"integration"})
public class RowBinaryTest extends BaseIntegrationTest {

    protected Client.Builder newClient() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean isSecure = isCloud();
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase());
    }


    @Test(groups = {"integration"})
    void testDefaultWithFunction() {
        final String table = "test_defaults_with_function";
        final String createTable = "CREATE TABLE " + table +" ( " +
                "   name String," +
                "   v Int64 DEFAULT 10, " +
                "   fingerPrint UInt64 DEFAULT xxHash64(name)," +
                "   comments String" +
                ") ENGINE = MergeTree()" +
                "ORDER BY name;";

        try (Client client = newClient().build()){

            client.execute("DROP TABLE IF EXISTS " + table);
            client.execute(createTable);

            client.register(DefaultWithFunctionPojo.class, client.getTableSchema(table));

            DefaultWithFunctionPojo entity = new DefaultWithFunctionPojo();
            entity.setName("test");
            entity.setComments("test");
            List<DefaultWithFunctionPojo> data = Collections.singletonList(entity);
            client.insert(table, data);

            List<GenericRecord> records = client.queryAll("SELECT * FROM " + table);
            Assert.assertEquals(records.size(), 1);
            GenericRecord record = records.get(0);
            Assert.assertEquals(record.getString("name"), "test");
            Assert.assertEquals(record.getLong("v"), 10);
            Assert.assertTrue(record.getLong("fingerPrint") > 0);
            Assert.assertEquals(record.getString("comments"), "test");
        }
    }

    @Data
    public static class DefaultWithFunctionPojo {
        private String name;
        private Long fingerPrint;
        private Long v;
        private String comments;
    }
}
