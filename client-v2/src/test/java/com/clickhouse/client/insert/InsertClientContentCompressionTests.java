package com.clickhouse.client.insert;

import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class InsertClientContentCompressionTests extends InsertTests {
    public InsertClientContentCompressionTests() {
        super(true, false);
    }


    @Test(groups = { "integration" })
    public void testInsertAndReadBackWithSecureConnection() {
        if (isCloud()) {
            return;
        }
        ClickHouseNode secureServer = getSecureServer(ClickHouseProtocol.HTTP);

        try (Client client = new Client.Builder()
                .addEndpoint("https://localhost:" + secureServer.getPort())
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setRootCertificate("containers/clickhouse-server/certs/localhost.crt")
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .compressClientRequest(true)
                .build()) {
            final String tableName = "single_pojo_table";
            final String createSQL = SamplePOJO.generateTableCreateSQL(tableName);
            final SamplePOJO pojo = new SamplePOJO();

            initTable(tableName, createSQL);

            client.register(SamplePOJO.class, client.getTableSchema(tableName));
            InsertSettings settings = new InsertSettings()
                    .setDeduplicationToken(RandomStringUtils.randomAlphabetic(36))
                    .setQueryId(String.valueOf(UUID.randomUUID()));
            System.out.println("Inserting POJO: " + pojo);
            try (InsertResponse response = client.insert(tableName, Collections.singletonList(pojo), settings).get(10, TimeUnit.SECONDS)) {
                Assert.assertEquals(response.getWrittenRows(), 1);
            }

            try (QueryResponse queryResponse =
                         client.query("SELECT * FROM " + tableName + " LIMIT 1").get(10, TimeUnit.SECONDS)) {

                ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(queryResponse);
                Assert.assertNotNull(reader.next());

                Assert.assertEquals(reader.getByte("byteValue"), pojo.getByteValue());
                Assert.assertEquals(reader.getByte("int8"), pojo.getInt8());
                Assert.assertEquals(reader.getShort("uint8"), pojo.getUint8());
                Assert.assertEquals(reader.getShort("int16"), pojo.getInt16());
                Assert.assertEquals(reader.getInteger("int32"), pojo.getInt32());
                Assert.assertEquals(reader.getLong("int64"), pojo.getInt64());
                Assert.assertEquals(reader.getFloat("float32"), pojo.getFloat32());
                Assert.assertEquals(reader.getDouble("float64"), pojo.getFloat64());
                Assert.assertEquals(reader.getString("string"), pojo.getString());
                Assert.assertEquals(reader.getString("fixedString"), pojo.getFixedString());
            }
            List<GenericRecord> records = client.queryAll("SELECT timezone()");
            Assert.assertTrue(records.size() > 0);
            Assert.assertEquals(records.get(0).getString(1), "UTC");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }
}
