package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.Records;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Test(groups = {"integration"})
public class BinaryReaderBackedRecordTest extends BaseIntegrationTest {

    private Client client;

    @BeforeMethod(groups = {"integration"})
    public void setUp() {
        client = newClient().build();
    }

    @AfterMethod(groups = {"integration"})
    public void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    private Client.Builder newClient() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase());
    }

    @Test(groups = {"integration"})
    public void testGetObjectArray() throws Exception {
        final String table = "test_binary_reader_backed_get_object_array";
        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute("CREATE TABLE " + table + " (" +
                "rowId Int32, " +
                "uint64_arr Array(UInt64), " +
                "enum_arr Array(Enum8('abc' = 1, 'cde' = 2, 'xyz' = 3)), " +
                "dt_arr Array(DateTime('UTC')), " +
                "str_arr Array(String), " +
                "int_arr Array(Int32), " +
                "arr2d Array(Array(Int64)), " +
                "arr3d Array(Array(Array(Int32)))" +
                ") Engine = MergeTree ORDER BY rowId").get();

        client.execute("INSERT INTO " + table + " VALUES " +
                "(1, " +
                "[100, 200], " +
                "['abc', 'cde'], " +
                "['2030-10-09 08:07:06', '2031-10-09 08:07:06'], " +
                "['hello', 'world'], " +
                "[10, 20, 30], " +
                "[[1, 2, 3], [4, 5]], " +
                "[[[1, 2], [3]], [[4, 5, 6]]]" +
                ")").get();

        List<GenericRecord> records = new ArrayList<>();
        try (Records rs = client.queryRecords("SELECT * FROM " + table + " ORDER BY rowId").get(10, TimeUnit.SECONDS)) {
            for (GenericRecord record : rs) {
                records.add(record);
            }
        }

        Assert.assertEquals(records.size(), 1);
        GenericRecord row = records.get(0);
        Assert.assertTrue(row instanceof BinaryReaderBackedRecord);

        // Array(UInt64) -> getObjectArray returns BigInteger[]
        Object[] uint64Arr = row.getObjectArray("uint64_arr");
        Assert.assertNotNull(uint64Arr);
        Assert.assertEquals(uint64Arr.length, 2);
        Assert.assertEquals(uint64Arr[0], BigInteger.valueOf(100));
        Assert.assertEquals(uint64Arr[1], BigInteger.valueOf(200));

        // Array(Enum8) -> getObjectArray returns EnumValue[]
        Object[] enumArr = row.getObjectArray("enum_arr");
        Assert.assertNotNull(enumArr);
        Assert.assertEquals(enumArr.length, 2);
        Assert.assertEquals(enumArr[0].toString(), "abc");
        Assert.assertEquals(enumArr[1].toString(), "cde");

        // Array(DateTime) -> getObjectArray returns ZonedDateTime[]
        Object[] dtArr = row.getObjectArray("dt_arr");
        Assert.assertNotNull(dtArr);
        Assert.assertEquals(dtArr.length, 2);
        Assert.assertTrue(dtArr[0] instanceof ZonedDateTime);
        ZonedDateTime zdt1 = (ZonedDateTime) dtArr[0];
        Assert.assertEquals(zdt1.getYear(), 2030);
        Assert.assertEquals(zdt1.getMonthValue(), 10);

        // Array(String) -> getObjectArray returns String[]
        Object[] strArr = row.getObjectArray("str_arr");
        Assert.assertNotNull(strArr);
        Assert.assertEquals(strArr[0], "hello");
        Assert.assertEquals(strArr[1], "world");

        // Array(Int32) -> getObjectArray returns boxed Integer[]
        Object[] intArr = row.getObjectArray("int_arr");
        Assert.assertNotNull(intArr);
        Assert.assertEquals(intArr.length, 3);
        Assert.assertEquals(intArr[0], 10);
        Assert.assertEquals(intArr[1], 20);
        Assert.assertEquals(intArr[2], 30);

        // Array(Array(Int64)) 2D -> getObjectArray returns nested Object[]
        Object[] arr2d = row.getObjectArray("arr2d");
        Assert.assertNotNull(arr2d);
        Assert.assertEquals(arr2d.length, 2);
        Assert.assertTrue(arr2d[0] instanceof Object[]);
        Object[] inner0 = (Object[]) arr2d[0];
        Assert.assertEquals(inner0.length, 3);
        Assert.assertEquals(inner0[0], 1L);
        Assert.assertEquals(inner0[1], 2L);
        Assert.assertEquals(inner0[2], 3L);

        // Array(Array(Array(Int32))) 3D -> getObjectArray returns 3-level nested Object[]
        Object[] arr3d = row.getObjectArray("arr3d");
        Assert.assertNotNull(arr3d);
        Assert.assertEquals(arr3d.length, 2);
        Object[] dim1_0 = (Object[]) arr3d[0];
        Assert.assertEquals(dim1_0.length, 2);
        Object[] dim2_0_0 = (Object[]) dim1_0[0];
        Assert.assertEquals(dim2_0_0[0], 1);
        Assert.assertEquals(dim2_0_0[1], 2);
    }

    @Test(groups = {"integration"})
    public void testGetObjectArrayEmptyAndEdgeCases() throws Exception {
        final String table = "test_binary_reader_backed_get_object_array_empty";
        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute("CREATE TABLE " + table + " (" +
                "rowId Int32, " +
                "empty_arr Array(Int32), " +
                "single_arr Array(String), " +
                "arr2d_empty Array(Array(Int64))" +
                ") Engine = MergeTree ORDER BY rowId").get();

        client.execute("INSERT INTO " + table + " VALUES (1, [], ['single'], [[]])").get();

        List<GenericRecord> records = new ArrayList<>();
        try (Records rs = client.queryRecords("SELECT * FROM " + table).get(10, TimeUnit.SECONDS)) {
            for (GenericRecord record : rs) {
                records.add(record);
            }
        }

        Assert.assertEquals(records.size(), 1);
        GenericRecord row = records.get(0);

        // Empty array
        Object[] emptyArr = row.getObjectArray("empty_arr");
        Assert.assertNotNull(emptyArr);
        Assert.assertEquals(emptyArr.length, 0);

        // Single-element array
        Object[] singleArr = row.getObjectArray("single_arr");
        Assert.assertNotNull(singleArr);
        Assert.assertEquals(singleArr.length, 1);
        Assert.assertEquals(singleArr[0], "single");

        // 2D with inner empty: [[]]
        Object[] arr2dEmpty = row.getObjectArray("arr2d_empty");
        Assert.assertNotNull(arr2dEmpty);
        Assert.assertEquals(arr2dEmpty.length, 1);
        Assert.assertTrue(arr2dEmpty[0] instanceof Object[]);
        Assert.assertEquals(((Object[]) arr2dEmpty[0]).length, 0);
    }
}