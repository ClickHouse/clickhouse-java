package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import lombok.Data;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;

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

    @Test(groups = {"integration"})
    void testGetObjectArray1D() {
        final String table = "test_get_object_array_1d";
        try (Client client = newClient().build()) {
            client.execute("DROP TABLE IF EXISTS " + table);
            client.execute("CREATE TABLE " + table + " (" +
                    "uint64_arr Array(UInt64), " +
                    "enum_arr Array(Enum8('abc' = 1, 'cde' = 2)), " +
                    "dt_arr Array(DateTime('UTC')), " +
                    "fstr_arr Array(FixedString(4)), " +
                    "str_arr Array(String), " +
                    "int_arr Array(Int32)" +
                    ") ENGINE = MergeTree() ORDER BY tuple()");

            client.execute("INSERT INTO " + table + " VALUES (" +
                    "[100, 200, 18000044073709551615], " +
                    "['abc', 'cde'], " +
                    "['2030-10-09 08:07:06', '2031-10-09 08:07:06'], " +
                    "['abcd', 'efgh'], " +
                    "['hello', 'world'], " +
                    "[100, 200, 65536])");

            QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.RowBinary);
            TableSchema schema = client.getTableSchema(table);
            try (QueryResponse response = client.query("SELECT * FROM " + table, settings).get()) {
                ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response, schema);
                reader.next();

                // Array(UInt64) -> BigInteger elements
                Object[] uint64Arr = reader.getObjectArray("uint64_arr");
                Assert.assertNotNull(uint64Arr);
                Assert.assertEquals(uint64Arr.length, 3);
                Assert.assertEquals(uint64Arr[0], BigInteger.valueOf(100));
                Assert.assertEquals(uint64Arr[1], BigInteger.valueOf(200));
                Assert.assertEquals(uint64Arr[2], new BigInteger("18000044073709551615"));

                // Array(Enum8) -> EnumValue elements via getObjectArray
                Object[] enumArr = reader.getObjectArray("enum_arr");
                Assert.assertNotNull(enumArr);
                Assert.assertEquals(enumArr.length, 2);
                Assert.assertTrue(enumArr[0] instanceof BinaryStreamReader.EnumValue);
                Assert.assertEquals(enumArr[0].toString(), "abc");
                Assert.assertEquals(enumArr[1].toString(), "cde");

                // Array(Enum8) -> String[] via getStringArray
                String[] enumStrings = reader.getStringArray("enum_arr");
                Assert.assertEquals(enumStrings, new String[]{"abc", "cde"});

                // Array(DateTime) -> ZonedDateTime elements
                Object[] dtArr = reader.getObjectArray("dt_arr");
                Assert.assertNotNull(dtArr);
                Assert.assertEquals(dtArr.length, 2);
                Assert.assertTrue(dtArr[0] instanceof ZonedDateTime);
                Assert.assertTrue(dtArr[1] instanceof ZonedDateTime);
                ZonedDateTime zdt1 = (ZonedDateTime) dtArr[0];
                ZonedDateTime zdt2 = (ZonedDateTime) dtArr[1];
                Assert.assertEquals(zdt1.getYear(), 2030);
                Assert.assertEquals(zdt1.getMonthValue(), 10);
                Assert.assertEquals(zdt1.getDayOfMonth(), 9);
                Assert.assertEquals(zdt2.getYear(), 2031);

                // Array(FixedString(4)) -> String elements
                Object[] fstrArr = reader.getObjectArray("fstr_arr");
                Assert.assertNotNull(fstrArr);
                Assert.assertEquals(fstrArr.length, 2);
                Assert.assertEquals(fstrArr[0], "abcd");
                Assert.assertEquals(fstrArr[1], "efgh");

                // Array(String) -> String elements
                Object[] strArr = reader.getObjectArray("str_arr");
                Assert.assertNotNull(strArr);
                Assert.assertEquals(strArr[0], "hello");
                Assert.assertEquals(strArr[1], "world");

                // getStringArray should also work for FixedString arrays
                String[] fstrStrings = reader.getStringArray("fstr_arr");
                Assert.assertEquals(fstrStrings, new String[]{"abcd", "efgh"});

                // Array(Int32)
                Object[] intArrObj = reader.getObjectArray("int_arr");
                Assert.assertEquals(intArrObj, new Integer[]{100, 200, 65536});

                Assert.assertNull(reader.next(), "Expected only one row");
            }
        } catch (Exception e) {
            Assert.fail("Unexpected exception", e);
        }
    }

    @Test(groups = {"integration"})
    void testGetObjectArray2D() {
        final String table = "test_get_object_array_2d";
        try (Client client = newClient().build()) {
            client.execute("DROP TABLE IF EXISTS " + table);
            client.execute("CREATE TABLE " + table + " (" +
                    "arr2d_int Array(Array(Int64)), " +
                    "arr2d_str Array(Array(String))" +
                    ") ENGINE = MergeTree() ORDER BY tuple()");

            client.execute("INSERT INTO " + table + " VALUES (" +
                    "[[1, 2, 3], [4, 5]], " +
                    "[['hello', 'world'], ['foo']])");

            QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.RowBinary);
            TableSchema schema = client.getTableSchema(table);
            try (QueryResponse response = client.query("SELECT * FROM " + table, settings).get()) {
                ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response, schema);
                reader.next();

                // Array(Array(Int64)) -> nested Object[]
                Object[] arr2dInt = reader.getObjectArray("arr2d_int");
                Assert.assertNotNull(arr2dInt);
                Assert.assertEquals(arr2dInt.length, 2);
                Assert.assertTrue(arr2dInt[0] instanceof Object[]);
                Assert.assertTrue(arr2dInt[1] instanceof Object[]);

                Object[] inner0 = (Object[]) arr2dInt[0];
                Assert.assertEquals(inner0.length, 3);
                Assert.assertEquals(inner0[0], 1L);
                Assert.assertEquals(inner0[1], 2L);
                Assert.assertEquals(inner0[2], 3L);

                Object[] inner1 = (Object[]) arr2dInt[1];
                Assert.assertEquals(inner1.length, 2);
                Assert.assertEquals(inner1[0], 4L);
                Assert.assertEquals(inner1[1], 5L);

                // Array(Array(String)) -> nested Object[]
                Object[] arr2dStr = reader.getObjectArray("arr2d_str");
                Assert.assertNotNull(arr2dStr);
                Assert.assertEquals(arr2dStr.length, 2);

                Object[] strInner0 = (Object[]) arr2dStr[0];
                Assert.assertEquals(strInner0, new Object[]{"hello", "world"});

                Object[] strInner1 = (Object[]) arr2dStr[1];
                Assert.assertEquals(strInner1, new Object[]{"foo"});

                Assert.assertNull(reader.next(), "Expected only one row");
            }
        } catch (Exception e) {
            Assert.fail("Unexpected exception", e);
        }
    }

    @Test(groups = {"integration"})
    void testGetObjectArray3D() {
        final String table = "test_get_object_array_3d";
        try (Client client = newClient().build()) {
            client.execute("DROP TABLE IF EXISTS " + table);
            client.execute("CREATE TABLE " + table + " (" +
                    "arr3d Array(Array(Array(Int32)))" +
                    ") ENGINE = MergeTree() ORDER BY tuple()");

            client.execute("INSERT INTO " + table + " VALUES (" +
                    "[[[1, 2], [3]], [[4, 5, 6]]])");

            QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.RowBinary);
            TableSchema schema = client.getTableSchema(table);
            try (QueryResponse response = client.query("SELECT * FROM " + table, settings).get()) {
                ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response, schema);
                reader.next();

                // Array(Array(Array(Int32))) -> 3-level nested Object[]
                Object[] arr3d = reader.getObjectArray("arr3d");
                Assert.assertNotNull(arr3d);
                Assert.assertEquals(arr3d.length, 2);

                // dim1[0] = [[1, 2], [3]]
                Assert.assertTrue(arr3d[0] instanceof Object[]);
                Object[] dim1_0 = (Object[]) arr3d[0];
                Assert.assertEquals(dim1_0.length, 2);

                Assert.assertTrue(dim1_0[0] instanceof Object[]);
                Object[] dim2_0_0 = (Object[]) dim1_0[0];
                Assert.assertEquals(dim2_0_0.length, 2);
                Assert.assertEquals(dim2_0_0[0], 1);
                Assert.assertEquals(dim2_0_0[1], 2);

                Assert.assertTrue(dim1_0[1] instanceof Object[]);
                Object[] dim2_0_1 = (Object[]) dim1_0[1];
                Assert.assertEquals(dim2_0_1.length, 1);
                Assert.assertEquals(dim2_0_1[0], 3);

                // dim1[1] = [[4, 5, 6]]
                Assert.assertTrue(arr3d[1] instanceof Object[]);
                Object[] dim1_1 = (Object[]) arr3d[1];
                Assert.assertEquals(dim1_1.length, 1);

                Assert.assertTrue(dim1_1[0] instanceof Object[]);
                Object[] dim2_1_0 = (Object[]) dim1_1[0];
                Assert.assertEquals(dim2_1_0.length, 3);
                Assert.assertEquals(dim2_1_0[0], 4);
                Assert.assertEquals(dim2_1_0[1], 5);
                Assert.assertEquals(dim2_1_0[2], 6);

                Assert.assertNull(reader.next(), "Expected only one row");
            }
        } catch (Exception e) {
            Assert.fail("Unexpected exception", e);
        }
    }

    @Test(groups = {"integration"})
    void testGetObjectArrayWithEmptyArrays() {
        final String table = "test_get_object_array_empty";
        try (Client client = newClient().build()) {
            client.execute("DROP TABLE IF EXISTS " + table);
            client.execute("CREATE TABLE " + table + " (" +
                    "empty_arr Array(Int32), " +
                    "empty_2d Array(Array(String))" +
                    ") ENGINE = MergeTree() ORDER BY tuple()");

            client.execute("INSERT INTO " + table + " VALUES ([], [])");

            QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.RowBinary);
            TableSchema schema = client.getTableSchema(table);
            try (QueryResponse response = client.query("SELECT * FROM " + table, settings).get()) {
                ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response, schema);
                reader.next();

                Object[] emptyArr = reader.getObjectArray("empty_arr");
                Assert.assertNotNull(emptyArr);
                Assert.assertEquals(emptyArr.length, 0);

                Object[] empty2d = reader.getObjectArray("empty_2d");
                Assert.assertNotNull(empty2d);
                Assert.assertEquals(empty2d.length, 0);

                Assert.assertNull(reader.next(), "Expected only one row");
            }
        } catch (Exception e) {
            Assert.fail("Unexpected exception", e);
        }
    }

    @Test(groups = {"integration"})
    void testGetObjectArrayMultipleRows() {
        final String table = "test_get_object_array_multi_row";
        try (Client client = newClient().build()) {
            client.execute("DROP TABLE IF EXISTS " + table);
            client.execute("CREATE TABLE " + table + " (" +
                    "id UInt32, " +
                    "arr Array(UInt64)" +
                    ") ENGINE = MergeTree() ORDER BY id");

            client.execute("INSERT INTO " + table + " VALUES " +
                    "(1, [100, 200]), " +
                    "(2, [300]), " +
                    "(3, [])");

            QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.RowBinary);
            TableSchema schema = client.getTableSchema(table);
            try (QueryResponse response = client.query("SELECT * FROM " + table + " ORDER BY id", settings).get()) {
                ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response, schema);

                // Row 1
                reader.next();
                Object[] arr1 = reader.getObjectArray("arr");
                Assert.assertEquals(arr1.length, 2);
                Assert.assertEquals(arr1[0], BigInteger.valueOf(100));
                Assert.assertEquals(arr1[1], BigInteger.valueOf(200));

                // Row 2
                reader.next();
                Object[] arr2 = reader.getObjectArray("arr");
                Assert.assertEquals(arr2.length, 1);
                Assert.assertEquals(arr2[0], BigInteger.valueOf(300));

                // Row 3
                reader.next();
                Object[] arr3 = reader.getObjectArray("arr");
                Assert.assertEquals(arr3.length, 0);

                Assert.assertNull(reader.next(), "Expected only three rows");
            }
        } catch (Exception e) {
            Assert.fail("Unexpected exception", e);
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
