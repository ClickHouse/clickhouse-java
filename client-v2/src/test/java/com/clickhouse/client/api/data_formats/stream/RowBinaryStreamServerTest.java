package com.clickhouse.client.api.data_formats.stream;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseVersion;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Inserts rows with {@link RowBinaryStreamWriter} and reads them back from a server.
 */
@Test(groups = {"integration"})
public class RowBinaryStreamServerTest extends BaseIntegrationTest {

    private static final int TIMEOUT_SECONDS = 30;

    private Client client;

    @BeforeMethod(groups = {"integration"})
    public void setUp() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .compressServerResponse(false)
                .compressClientRequest(false)
                .useHttpCompression(false)
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .serverSetting(ServerSettings.ASYNC_INSERT, "0")
                .serverSetting(ServerSettings.WAIT_END_OF_QUERY, "1")
                .build();
    }

    @AfterMethod(groups = {"integration"})
    public void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @Test(groups = {"integration"})
    public void scalarsAndNullableString() throws Exception {
        List<GenericRecord> rows = insert("stream_scalars",
                "id Int32, name Nullable(String), tail Float64", writer -> {
                    try (ValueWriter row = writer.beginRow()) {
                        row.writeInt32(1);
                        row.writeString((String) null);
                        row.writeFloat64(2.5d);
                    }
                    try (ValueWriter row = writer.beginRow()) {
                        row.writeInt32(2);
                        row.writeString("ok");
                        row.writeFloat64(3.5d);
                    }
                });
        Assert.assertEquals(rows.get(0).getInteger("id"), 1);
        Assert.assertNull(rows.get(0).getObject("name"));
        Assert.assertEquals(rows.get(0).getDouble("tail"), 2.5d);
        Assert.assertEquals(rows.get(1).getInteger("id"), 2);
        Assert.assertEquals(rows.get(1).getString("name"), "ok");
        Assert.assertEquals(rows.get(1).getDouble("tail"), 3.5d);
    }

    @Test(groups = {"integration"})
    public void arrayTupleMapAndNested() throws Exception {
        List<GenericRecord> arrays = insert("stream_array",
                "id Int32, items Array(Nullable(String)), tail Float64", writer -> {
                    try (ValueWriter row = writer.beginRow()) {
                        row.writeInt32(1);
                        try (ValueWriter items = row.beginArray(3)) {
                            items.writeString("a");
                            items.writeString((String) null);
                            items.writeString("b");
                        }
                        row.writeFloat64(1.5d);
                    }
                });
        Assert.assertEquals(asList(arrays.get(0).getObject("items")), Arrays.asList("a", null, "b"));
        Assert.assertEquals(arrays.get(0).getDouble("tail"), 1.5d);

        List<GenericRecord> tuples = insert("stream_tuple",
                "id Int32, t Tuple(Int32, Array(Nullable(String)), Float64), tail Float64", writer -> {
                    try (ValueWriter row = writer.beginRow()) {
                        row.writeInt32(1);
                        try (ValueWriter tuple = row.beginTuple()) {
                            tuple.writeInt32(9);
                            try (ValueWriter values = tuple.beginArray(2)) {
                                values.writeString("x");
                                values.writeString((String) null);
                            }
                            tuple.writeFloat64(0.5d);
                        }
                        row.writeFloat64(1.5d);
                    }
                });
        Object[] tuple = (Object[]) tuples.get(0).getObject("t");
        Assert.assertEquals(tuple[0], 9);
        Assert.assertEquals(asList(tuple[1]), Arrays.asList("x", null));
        Assert.assertEquals(((Number) tuple[2]).doubleValue(), 0.5d);

        List<GenericRecord> maps = insert("stream_map",
                "id Int32, m Map(String, Array(Int32)), tail Float64", writer -> {
                    try (ValueWriter row = writer.beginRow()) {
                        row.writeInt32(1);
                        try (ValueWriter map = row.beginMap(2)) {
                            map.writeString("a");
                            try (ValueWriter values = map.beginArray(2)) {
                                values.writeInt32(1);
                                values.writeInt32(2);
                            }
                            map.writeString("b");
                            try (ValueWriter values = map.beginArray(0)) {
                            }
                        }
                        row.writeFloat64(1.5d);
                    }
                });
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) maps.get(0).getObject("m");
        Assert.assertEquals(asList(map.get("a")), Arrays.asList(1, 2));
        Assert.assertEquals(asList(map.get("b")), Arrays.asList());

        List<GenericRecord> nested = insert("stream_nested",
                "id Int32, n Nested(n1 Int32, n2 Nullable(String)), tail Float64", writer -> {
                    try (ValueWriter row = writer.beginRow()) {
                        row.writeInt32(1);
                        try (ValueWriter rows = row.beginNested(2)) {
                            try (ValueWriter inner = rows.beginTuple()) {
                                inner.writeInt32(5);
                                inner.writeString("a");
                            }
                            try (ValueWriter inner = rows.beginTuple()) {
                                inner.writeInt32(6);
                                inner.writeString((String) null);
                            }
                        }
                        row.writeFloat64(1.5d);
                    }
                });
        List<?> rows = asList(nested.get(0).getObject("n"));
        Assert.assertEquals(((Object[]) rows.get(0))[0], 5);
        Assert.assertEquals(((Object[]) rows.get(0))[1], "a");
        Assert.assertEquals(((Object[]) rows.get(1))[0], 6);
        Assert.assertNull(((Object[]) rows.get(1))[1]);
    }

    @Test(groups = {"integration"})
    public void binaryJsonWithNestedObjectsNullsAndArrays() throws Exception {
        if (jsonUnsupported()) {
            return;
        }
        List<GenericRecord> rows = insert("stream_json", "id Int32, doc JSON", writer -> {
            try (ValueWriter row = writer.beginRow()) {
                row.writeInt32(1);
                try (ValueWriter doc = row.beginJson()) {
                    doc.writeString("name", "ada");
                    try (ValueWriter address = doc.beginObject("address", 2)) {
                        address.writeString("city", "Berlin");
                        address.writeNull("zip");
                    }
                    try (ValueWriter tags = doc.beginArray("tags", ClickHouseColumn.of("e", "Nullable(String)"), 3)) {
                        tags.writeString("a");
                        tags.writeNull();
                        tags.writeString("b");
                    }
                    try (ValueWriter items = doc.beginArray("items", ClickHouseColumn.of("e", "JSON"), 2)) {
                        try (ValueWriter first = items.beginJson(1)) {
                            first.writeInt64("a", 100L);
                        }
                        try (ValueWriter second = items.beginJson(1)) {
                            second.writeString("b", "name");
                        }
                    }
                }
            }
            try (ValueWriter row = writer.beginRow()) {
                row.writeInt32(2);
                try (ValueWriter doc = row.beginJson(0)) {
                }
            }
        });

        Map<String, Object> doc = jsonMap(rows.get(0).getObject("doc"));
        Assert.assertEquals(doc.get("name"), "ada");
        Assert.assertEquals(doc.get("address.city"), "Berlin");
        Assert.assertEquals(asList(doc.get("tags")), Arrays.asList("a", null, "b"));
        List<?> items = asList(doc.get("items"));
        Assert.assertEquals(items.size(), 2);
        Assert.assertEquals(jsonMap(items.get(0)).get("a"), 100L);
        Assert.assertEquals(jsonMap(items.get(1)).get("b"), "name");
        Assert.assertTrue(jsonMap(rows.get(1).getObject("doc")).isEmpty());

        List<GenericRecord> fields = client.queryAll(
                "SELECT doc.address.city AS city, doc.address.zip AS zip, toJSONString(doc) AS text "
                        + "FROM stream_json WHERE id = 1");
        Assert.assertEquals(fields.get(0).getString("city"), "Berlin");
        Assert.assertNull(fields.get(0).getObject("zip"));
        String text = fields.get(0).getString("text");
        Assert.assertTrue(text.contains("\"city\":\"Berlin\""), text);
        Assert.assertTrue(text.contains("\"tags\":[\"a\",null,\"b\"]"), text);
        Assert.assertTrue(text.contains("\"items\":[{\"a\":100},{\"b\":\"name\"}]"), text);
    }

    @Test(groups = {"integration"})
    public void typedJsonPathsSkipTheDynamicTag() throws Exception {
        if (jsonUnsupported()) {
            return;
        }
        List<GenericRecord> rows = insert("stream_json_typed",
                "doc JSON(name String, count Int64)", writer -> {
                    try (ValueWriter row = writer.beginRow()) {
                        try (ValueWriter doc = row.beginJson(2)) {
                            doc.writeString("name", "ada");
                            doc.writeInt64("count", 7L);
                        }
                    }
                });
        Map<String, Object> doc = jsonMap(rows.get(0).getObject("doc"));
        Assert.assertEquals(doc.get("name"), "ada");
        Assert.assertEquals(doc.get("count"), 7L);
    }

    private List<GenericRecord> insert(String table, String columns, RowWriter write) throws Exception {
        CommandSettings command = new CommandSettings();
        command.serverSetting("allow_experimental_json_type", "1");
        client.execute("DROP TABLE IF EXISTS " + table, command).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        String nested = columns.contains("Nested(") ? " SETTINGS flatten_nested = 0" : "";
        client.execute("CREATE TABLE " + table + " (" + columns + ") ENGINE = MergeTree ORDER BY tuple()" + nested,
                command).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        TableSchema schema = client.getTableSchema(table);
        client.insert(table, out -> {
            RowBinaryStreamWriter writer = new RowBinaryStreamWriter(out, schema, ClickHouseFormat.RowBinary);
            write.accept(writer);
        }, ClickHouseFormat.RowBinary, new InsertSettings()).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return client.queryAll("SELECT * FROM " + table + " ORDER BY " + orderBy(columns));
    }

    private static String orderBy(String columns) {
        int comma = columns.indexOf(',');
        String first = comma < 0 ? columns : columns.substring(0, comma);
        String name = first.trim().split(" ")[0];
        if ("id".equals(name)) {
            return "id";
        }
        return "tuple()";
    }

    private boolean jsonUnsupported() throws Exception {
        if (isCloud()) {
            return true;
        }
        List<GenericRecord> version = client.queryAll("SELECT version()");
        return ClickHouseVersion.of(version.get(0).getString(1)).check("(,24.8]");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> jsonMap(Object value) {
        Assert.assertTrue(value instanceof Map, String.valueOf(value));
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> asList(Object value) {
        if (value instanceof BinaryStreamReader.ArrayValue) {
            return ((BinaryStreamReader.ArrayValue) value).asList();
        }
        if (value instanceof List) {
            return (List<Object>) value;
        }
        throw new AssertionError("Expected a list but was " + (value == null ? "null" : value.getClass().getName()));
    }

    private interface RowWriter {
        void accept(RowBinaryStreamWriter writer) throws IOException;
    }
}
