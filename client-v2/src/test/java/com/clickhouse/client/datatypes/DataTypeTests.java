package com.clickhouse.client.datatypes;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.data.ClickHouseDataType;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataTypeTests extends BaseIntegrationTest {

    private Client client;
    private InsertSettings settings;

    private boolean useClientCompression = false;

    private boolean useHttpCompression = false;

    private static final int EXECUTE_CMD_TIMEOUT = 10; // seconds

    public DataTypeTests(boolean useClientCompression, boolean useHttpCompression) {
        this.useClientCompression = useClientCompression;
        this.useHttpCompression = useHttpCompression;
    }

    public DataTypeTests() {
        this(false, false);
    }

    @BeforeMethod(groups = {"integration"})
    public void setUp() throws IOException {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), false)
                .setUsername("default")
                .setPassword("")
                .useNewImplementation(System.getProperty("client.tests.useNewImplementation", "true").equals("true"))
                .compressClientRequest(useClientCompression)
                .useHttpCompression(useHttpCompression)
                .build();
    }

    @AfterMethod(groups = { "integration" })
    public void tearDown() {
        client.close();
    }


    @Test
    public void testNestedDataTypes() throws Exception {
        final String table = "test_nested_types";
        String tblCreateSQL = NestedTypesDTO.tblCreateSQL(table);
        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute(tblCreateSQL);

        client.register(NestedTypesDTO.class, client.getTableSchema(table));

        List<NestedTypesDTO> data =
                Arrays.asList(new NestedTypesDTO(0, new Object[] {(short)127, "test 1"}, new Double[] {0.3d, 0.4d} ));
        client.insert(table, data);

        List<GenericRecord> rows = client.queryAll("SELECT * FROM " + table);
        for (GenericRecord row : rows) {
            NestedTypesDTO dto = data.get(row.getInteger("rowId"));
            Assert.assertEquals(row.getTuple("tuple1"), dto.getTuple1());
            Assert.assertEquals(row.getGeoPoint("point1").getValue(), dto.getPoint1());
        }

    }

    @Test
    public void testVariantWithSimpleDataTypes() throws Exception {
        final String table = "test_variant_primitives";
        final DataTypesTestingPOJO sample = new DataTypesTestingPOJO();
        System.out.println("sample: " + sample);

        dataTypesLoop:
        for (ClickHouseDataType dataType : ClickHouseDataType.values()) {
            System.out.println("Testing " + dataType);
            client.execute("DROP TABLE IF EXISTS " + table).get();
            StringBuilder b = new StringBuilder(" CREATE TABLE ");
            b.append(table).append(" ( rowId Int64, field Variant(String, ").append(dataType.name());

            switch (dataType) {
                case String:
                case FixedString:
                case IntervalYear:
                case IntervalDay:
                case IntervalHour:
                case IntervalWeek:
                case IntervalMonth:
                case IntervalMinute:
                case IntervalSecond:
                case IntervalNanosecond:
                case IntervalMicrosecond:
                case IntervalQuarter:
                case IntervalMillisecond:
                case Nothing:
                case Variant:
                case JSON:
                case Object:
                    // skipped
                    continue dataTypesLoop;

                case Decimal:
                case Decimal32:
                case Decimal64:
                case Decimal128:
                case Decimal256:
                case Array:
                case Map:
                case Nested:
                case Tuple:
                case SimpleAggregateFunction:
                case AggregateFunction:
                case Enum8:
                case Enum16:
                    // tested separately
                    continue dataTypesLoop;

            }
            b.append(")) Engine = MergeTree ORDER BY ()");

            client.execute(b.toString(), (CommandSettings) new CommandSettings().serverSetting("enable_variant_type", "1"));
            client.register(DTOForVariantPrimitivesTests.class, client.getTableSchema(table));

            Object value = null;
            for (Method m : sample.getClass().getDeclaredMethods()) {
                if (m.getName().equalsIgnoreCase("get" + dataType.name())) {
                    value = m.invoke(sample);
                    System.out.println("selected " + value + " returned by method " + m.getName());
                    break;
                }
            }

            List<DTOForVariantPrimitivesTests> data = new ArrayList<>();
            data.add(new DTOForVariantPrimitivesTests(0, value));
            client.insert(table, data).get().close();

            List<GenericRecord> rows = client.queryAll("SELECT * FROM " + table);
            for (GenericRecord row : rows) {
                String strValue = row.getString("field");
                switch (dataType) {
                    case Date:
                    case Date32:
                        strValue = row.getLocalDate("field").toString();
                        break;
                    case DateTime64:
                    case DateTime:
                    case DateTime32:
                        strValue = row.getLocalDateTime("field").truncatedTo(ChronoUnit.SECONDS).toString();
                        value = ((LocalDateTime)value ).truncatedTo(ChronoUnit.SECONDS).toString();
                        break;
                    case Point:
                        strValue = row.getGeoPoint("field").toString();
                        break;
                    case Ring:
                        strValue = row.getGeoRing("field").toString();
                        break;
                    case Polygon:
                        strValue = row.getGeoPolygon("field").toString();
                        break;
                    case MultiPolygon:
                        strValue = row.getGeoMultiPolygon("field").toString();
                        break;
                }
                System.out.println("field: " + strValue  + " value " + value);
                if (value.getClass().isPrimitive()) {
                    Assert.assertEquals(strValue, String.valueOf(value));
                } else {
                    Assert.assertEquals(strValue, String.valueOf(value));
                }
            }
        }
    }

    @Data
    @AllArgsConstructor
    public static class DTOForVariantPrimitivesTests {
        private int rowId;
        private Object field;
    }

    public void testVariantWithDecimals() throws Exception {

    }

    public void testVariantWithDateTime() throws Exception {

    }

    public void testVariantWithNullable() throws Exception {

    }

    public void testVariantWithArrays() throws Exception {

    }

    public void testVariantWithMaps() throws Exception {

    }

    public static String tableDefinition(String table, String... columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE " + table + " ( ");
        Arrays.stream(columns).forEach(s -> {
            sb.append(s).append(", ");
        });
        sb.setLength(sb.length() - 2);
        sb.append(") Engine = MergeTree ORDER BY ()");
        return sb.toString();
    }

}
