package com.clickhouse.client.datatypes;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
    public void testVariantDataTypeWithPrimitives() throws Exception {
        final String table = "test_variant_primitives";
        String tblCreateSQL = VariantDTO.tblCreateSQL(table);
        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute(tblCreateSQL, (CommandSettings) new CommandSettings().serverSetting("enable_variant_type", "1"));

        client.register(VariantDTO.class, client.getTableSchema(table));

//        List<VariantDTO> data = Arrays.asList(new VariantDTO(1, (short)200), new VariantDTO(2, (byte)127), new VariantDTO(3, "test ☺"));
        List<VariantDTO> data = Arrays.asList(new VariantDTO(1, (short)200), new VariantDTO(2, (byte)127));
        client.insert(table, data);

        List<GenericRecord> rows = client.queryAll("SELECT * FROM " + table);
        for (GenericRecord row : rows) {
            System.out.println(row.getInteger("rowId") + " " + row.getInteger("a"));
        }
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
