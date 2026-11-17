package com.clickhouse.client.datatypes.writers;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.datatypes.DataSetPojoGenerator;
import com.clickhouse.client.datatypes.DataSetPojoGenerator.DataSetPojo;
import com.clickhouse.client.datatypes.readers.ReaderValues;
import com.clickhouse.client.testdata.DataTypeGenerator;
import com.clickhouse.client.testdata.GenerateTestDataSet;
import com.clickhouse.client.testdata.GenerateTestDataSet.DataSetEntry;
import com.clickhouse.client.testdata.TestDataSet;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseVersion;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Writes every storable dataset from the catalog via {@link Client#insert(String, List)} using an on-the-fly
 * generated POJO registered on a fresh {@link Client} instance, then reads the rows back with a binary format
 * reader (non-POJO path) and verifies every value.
 */
public class PojoWriterDataSetTest extends BaseIntegrationTest {

    private static final int TIMEOUT_SECONDS = 60;
    private static final Pattern TIME_TYPE = Pattern.compile("(^|[(, ])Time(64)?\\b");

    private static final List<PojoKnownIssue> KNOWN_ISSUES = Collections.unmodifiableList(Arrays.asList(
            new PojoKnownIssue("Time", IllegalArgumentException.class,
                    "SerializerUtils expects Integer/Number for Time, not Duration"),
            new PojoKnownIssue("Time64\\(\\d\\)", UnsupportedOperationException.class,
                    "SerializerUtils.serializeTime64 does not support Duration")
    ));

    private final Set<String> testedTypes = ConcurrentHashMap.newKeySet();
    private final Set<PojoKnownIssue> matchedIssues = ConcurrentHashMap.newKeySet();

    private ClickHouseVersion serverVersion;

    @BeforeClass(groups = {"integration"})
    public void setUp() {
        try (Client client = newClientBuilder().build()) {
            serverVersion = ClickHouseVersion.of(client.queryAll("SELECT version()").get(0).getString(1));
        }
    }

    @DataProvider
    public Object[][] dataSetEntries() {
        return GenerateTestDataSet.readDataSetIndex().stream()
                .filter(DataSetEntry::isStorable)
                .map(entry -> new Object[] {entry})
                .toArray(Object[][]::new);
    }

    @DataProvider
    public Object[][] primitiveDataSetEntries() {
        List<Object[]> cases = new ArrayList<>();
        for (DataSetEntry entry : GenerateTestDataSet.readDataSetIndex()) {
            if (!entry.isStorable()) {
                continue;
            }
            Class<?> primitiveType = DataSetPojoGenerator.primitiveJavaTypeFor(entry);
            if (primitiveType != null) {
                cases.add(new Object[] {entry, primitiveType});
            }
        }
        return cases.toArray(new Object[0][]);
    }

    @Test(groups = {"integration"}, dataProvider = "dataSetEntries")
    public void testWriteDataSetWithPojo(DataSetEntry entry) throws Exception {
        runPojoWriteTest(entry, DataSetPojoGenerator.forDataSet(entry), true);
    }

    @Test(groups = {"integration"}, dataProvider = "primitiveDataSetEntries")
    public void testWriteDataSetWithPrimitivePojo(DataSetEntry entry, Class<?> primitiveType) throws Exception {
        runPojoWriteTest(entry, DataSetPojoGenerator.forDataSet(entry, int.class, primitiveType), false);
    }

    @Test(groups = {"integration"}, dependsOnMethods = "testWriteDataSetWithPojo", alwaysRun = true)
    public void testKnownIssuesAreCurrent() {
        if (!testedTypes.containsAll(GenerateTestDataSet.getStorableTypes())) {
            throw new SkipException("Not all data sets were written, cannot tell whether known issues still happen");
        }
        List<String> fixed = new ArrayList<>();
        for (PojoKnownIssue issue : KNOWN_ISSUES) {
            if (!matchedIssues.contains(issue)) {
                fixed.add(issue.toString());
            }
        }
        Assert.assertTrue(fixed.isEmpty(),
                "Known issues do not happen anymore, remove them from PojoWriterDataSetTest:\n  "
                        + String.join("\n  ", fixed));
    }

    private void runPojoWriteTest(DataSetEntry entry, DataSetPojo pojo, boolean trackKnownIssues) throws Exception {
        if (TIME_TYPE.matcher(entry.getType()).find() && serverVersion.check("(,25.5]")) {
            throw new SkipException("Time and Time64 types require ClickHouse 25.6+");
        }

        DataTypeGenerator generator = GenerateTestDataSet.parseType(entry.getType());
        TestDataSet dataSet = TestDataSet.of(generator);
        Map<String, String> settings = dataSet.getRequiredSettings();
        String table = "pojo_writer_" + entry.getName().toLowerCase(Locale.ROOT)
                + "_" + pojo.getValueType().getSimpleName().toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "");

        try (Client client = newClient(entry, settings)) {
            dropTable(client, table);
            client.execute(dataSet.getCreateTableSql(table, null, isCluster()), commandSettings(settings))
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS).close();
            try {
                TableSchema schema = client.getTableSchema(table);
                client.register(pojo.getPojoClass(), schema);

                List<Object> pojos = pojo.toPojoList(dataSet);
                try (InsertResponse insertResponse = client.insert(table, pojos)
                        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    Assert.assertEquals(insertResponse.getWrittenRows(), pojos.size(),
                            "Written rows for " + entry.getType());
                } catch (Exception e) {
                    if (trackKnownIssues) {
                        PojoKnownIssue issue = findKnownIssue(entry.getType(), e);
                        if (issue != null) {
                            matchedIssues.add(issue);
                            testedTypes.add(entry.getType());
                            return;
                        }
                    }
                    throw e;
                }

                readAndVerify(client, entry, dataSet, table, settings);
                if (trackKnownIssues) {
                    testedTypes.add(entry.getType());
                }
            } finally {
                dropTable(client, table);
            }
        }
    }

    private static void readAndVerify(Client client, DataSetEntry entry, TestDataSet dataSet,
            String table, Map<String, String> settings) throws Exception {
        QuerySettings querySettings = new QuerySettings().setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
        settings.forEach(querySettings::serverSetting);
        List<Object[]> expectedRows = dataSet.getRows();
        ClickHouseColumn valueColumn = ClickHouseColumn.of("value", entry.getClickHouseType());

        int rowCount = 0;
        try (QueryResponse response = client.query("SELECT * FROM " + table + " ORDER BY id", querySettings)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            while (reader.next() != null) {
                Assert.assertTrue(rowCount < expectedRows.size(),
                        "More rows than in dataset: " + expectedRows.size());
                Object[] expectedRow = expectedRows.get(rowCount++);
                Assert.assertEquals(reader.getInteger("id"), expectedRow[0],
                        "Rows read out of order for " + entry.getType());

                Object raw = readActualValue(reader, entry, valueColumn);
                Object expected = ReaderValues.normalize(expectedRow[1]);
                Object actual = ReaderValues.normalize(raw);
                Assert.assertTrue(ReaderValues.deepEquals(expected, actual),
                        entry.getType() + " mismatch at row " + expectedRow[0]
                                + ": expected " + ReaderValues.describe(expectedRow[1])
                                + " but got " + ReaderValues.describe(raw));
            }
        }
        Assert.assertEquals(rowCount, expectedRows.size(), "Number of rows read for " + entry.getType());
    }

    private static Object readActualValue(ClickHouseBinaryFormatReader reader, DataSetEntry entry,
            ClickHouseColumn valueColumn) {
        if ("BinaryString".equals(entry.getType())) {
            return reader.hasValue("value") ? reader.getByteArray("value") : null;
        }
        ClickHouseDataType dataType = valueColumn.getDataType();
        if (dataType == ClickHouseDataType.IPv6) {
            return reader.hasValue("value") ? reader.getInet6Address("value") : null;
        }
        if (dataType == ClickHouseDataType.Time || dataType == ClickHouseDataType.Time64) {
            return reader.hasValue("value") ? reader.getDuration("value") : null;
        }
        return reader.readValue("value");
    }

    private static PojoKnownIssue findKnownIssue(String type, Throwable error) {
        for (PojoKnownIssue issue : KNOWN_ISSUES) {
            if (issue.matches(type, error)) {
                return issue;
            }
        }
        return null;
    }

    private Client newClient(DataSetEntry entry, Map<String, String> settings) {
        Client.Builder builder = newClientBuilder()
                .serverSetting(ServerSettings.ASYNC_INSERT, "0")
                .serverSetting(ServerSettings.WAIT_END_OF_QUERY, "1");
        settings.forEach(builder::serverSetting);
        if ("BinaryString".equals(entry.getType())) {
            builder.binaryStringSupport(true);
        }
        return builder.build();
    }

    private Client.Builder newClientBuilder() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase());
    }

    private static void dropTable(Client client, String table) throws Exception {
        client.execute("DROP TABLE IF EXISTS " + table + " SYNC")
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS).close();
    }

    private static CommandSettings commandSettings(Map<String, String> settings) {
        CommandSettings commandSettings = new CommandSettings();
        settings.forEach(commandSettings::serverSetting);
        return commandSettings;
    }

    private static final class PojoKnownIssue {
        private final Pattern typePattern;
        private final Class<? extends Throwable> rootCause;
        private final String description;

        PojoKnownIssue(String typeRegex, Class<? extends Throwable> rootCause, String description) {
            this.typePattern = Pattern.compile(typeRegex);
            this.rootCause = rootCause;
            this.description = description;
        }

        boolean matches(String type, Throwable error) {
            if (!typePattern.matcher(type).matches()) {
                return false;
            }
            for (Throwable t = error; t != null; t = t.getCause()) {
                if (rootCause.isInstance(t)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            return typePattern + " (" + rootCause.getSimpleName() + "): " + description;
        }
    }
}
