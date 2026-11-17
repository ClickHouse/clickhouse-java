package com.clickhouse.client.datatypes.readers;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.datatypes.DataSetPojoGenerator;
import com.clickhouse.client.datatypes.DataSetPojoGenerator.DataSetPojo;
import com.clickhouse.client.testdata.DataTypeGenerator;
import com.clickhouse.client.testdata.GenerateTestDataSet;
import com.clickhouse.client.testdata.GenerateTestDataSet.DataSetEntry;
import com.clickhouse.client.testdata.TestDataSet;
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
 * Reads every storable dataset from the catalog via {@link Client#queryAll(String, Class, TableSchema)} into
 * an on-the-fly generated POJO registered on a fresh {@link Client} instance, after inserting the dataset
 * using ordinary SQL.
 */
public class PojoReaderDataSetTest extends BaseIntegrationTest {

    private static final int TIMEOUT_SECONDS = 60;
    private static final Pattern TIME_TYPE = Pattern.compile("(^|[(, ])Time(64)?\\b");

    private static final List<PojoKnownIssue> KNOWN_ISSUES = Collections.unmodifiableList(Arrays.asList(
            new PojoKnownIssue("Time(64\\(\\d\\))?", ClassCastException.class,
                    "BinaryStreamReader returns LocalDateTime for Time/Time64, POJO setter expects Duration"),
            new PojoKnownIssue("IPv6", ClassCastException.class,
                    "BinaryStreamReader returns Inet4Address for IPv4-mapped IPv6 addresses"),
            new PojoKnownIssue("Enum.*", ClassCastException.class,
                    "BinaryStreamReader returns EnumValue, POJO String setter cannot cast it")
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
    public void testReadDataSetWithPojo(DataSetEntry entry) throws Exception {
        runPojoReadTest(entry, DataSetPojoGenerator.forDataSet(entry), true);
    }

    @Test(groups = {"integration"}, dataProvider = "primitiveDataSetEntries")
    public void testReadDataSetWithPrimitivePojo(DataSetEntry entry, Class<?> primitiveType) throws Exception {
        runPojoReadTest(entry, DataSetPojoGenerator.forDataSet(entry, int.class, primitiveType), false);
    }

    @Test(groups = {"integration"}, dependsOnMethods = "testReadDataSetWithPojo", alwaysRun = true)
    public void testKnownIssuesAreCurrent() {
        if (!testedTypes.containsAll(GenerateTestDataSet.getStorableTypes())) {
            throw new SkipException("Not all data sets were read, cannot tell whether known issues still happen");
        }
        List<String> fixed = new ArrayList<>();
        for (PojoKnownIssue issue : KNOWN_ISSUES) {
            if (!matchedIssues.contains(issue)) {
                fixed.add(issue.toString());
            }
        }
        Assert.assertTrue(fixed.isEmpty(),
                "Known issues do not happen anymore, remove them from PojoReaderDataSetTest:\n  "
                        + String.join("\n  ", fixed));
    }

    private void runPojoReadTest(DataSetEntry entry, DataSetPojo pojo, boolean trackKnownIssues) throws Exception {
        if (TIME_TYPE.matcher(entry.getType()).find() && serverVersion.check("(,25.5]")) {
            throw new SkipException("Time and Time64 types require ClickHouse 25.6+");
        }

        DataTypeGenerator generator = GenerateTestDataSet.parseType(entry.getType());
        TestDataSet dataSet = TestDataSet.of(generator);
        Map<String, String> settings = dataSet.getRequiredSettings();
        String table = "pojo_reader_" + entry.getName().toLowerCase(Locale.ROOT)
                + "_" + pojo.getValueType().getSimpleName().toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "");

        try (Client client = newClient(entry, settings)) {
            dropTable(client, table);
            client.execute(dataSet.getCreateTableSql(table, null, isCluster()), commandSettings(settings))
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS).close();
            client.execute(dataSet.getInsertSql(table), commandSettings(settings))
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS).close();
            try {
                TableSchema schema = client.getTableSchema(table);
                client.register(pojo.getPojoClass(), schema);

                List<?> actualRows;
                try {
                    actualRows = client.queryAll(
                            "SELECT * FROM " + table + " ORDER BY id", pojo.getPojoClass(), schema);
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

                List<Object[]> expectedRows = dataSet.getRows();
                Assert.assertEquals(actualRows.size(), expectedRows.size(),
                        "Row count for " + entry.getType());

                for (int i = 0; i < expectedRows.size(); i++) {
                    Object record = actualRows.get(i);
                    Object[] expectedRow = expectedRows.get(i);
                    Assert.assertEquals(pojo.getId(record), ((Integer) expectedRow[0]).intValue(),
                            "Row order mismatch for " + entry.getType());
                    Object expected = ReaderValues.normalize(expectedRow[1]);
                    Object actual = ReaderValues.normalize(pojo.getValue(record));
                    Assert.assertTrue(ReaderValues.deepEquals(expected, actual),
                            entry.getType() + " mismatch at row " + expectedRow[0]
                                    + ": expected " + ReaderValues.describe(expectedRow[1])
                                    + " but got " + ReaderValues.describe(pojo.getValue(record)));
                }
                if (trackKnownIssues) {
                    testedTypes.add(entry.getType());
                }
            } finally {
                dropTable(client, table);
            }
        }
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
        Client.Builder builder = newClientBuilder();
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
