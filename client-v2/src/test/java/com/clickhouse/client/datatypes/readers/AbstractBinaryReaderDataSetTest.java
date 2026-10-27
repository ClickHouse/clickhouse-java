package com.clickhouse.client.datatypes.readers;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.datatypes.readers.KnownIssue.Deviation;
import com.clickhouse.client.datatypes.readers.ReaderExpectations.Expected;
import com.clickhouse.client.testdata.DataTypeGenerator;
import com.clickhouse.client.testdata.GenerateTestDataSet;
import com.clickhouse.client.testdata.TestDataSet;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseVersion;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Reads the data set of every type in {@link GenerateTestDataSet#STORABLE_TYPES} with a binary format reader and
 * verifies every accessor of {@link com.clickhouse.client.api.data_formats.ClickHouseFormatReader} for every row,
 * by column index and by name, against {@link ReaderExpectations}. Deviations listed in {@link ReaderKnownIssues}
 * are allowed; known issues that do not happen anymore fail {@link #testKnownIssuesAreCurrent()}.
 */
public abstract class AbstractBinaryReaderDataSetTest extends BaseIntegrationTest {

    private static final String VALUE_COLUMN = "value";
    private static final int VALUE_INDEX = 2;
    private static final int TIMEOUT_SECONDS = 60;
    private static final Pattern TIME_TYPE = Pattern.compile("(^|[(, ])Time(64)?\\b");

    private final Set<String> testedTypes = ConcurrentHashMap.newKeySet();
    private final Set<KnownIssue> matchedIssues = ConcurrentHashMap.newKeySet();

    private Client client;
    private ClickHouseVersion serverVersion;

    protected abstract ClickHouseFormat getFormat();

    @BeforeClass(groups = {"integration"})
    public void setUp() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .build();
        serverVersion = ClickHouseVersion.of(client.queryAll("SELECT version()").get(0).getString(1));
    }

    @AfterClass(groups = {"integration"})
    public void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @DataProvider
    public Object[][] dataSetTypes() {
        Object[][] types = new Object[GenerateTestDataSet.STORABLE_TYPES.size()][];
        for (int i = 0; i < types.length; i++) {
            types[i] = new Object[] {GenerateTestDataSet.STORABLE_TYPES.get(i)};
        }
        return types;
    }

    @Test(groups = {"integration"}, dataProvider = "dataSetTypes")
    public void testReadDataSet(String type) throws Exception {
        if (TIME_TYPE.matcher(type).find() && serverVersion.check("(,25.5]")) {
            throw new SkipException("Time and Time64 types require ClickHouse 25.6+");
        }
        DataTypeGenerator generator = GenerateTestDataSet.parseType(type);
        TestDataSet dataSet = TestDataSet.of(generator);
        String table = "reader_" + getFormat().name().toLowerCase() + "_"
                + type.toLowerCase().replaceAll("[^a-z0-9]+", "_").replaceAll("^_|_$", "");
        Map<String, String> settings = dataSet.getRequiredSettings();

        client.execute("DROP TABLE IF EXISTS " + table).get(TIMEOUT_SECONDS, TimeUnit.SECONDS).close();
        client.execute(dataSet.getCreateTableSql(table), commandSettings(settings))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS).close();
        client.execute(dataSet.getInsertSql(table), commandSettings(settings))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS).close();
        try {
            Map<String, DeviationSummary> deviations = readAndVerify(dataSet, table, settings);
            testedTypes.add(type);
            List<String> failures = new ArrayList<>();
            for (DeviationSummary deviation : deviations.values()) {
                KnownIssue issue = findKnownIssue(type, deviation);
                if (issue == null) {
                    failures.add(deviation.toString());
                } else {
                    matchedIssues.add(issue);
                }
            }
            if (!failures.isEmpty()) {
                Assert.fail(getFormat() + " reader deviates for " + type + ":\n  " + String.join("\n  ", failures));
            }
        } finally {
            client.execute("DROP TABLE IF EXISTS " + table).get(TIMEOUT_SECONDS, TimeUnit.SECONDS).close();
        }
    }

    @Test(groups = {"integration"}, dependsOnMethods = "testReadDataSet", alwaysRun = true)
    public void testKnownIssuesAreCurrent() {
        if (!testedTypes.containsAll(GenerateTestDataSet.STORABLE_TYPES)) {
            throw new SkipException("Not all data sets were read, cannot tell whether known issues still happen");
        }
        List<String> fixed = new ArrayList<>();
        for (KnownIssue issue : ReaderKnownIssues.ALL) {
            if (issue.appliesTo(getFormat()) && !matchedIssues.contains(issue)) {
                fixed.add(issue.toString());
            }
        }
        Assert.assertTrue(fixed.isEmpty(), "Known issues do not happen anymore, remove them from ReaderKnownIssues:\n  "
                + String.join("\n  ", fixed));
    }

    private Map<String, DeviationSummary> readAndVerify(TestDataSet dataSet, String table,
            Map<String, String> settings) throws Exception {
        Map<String, DeviationSummary> deviations = new LinkedHashMap<>();
        QuerySettings querySettings = new QuerySettings().setFormat(getFormat());
        settings.forEach(querySettings::serverSetting);
        TableSchema tableSchema = client.getTableSchema(table);
        DataTypeGenerator generator = dataSet.getColumns().get(VALUE_COLUMN);
        List<Object[]> rows = dataSet.getRows();
        int rowCount = 0;
        try (QueryResponse response = client.query("SELECT * FROM " + table + " ORDER BY id", querySettings)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
             ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response, tableSchema)) {
            ReaderExpectations expectations = null;
            while (reader.hasNext()) {
                Map<String, Object> record = reader.next();
                if (expectations == null) {
                    ClickHouseColumn column = reader.getSchema().getColumnByName(VALUE_COLUMN);
                    expectations = new ReaderExpectations(column, generator);
                }
                Assert.assertTrue(rowCount < rows.size(), "More rows than in the data set: " + rows.size());
                Object[] row = rows.get(rowCount++);
                Assert.assertEquals(reader.getInteger(1), row[0], "Rows are read out of order");
                verifyRow(reader, record, expectations, row, deviations);
            }
        } catch (Exception e) {
            DeviationSummary failure = new DeviationSummary(null, Deviation.READ_FAILURE, false, null, rowCount,
                    "reading failed with " + e);
            failure.count = 1;
            deviations.put("read", failure);
            return deviations;
        }
        Assert.assertEquals(rowCount, rows.size(), "Number of rows read");
        return deviations;
    }

    private void verifyRow(ClickHouseBinaryFormatReader reader, Map<String, Object> record,
            ReaderExpectations expectations, Object[] row, Map<String, DeviationSummary> deviations) {
        Object value = row[1];
        Object raw = reader.readValue(VALUE_INDEX);
        Assert.assertSame(reader.readValue(VALUE_COLUMN), raw, "readValue by name and by index");
        Assert.assertSame(record.get(VALUE_COLUMN), raw, "record returned by next() and readValue");
        Assert.assertEquals(reader.hasValue(VALUE_INDEX), value != null, "hasValue by index for row " + row[0]);
        Assert.assertEquals(reader.hasValue(VALUE_COLUMN), value != null, "hasValue by name for row " + row[0]);

        for (ReaderAccessor accessor : ReaderAccessor.values()) {
            Expected expected = expectations.expected(accessor, value);
            verify(accessor, expected, row, "index", () -> accessor.get(reader, VALUE_INDEX), deviations);
            verify(accessor, expected, row, "name", () -> accessor.get(reader, VALUE_COLUMN), deviations);
        }
    }

    private void verify(ReaderAccessor accessor, Expected expected, Object[] row, String by,
            Callable<Object> call, Map<String, DeviationSummary> deviations) {
        Object actual;
        try {
            actual = call.call();
        } catch (Exception e) {
            if (!expected.isException()) {
                record(deviations, accessor, Deviation.UNEXPECTED_EXCEPTION, row, by, expected, e);
            } else if (!expected.getException().isInstance(e)) {
                record(deviations, accessor, Deviation.WRONG_EXCEPTION, row, by, expected, e);
            }
            return;
        }
        if (expected.isException()) {
            record(deviations, accessor, Deviation.MISSING_EXCEPTION, row, by, expected, actual);
        } else if (!expected.matches(actual)) {
            record(deviations, accessor, Deviation.WRONG_VALUE, row, by, expected, actual);
        }
    }

    /**
     * Records a deviation; deviations are counted per accessor, kind, {@code NULL} value and exception class.
     */
    private static void record(Map<String, DeviationSummary> deviations, ReaderAccessor accessor,
            Deviation deviation, Object[] row, String by, Expected expected, Object actual) {
        boolean nullValue = row[1] == null;
        Class<? extends Throwable> thrown = actual instanceof Throwable ? ((Throwable) actual).getClass() : null;
        String actualText = thrown != null ? actual.toString() : ReaderValues.describe(actual);
        deviations.computeIfAbsent(accessor + " " + deviation + (nullValue ? " on NULL" : "") + " " + thrown,
                k -> new DeviationSummary(accessor, deviation, nullValue, thrown, (Integer) row[0],
                        "by " + by + " expected " + expected + " but got " + actualText)).count++;
    }

    private KnownIssue findKnownIssue(String type, DeviationSummary deviation) {
        for (KnownIssue issue : ReaderKnownIssues.ALL) {
            if (issue.matches(getFormat(), type, deviation.accessor, deviation.deviation, deviation.nullValue,
                    deviation.thrown)) {
                return issue;
            }
        }
        return null;
    }

    private static CommandSettings commandSettings(Map<String, String> settings) {
        CommandSettings commandSettings = new CommandSettings();
        settings.forEach(commandSettings::serverSetting);
        return commandSettings;
    }

    private static final class DeviationSummary {
        private final ReaderAccessor accessor;
        private final Deviation deviation;
        private final boolean nullValue;
        private final Class<? extends Throwable> thrown;
        private final int firstRow;
        private final String example;
        private int count;

        DeviationSummary(ReaderAccessor accessor, Deviation deviation, boolean nullValue,
                Class<? extends Throwable> thrown, int firstRow, String example) {
            this.accessor = accessor;
            this.deviation = deviation;
            this.nullValue = nullValue;
            this.thrown = thrown;
            this.firstRow = firstRow;
            this.example = example;
        }

        @Override
        public String toString() {
            return (accessor == null ? "" : accessor + " ") + deviation + (nullValue ? " on NULL" : "") + " in " + count
                    + " call(s), first at row " + firstRow + ": " + example;
        }
    }
}
