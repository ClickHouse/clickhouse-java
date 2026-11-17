package com.clickhouse.client.datatypes.readers;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.datatypes.readers.KnownIssue.Deviation;
import com.clickhouse.client.datatypes.readers.ReaderExpectations.Expected;
import com.clickhouse.client.testdata.DataTypeGenerator;
import com.clickhouse.client.testdata.GenerateTestDataSet;
import com.clickhouse.client.testdata.TestDataSet;
import com.clickhouse.data.ClickHouseVersion;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Shared integration test harness that populates a table for each storable type in
 * {@link GenerateTestDataSet#getStorableTypes()}, reads it via a concrete reader or record implementation,
 * and checks every accessor against {@link ReaderExpectations} while tracking {@link KnownIssue} entries.
 */
public abstract class AbstractAccessorDataSetTest extends BaseIntegrationTest {

    protected static final String VALUE_COLUMN = "value";
    protected static final int VALUE_INDEX = 2;
    protected static final int TIMEOUT_SECONDS = 60;
    private static final Pattern TIME_TYPE = Pattern.compile("(^|[(, ])Time(64)?\\b");

    private final Set<String> testedTypes = ConcurrentHashMap.newKeySet();
    private final Set<KnownIssue> matchedIssues = ConcurrentHashMap.newKeySet();

    private Client client;
    private ClickHouseVersion serverVersion;

    protected Client getClient() {
        return client;
    }

    protected abstract String getSubjectName();

    protected abstract List<KnownIssue> getKnownIssues();

    protected boolean issueApplies(KnownIssue issue) {
        return true;
    }

    protected boolean issueMatches(KnownIssue issue, String type, DeviationSummary deviation) {
        return issue.matches(null, type, deviation.accessor, deviation.deviation, deviation.nullValue,
                deviation.thrown);
    }

    protected abstract Map<String, DeviationSummary> readAndVerify(TestDataSet dataSet, String table,
            Map<String, String> settings) throws Exception;

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
        List<String> storableTypes = GenerateTestDataSet.getStorableTypes();
        Object[][] types = new Object[storableTypes.size()][];
        for (int i = 0; i < types.length; i++) {
            types[i] = new Object[] {storableTypes.get(i)};
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
        String name = getClass().getSimpleName().isEmpty() ? getSubjectName() : getClass().getSimpleName();
        String table = "reader_" + name.toLowerCase(Locale.ROOT) + "_"
                + type.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "_").replaceAll("^_|_$", "");
        Map<String, String> settings = dataSet.getRequiredSettings();

        dropTable(table);
        client.execute(dataSet.getCreateTableSql(table, getClusterName()), commandSettings(settings))
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
                Assert.fail(getSubjectName() + " deviates for " + type + ":\n  " + String.join("\n  ", failures));
            }
        } finally {
            dropTable(table);
        }
    }

    @Test(groups = {"integration"}, dependsOnMethods = "testReadDataSet", alwaysRun = true)
    public void testKnownIssuesAreCurrent() {
        if (!testedTypes.containsAll(GenerateTestDataSet.getStorableTypes())) {
            throw new SkipException("Not all data sets were read, cannot tell whether known issues still happen");
        }
        List<String> fixed = new ArrayList<>();
        for (KnownIssue issue : getKnownIssues()) {
            if (issueApplies(issue) && !matchedIssues.contains(issue)) {
                fixed.add(issue.toString());
            }
        }
        Assert.assertTrue(fixed.isEmpty(), "Known issues do not happen anymore, remove them:\n  "
                + String.join("\n  ", fixed));
    }

    protected static void verifyRecordAccessors(GenericRecord record, ReaderExpectations expectations, Object[] row,
            Map<String, DeviationSummary> deviations) {
        Object value = row[1];
        for (ReaderAccessor accessor : ReaderAccessor.values()) {
            Expected expected = expectations.expected(accessor, value);
            verify(accessor, expected, row, "index", () -> accessor.get(record, VALUE_INDEX), deviations);
            verify(accessor, expected, row, "name", () -> accessor.get(record, VALUE_COLUMN), deviations);
        }
    }

    protected static void verify(ReaderAccessor accessor, Expected expected, Object[] row, String by,
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
    protected static void record(Map<String, DeviationSummary> deviations, ReaderAccessor accessor,
            Deviation deviation, Object[] row, String by, Expected expected, Object actual) {
        boolean nullValue = row[1] == null;
        Class<? extends Throwable> thrown = actual instanceof Throwable ? ((Throwable) actual).getClass() : null;
        String actualText = thrown != null ? actual.toString() : ReaderValues.describe(actual);
        deviations.computeIfAbsent(accessor + " " + deviation + (nullValue ? " on NULL" : "") + " " + thrown,
                k -> new DeviationSummary(accessor, deviation, nullValue, thrown, (Integer) row[0],
                        "by " + by + " expected " + expected + " but got " + actualText)).count++;
    }

    protected static DeviationSummary readFailure(int rowCount, Exception e) {
        DeviationSummary failure = new DeviationSummary(null, Deviation.READ_FAILURE, false, null, rowCount,
                "reading failed with " + e);
        failure.count = 1;
        return failure;
    }

    private KnownIssue findKnownIssue(String type, DeviationSummary deviation) {
        for (KnownIssue issue : getKnownIssues()) {
            if (issueMatches(issue, type, deviation)) {
                return issue;
            }
        }
        return null;
    }

    private void dropTable(String table) throws Exception {
        client.execute("DROP TABLE IF EXISTS " + table + onCluster() + " SYNC")
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS).close();
    }

    private static CommandSettings commandSettings(Map<String, String> settings) {
        CommandSettings commandSettings = new CommandSettings();
        settings.forEach(commandSettings::serverSetting);
        return commandSettings;
    }

    protected static final class DeviationSummary {
        final ReaderAccessor accessor;
        final Deviation deviation;
        final boolean nullValue;
        final Class<? extends Throwable> thrown;
        final int firstRow;
        final String example;
        int count;

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
