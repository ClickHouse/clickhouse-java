package com.clickhouse.client.datatypes.readers;

import com.clickhouse.client.api.data_formats.internal.MapBackedRecord;
import com.clickhouse.client.api.metadata.NoSuchColumnException;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.testdata.DataTypeGenerator;
import com.clickhouse.client.testdata.GenerateTestDataSet;
import com.clickhouse.client.testdata.TestDataSet;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseFormat;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.clickhouse.client.datatypes.readers.KnownIssue.Deviation.MISSING_EXCEPTION;
import static com.clickhouse.client.datatypes.readers.KnownIssue.Deviation.UNEXPECTED_EXCEPTION;
import static com.clickhouse.client.datatypes.readers.KnownIssue.Deviation.WRONG_EXCEPTION;
import static com.clickhouse.client.datatypes.readers.ReaderAccessor.BOOLEAN_ARRAY;
import static com.clickhouse.client.datatypes.readers.ReaderAccessor.BYTE_ARRAY;
import static com.clickhouse.client.datatypes.readers.ReaderAccessor.DOUBLE_ARRAY;
import static com.clickhouse.client.datatypes.readers.ReaderAccessor.ENUM16;
import static com.clickhouse.client.datatypes.readers.ReaderAccessor.ENUM8;
import static com.clickhouse.client.datatypes.readers.ReaderAccessor.FLOAT_ARRAY;
import static com.clickhouse.client.datatypes.readers.ReaderAccessor.INSTANT;
import static com.clickhouse.client.datatypes.readers.ReaderAccessor.INT_ARRAY;
import static com.clickhouse.client.datatypes.readers.ReaderAccessor.LONG_ARRAY;
import static com.clickhouse.client.datatypes.readers.ReaderAccessor.SHORT_ARRAY;
import static com.clickhouse.client.datatypes.readers.ReaderAccessor.TEMPORAL_AMOUNT;

/**
 * Reads the data set of every type in {@link GenerateTestDataSet#getStorableTypes()} via
 * {@link com.clickhouse.client.api.Client#queryAll(String, QuerySettings)} (producing {@link MapBackedRecord}) and
 * verifies every accessor of {@link GenericRecord} by column index and by name against {@link ReaderExpectations}.
 */
public class MapBackedRecordDataSetTest extends AbstractAccessorDataSetTest {

    private static final List<KnownIssue> KNOWN_ISSUES = buildKnownIssues();

    private static List<KnownIssue> buildKnownIssues() {
        List<KnownIssue> issues = new ArrayList<>();
        for (KnownIssue issue : ReaderKnownIssues.ALL) {
            if (!issue.isReadersOnly() && issue.appliesTo(ClickHouseFormat.RowBinaryWithNamesAndTypes)) {
                issues.add(issue);
            }
        }
        issues.addAll(Arrays.asList(
                KnownIssue.of(WRONG_EXCEPTION, "getTemporalAmount(int) casts the value without checking the column type")
                        .throwing(ClassCastException.class).accessors(TEMPORAL_AMOUNT),
                KnownIssue.of(UNEXPECTED_EXCEPTION,
                                "getTemporalAmount(int) returns LocalDateTime instead of Duration for Time/Time64")
                        .throwing(ClassCastException.class).accessors(TEMPORAL_AMOUNT).types("Time(64\\(\\d\\))?"),
                KnownIssue.of(WRONG_EXCEPTION, "primitive array accessors cast the value without checking the column type")
                        .throwing(ClassCastException.class)
                        .accessors(BYTE_ARRAY, SHORT_ARRAY, INT_ARRAY, LONG_ARRAY, FLOAT_ARRAY, DOUBLE_ARRAY,
                                BOOLEAN_ARRAY),
                KnownIssue.of(UNEXPECTED_EXCEPTION, "getByteArray does not convert InetAddress for IPv4/IPv6")
                        .throwing(ClassCastException.class).accessors(BYTE_ARRAY).types("IPv4|IPv6"),
                KnownIssue.of(UNEXPECTED_EXCEPTION, "getInstant looks up column index -1 on Date/Date32")
                        .throwing(NoSuchColumnException.class).accessors(INSTANT).types("Date(32)?"),
                KnownIssue.of(MISSING_EXCEPTION, "getEnum8 casts Byte directly without checking column type")
                        .accessors(ENUM8).types("Int8"),
                KnownIssue.of(MISSING_EXCEPTION, "getEnum16 casts Short directly without checking column type")
                        .accessors(ENUM16).types("Int16|UInt8")
        ));
        return Collections.unmodifiableList(issues);
    }

    @Override
    protected String getSubjectName() {
        return "MapBackedRecord";
    }

    @Override
    protected List<KnownIssue> getKnownIssues() {
        return KNOWN_ISSUES;
    }

    @Override
    protected boolean issueMatches(KnownIssue issue, String type, DeviationSummary deviation) {
        return issue.matches(ClickHouseFormat.RowBinaryWithNamesAndTypes, type, deviation.accessor,
                deviation.deviation, deviation.nullValue, deviation.thrown);
    }

    @Override
    protected Map<String, DeviationSummary> readAndVerify(TestDataSet dataSet, String table,
            Map<String, String> settings) throws Exception {
        Map<String, DeviationSummary> deviations = new LinkedHashMap<>();
        QuerySettings querySettings = new QuerySettings();
        settings.forEach(querySettings::serverSetting);
        DataTypeGenerator generator = dataSet.getColumns().get(VALUE_COLUMN);
        List<Object[]> rows = dataSet.getRows();
        int rowCount = 0;
        List<GenericRecord> records;
        try {
            records = getClient().queryAll("SELECT * FROM " + table + " ORDER BY id", querySettings);
        } catch (Exception e) {
            deviations.put("read", readFailure(rowCount, e));
            return deviations;
        }
        ReaderExpectations expectations = null;
        for (GenericRecord record : records) {
            Assert.assertTrue(record instanceof MapBackedRecord,
                    "Expected MapBackedRecord but got " + record.getClass());
            if (expectations == null) {
                ClickHouseColumn column = record.getSchema().getColumnByName(VALUE_COLUMN);
                expectations = new ReaderExpectations(column, generator);
            }
            Assert.assertTrue(rowCount < rows.size(), "More rows than in the data set: " + rows.size());
            Object[] row = rows.get(rowCount++);
            Assert.assertEquals(record.getInteger(1), row[0], "Rows are read out of order");
            verifyRow(record, expectations, row, deviations);
        }
        Assert.assertEquals(rowCount, rows.size(), "Number of rows read");
        return deviations;
    }

    private static void verifyRow(GenericRecord record, ReaderExpectations expectations, Object[] row,
            Map<String, DeviationSummary> deviations) {
        Object value = row[1];
        Object raw = record.getObject(VALUE_INDEX);
        Assert.assertSame(record.getObject(VALUE_COLUMN), raw, "getObject by name and by index");
        Assert.assertSame(record.getValues().get(VALUE_COLUMN), raw, "getValues() and getObject");
        Assert.assertEquals(record.hasValue(VALUE_INDEX), value != null, "hasValue by index for row " + row[0]);
        Assert.assertEquals(record.hasValue(VALUE_COLUMN), value != null, "hasValue by name for row " + row[0]);
        verifyRecordAccessors(record, expectations, row, deviations);
    }
}
