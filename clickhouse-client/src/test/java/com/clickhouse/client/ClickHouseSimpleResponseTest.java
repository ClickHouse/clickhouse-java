package com.clickhouse.client;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.value.ClickHouseByteValue;
import com.clickhouse.data.value.ClickHouseStringValue;
import com.clickhouse.data.value.UnsignedByte;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseSimpleResponseTest {
    private final ClickHouseConfig config = new ClickHouseConfig();

    @Test(groups = { "unit" })
    public void testNullOrEmptyInput() {
        ClickHouseResponse nullResp = ClickHouseSimpleResponse.of(config, (List<ClickHouseColumn>) null, null);
        Assert.assertEquals(nullResp.getColumns(), Collections.emptyList());
        Assert.assertTrue(((List<?>) nullResp.records()).isEmpty());
        Assert.assertThrows(NoSuchElementException.class, () -> nullResp.firstRecord());

        ClickHouseResponse emptyResp1 = ClickHouseSimpleResponse.of(config, ClickHouseColumn.parse("a String"), null);
        Assert.assertEquals(emptyResp1.getColumns(), ClickHouseColumn.parse("a String"));
        Assert.assertTrue(((List<?>) emptyResp1.records()).isEmpty());
        Assert.assertThrows(NoSuchElementException.class, () -> emptyResp1.firstRecord());

        ClickHouseResponse emptyResp2 = ClickHouseSimpleResponse.of(config, ClickHouseColumn.parse("a String"),
                new Object[0][]);
        Assert.assertEquals(emptyResp2.getColumns(), ClickHouseColumn.parse("a String"));
        Assert.assertTrue(((List<?>) emptyResp2.records()).isEmpty());
        Assert.assertThrows(NoSuchElementException.class, () -> emptyResp2.firstRecord());
    }

    @Test(groups = { "unit" })
    public void testMismatchedColumnsAndRecords() {
        ClickHouseResponse resp = ClickHouseSimpleResponse
                .of(config, ClickHouseColumn.parse("a Nullable(String), b UInt8, c Array(UInt32)"),
                        new Object[][] { new Object[0], null, new Object[] { 's' },
                                new Object[] { null, null, null, null },
                                new Object[] { "123", 1, new int[] { 3, 2, 1 } } });
        int i = 0;
        for (ClickHouseRecord r : resp.records()) {
            switch (i) {
                case 0:
                case 1:
                case 3:
                    Assert.assertNull(r.getValue(0).asObject());
                    Assert.assertNull(r.getValue(1).asObject());
                    Assert.assertEquals(r.getValue(2).asObject(), new long[0]);
                    break;
                case 2:
                    Assert.assertEquals(r.getValue(0).asObject(), "s");
                    Assert.assertNull(r.getValue(1).asObject());
                    Assert.assertEquals(r.getValue(2).asObject(), new long[0]);
                    break;
                case 4:
                    Assert.assertEquals(r.getValue(0).asObject(), "123");
                    Assert.assertEquals(r.getValue(1).asObject(), UnsignedByte.ONE);
                    Assert.assertEquals(r.getValue(2).asObject(), new int[] { 3, 2, 1 });
                    break;
                default:
                    Assert.fail("Should not fail");
                    break;
            }
            i++;
        }
    }

    @Test(groups = { "unit" })
    public void testFirstRecord() {
        ClickHouseResponse resp = ClickHouseSimpleResponse.of(config,
                ClickHouseColumn.parse("a Nullable(String), b UInt8, c String"),
                new Object[][] { new Object[] { "aaa", 2, "ccc" }, null });
        ClickHouseRecord record = resp.firstRecord();
        Assert.assertEquals(record.getValue("a"), ClickHouseStringValue.of("aaa"));
        Assert.assertEquals(record.getValue("b"), ClickHouseByteValue.ofUnsigned(2));
        Assert.assertEquals(record.getValue("c"), ClickHouseStringValue.of("ccc"));

        ClickHouseRecord sameRecord = resp.firstRecord();
        Assert.assertTrue(record == sameRecord);
    }

    @Test(groups = { "unit" })
    public void testRecords() {
        ClickHouseResponse resp = ClickHouseSimpleResponse.of(config,
                ClickHouseColumn.parse("a Nullable(String), b UInt8, c String"),
                new Object[][] { new Object[] { "aaa1", null, "ccc1" }, new Object[] { "aaa2", 2, "ccc2" },
                        new Object[] { null, 3L, null } });
        int i = 0;
        for (ClickHouseRecord r : resp.records()) {
            switch (i) {
                case 0:
                    Assert.assertEquals(r.getValue(0).asObject(), "aaa1");
                    Assert.assertNull(r.getValue(1).asObject());
                    Assert.assertEquals(r.getValue(2).asObject(), "ccc1");
                    break;
                case 1:
                    Assert.assertEquals(r.getValue("a").asObject(), "aaa2");
                    Assert.assertEquals(r.getValue("b").asObject(), UnsignedByte.valueOf((byte) 2));
                    Assert.assertEquals(r.getValue("c").asObject(), "ccc2");
                    break;
                case 2:
                    Assert.assertNull(r.getValue(0).asObject());
                    Assert.assertEquals(r.getValue(1).asObject(), UnsignedByte.valueOf((byte) 3));
                    Assert.assertNull(r.getValue(0).asObject());
                    break;
                default:
                    Assert.fail("Should not fail");
                    break;
            }
            i++;
        }
    }
}
