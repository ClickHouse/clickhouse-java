package com.clickhouse.data.mapper;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseRecordMapper;
import com.clickhouse.data.ClickHouseSimpleRecord;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.value.ClickHouseStringValue;

public class CustomRecordMappersTest {
    static class TestPojo {
        ClickHouseRecord r;

        TestPojo() {
            this.r = null;
        }

        TestPojo(ClickHouseSimpleRecord r) {
            this.r = r;
        }
    }

    static class DerivedPojo extends TestPojo {
        static DerivedPojo magic(ClickHouseSimpleRecord r) {
            DerivedPojo p = new DerivedPojo();
            p.r = r;
            return p;
        }
    }

    static class ComplexPojo extends TestPojo {
        static AnotherPojo create(ClickHouseSimpleRecord r) {
            AnotherPojo p = new AnotherPojo();
            p.r = r;
            return p;
        }
    }

    static class AnotherPojo extends ComplexPojo {
    }

    @Test(groups = { "unit" })
    public void testCustomConstructor() throws Exception {
        List<ClickHouseColumn> c = ClickHouseColumn.parse("id UInt32, name String");
        ClickHouseRecord r = ClickHouseSimpleRecord.of(c,
                new ClickHouseValue[] { ClickHouseStringValue.ofNull(), ClickHouseStringValue.ofNull() });
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new CustomRecordMappers.RecordConstructor(Object.class, null).mapTo(r, null));

        ClickHouseRecordMapper m = new CustomRecordMappers.RecordConstructor(TestPojo.class,
                TestPojo.class.getDeclaredConstructor(ClickHouseSimpleRecord.class));
        Assert.assertTrue(r == m.mapTo(r, TestPojo.class).r);

        m = RecordMapperFactory.of(c, TestPojo.class);
        Assert.assertEquals(m.getClass(), CustomRecordMappers.RecordConstructor.class);
        Assert.assertTrue(r == m.mapTo(r, TestPojo.class).r);
    }

    @Test(groups = { "unit" })
    public void testCustomCreator() throws Exception {
        List<ClickHouseColumn> c = ClickHouseColumn.parse("id UInt32, name String");
        ClickHouseRecord r = ClickHouseSimpleRecord.of(c,
                new ClickHouseValue[] { ClickHouseStringValue.ofNull(), ClickHouseStringValue.ofNull() });
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new CustomRecordMappers.RecordCreator(Object.class, null).mapTo(r, null));

        ClickHouseRecordMapper m = new CustomRecordMappers.RecordCreator(DerivedPojo.class,
                DerivedPojo.class.getDeclaredMethods()[0]);
        Assert.assertTrue(r == m.mapTo(r, DerivedPojo.class).r);

        m = RecordMapperFactory.of(c, DerivedPojo.class);
        Assert.assertEquals(m.getClass(), CustomRecordMappers.RecordCreator.class);
        Assert.assertTrue(r == m.mapTo(r, DerivedPojo.class).r);

        m = RecordMapperFactory.of(c, ComplexPojo.class);
        Assert.assertEquals(m.getClass(), CustomRecordMappers.RecordCreator.class);
        Assert.assertTrue(r == m.mapTo(r, ComplexPojo.class).r);
    }
}