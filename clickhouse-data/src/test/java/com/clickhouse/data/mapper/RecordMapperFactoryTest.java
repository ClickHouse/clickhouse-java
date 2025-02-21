package com.clickhouse.data.mapper;

import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseRecordMapper;
import com.clickhouse.data.ClickHouseSimpleRecord;
import com.clickhouse.data.ClickHouseTestDataConfig;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.value.ClickHouseLongValue;
import com.clickhouse.data.value.ClickHouseStringValue;

public class RecordMapperFactoryTest {
    static class CompilationConfig extends ClickHouseTestDataConfig {
        @Override
        public boolean isUseCompilation() {
            return true;
        }
    }

    static interface TestInterface {
    }

    static final class TestPojo {
        final ClickHouseRecord r;

        private TestPojo(ClickHouseRecord r) {
            this.r = r;
        }

        private TestPojo() {
            this(null);
        }
    }

    @Test(groups = { "unit" })
    public void testGet() {
        ClickHouseDataConfig config = new ClickHouseTestDataConfig();
        Assert.assertThrows(NullPointerException.class, () -> RecordMapperFactory.get(null));

        Assert.assertNotNull(RecordMapperFactory.get(TestInterface.class));
        Assert.assertNotNull(RecordMapperFactory.get(TestInterface.class).get(null, null));
        Assert.assertNotNull(RecordMapperFactory.get(TestInterface.class).get(config, null));
        Assert.assertFalse(
                RecordMapperFactory.get(TestInterface.class) == RecordMapperFactory.get(TestInterface.class));
    }

    @Test(groups = { "unit" })
    public void testGetCustom() {
        ClickHouseDataConfig config = new ClickHouseTestDataConfig();
        List<ClickHouseColumn> columns = ClickHouseColumn.parse("id UInt64, str Nullable(String)");
        HashMap<String, Integer> columnsIndex = IntStream.range(0, columns.size()).boxed()
                .collect(HashMap::new, (m, i) -> m.put(columns.get(i).getColumnName(), i), HashMap::putAll);

        ClickHouseRecord r = ClickHouseSimpleRecord.of(columnsIndex,
                new ClickHouseValue[] { ClickHouseLongValue.ofUnsigned(5), ClickHouseStringValue.of("555...") });
        ClickHouseRecordMapper mapper = RecordMapperFactory.of(config, columns, Object.class);
        Assert.assertNotNull(mapper);
        Assert.assertNotNull(RecordMapperFactory.of(config, columns, Object.class).mapTo(r, null));
    }
}