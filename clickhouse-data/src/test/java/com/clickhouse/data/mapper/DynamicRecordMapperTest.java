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
import com.clickhouse.data.value.ClickHouseIntegerValue;
import com.clickhouse.data.value.ClickHouseObjectValue;
import com.clickhouse.data.value.ClickHouseStringValue;
import com.clickhouse.data.value.array.ClickHouseIntArrayValue;

public class DynamicRecordMapperTest {
    static class PrivatePojo {
        private PrivatePojo() {
        }
    }

    static class SimplePojo {
        private int id;
        private String name;
        private String description;

        public SimplePojo() {
            this.id = 0;
            this.name = "";
            this.description = "";
        }

        public int getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName() {
            this.name = "";
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(ClickHouseObjectValue<?> description) {
            this.description = description.asString();
        }
    }

    public static class RecordPojo {
        private final int id;
        private final String name;
        private final int[] values;

        public RecordPojo(ClickHouseRecord r) {
            this.id = r.getValue("id").asInteger();
            this.name = r.getValue("code").asString();
            this.values = r.getValue("description").asObject(int[].class);
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public int[] getValues() {
            return values;
        }
    }

    @Test(groups = { "unit" })
    public void testInvalidPojo() {
        ClickHouseDataConfig config = new ClickHouseTestDataConfig();
        List<ClickHouseColumn> columns = ClickHouseColumn.parse("id String, value Int32");

        Assert.assertThrows(IllegalArgumentException.class,
                () -> new DynamicRecordMapper(ClickHouseRecord.class).get(config, columns).mapTo(ClickHouseRecord.EMPTY,
                        ClickHouseRecord.class));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new DynamicRecordMapper(ClickHouseRecord.class).get(config, columns).mapTo(ClickHouseRecord.EMPTY,
                        getClass()));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new DynamicRecordMapper(PrivatePojo.class).get(config, columns).mapTo(ClickHouseRecord.EMPTY,
                        PrivatePojo.class));
    }

    @Test(groups = { "unit" })
    public void testSimplePojo() {
        ClickHouseDataConfig config = new ClickHouseTestDataConfig();
        List<ClickHouseColumn> columns = ClickHouseColumn.parse("code String, id Int32, description Nullable(String)");
        ClickHouseValue[] values = new ClickHouseValue[] { ClickHouseStringValue.of("secret"),
                ClickHouseIntegerValue.of(5), ClickHouseIntArrayValue.of(new int[] { 1, 2, 3 }) };
        ClickHouseRecordMapper mapper = new DynamicRecordMapper(SimplePojo.class).get(config, columns);
        // Assert.assertThrows(IllegalArgumentException.class,
        // () -> mapper.mapTo(ClickHouseSimpleRecord.of(columns, values),
        // PrivatePojo.class));
        HashMap<String, Integer> columnsIndex = IntStream.range(0, columns.size()).boxed()
                .collect(HashMap::new, (m, i) -> m.put(columns.get(i).getColumnName(), i), HashMap::putAll);

        SimplePojo pojo = mapper.mapTo(ClickHouseSimpleRecord.of(columnsIndex, values), SimplePojo.class, new SimplePojo());
        Assert.assertNotNull(pojo, "Result should NOT be null");
        Assert.assertEquals(pojo.getId(), values[1].asInteger());
        Assert.assertEquals(pojo.getName(), "");
        Assert.assertEquals(pojo.getDescription(), values[2].asString());

        RecordPojo rpojo = RecordMapperFactory.of(config, columns, RecordPojo.class)
                .mapTo(ClickHouseSimpleRecord.of(columnsIndex, values), RecordPojo.class);
        Assert.assertNotNull(rpojo);
        Assert.assertEquals(rpojo.getId(), values[1].asInteger());
        Assert.assertEquals(rpojo.getName(), values[0].asString());
        Assert.assertEquals(rpojo.getValues(), values[2].asRawObject());
    }
}