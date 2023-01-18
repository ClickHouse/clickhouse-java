package com.clickhouse.data.format;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseTestDataConfig;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.value.ClickHouseStringValue;

public class ApacheAvroProcessorTest {
    @Test(groups = { "unit" })
    public void testInit() throws IOException {
        List<ClickHouseColumn> list = ClickHouseColumn.parse("a String, b Nullable(Int32)");
        ClickHouseValue value = ClickHouseStringValue.of("aaa");
        ClickHouseDataConfig config = new ClickHouseTestDataConfig() {
            @Override
            public ClickHouseFormat getFormat() {
                return ClickHouseFormat.Avro;
            }
        };
        ApacheAvroProcessor p = new ApacheAvroProcessor(config,
                ClickHouseInputStream.of(new File("/Users/zhicwu/Sources/Github/a.avro")), null, null, null);
    }
}
