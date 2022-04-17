package com.clickhouse.client.data;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseDataProcessor;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHouseOutputStream;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseOption;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClickHouseTabSeparatedProcessorTest {
    @DataProvider(name = "simpleTsvDataProvider")
    private Object[][] getSimpleTsvData() {
        return new Object[][] {
                { UUID.randomUUID().toString(), 0 },
                { UUID.randomUUID().toString(), 1 },
                { UUID.randomUUID().toString(), 2 },
                { UUID.randomUUID().toString(), 3 },
                { UUID.randomUUID().toString(), 256 } };
    }

    @Test(dataProvider = "simpleTsvDataProvider", groups = { "unit" })
    public void testRead(String id, int rows) throws IOException {
        String result = String.format("\\'%s\\'\nString\n", id);
        for (int i = 0; i < rows; i++) {
            result += String.format("%s\n", id);
        }
        Map<ClickHouseOption, Serializable> options = new HashMap<>();
        options.put(ClickHouseClientOption.FORMAT, ClickHouseFormat.TabSeparatedWithNamesAndTypes);
        ClickHouseConfig config = new ClickHouseConfig(options, null, null, null);
        ClickHouseInputStream input = ClickHouseInputStream.of(Collections.singletonList(result), String.class,
                s -> s.getBytes(), null);
        ClickHouseTabSeparatedProcessor p = new ClickHouseTabSeparatedProcessor(config, input, null, null, null);
        Assert.assertEquals(p.getColumns(),
                Collections.singletonList(ClickHouseColumn.of(String.format("'%s'", id), "String")));
        int count = 0;
        for (ClickHouseRecord r : p.records()) {
            Assert.assertEquals(r.getValue(0).asString(), id);
            count++;
        }
        Assert.assertEquals(count, rows);
    }

    @Test(groups = { "unit" })
    public void testWriteTsv() throws IOException {
        List<ClickHouseColumn> list = ClickHouseColumn.parse("a String, b Nullable(Int32)");
        ClickHouseValue value = ClickHouseStringValue.of("aaa");
        Map<ClickHouseOption, Serializable> options = new HashMap<>();
        options.put(ClickHouseClientOption.FORMAT, ClickHouseFormat.TabSeparated);
        ClickHouseConfig config = new ClickHouseConfig(options, null, null, null);
        try (ByteArrayOutputStream bas = new ByteArrayOutputStream();
                ClickHouseOutputStream out = ClickHouseOutputStream.of(bas)) {
            ClickHouseTabSeparatedProcessor p = new ClickHouseTabSeparatedProcessor(config, null, out,
                    ClickHouseDataProcessor.DEFAULT_COLUMNS, null);
            p.write(value, ClickHouseDataProcessor.DEFAULT_COLUMNS.get(0));
            out.flush();
            Assert.assertEquals(bas.toByteArray(), new byte[] { 97, 97, 97, 10 });
        }

        for (int i = 1; i <= 6; i++) {
            try (ByteArrayOutputStream bas = new ByteArrayOutputStream();
                    ClickHouseOutputStream out = ClickHouseOutputStream.of(bas, i)) {
                ClickHouseTabSeparatedProcessor p = new ClickHouseTabSeparatedProcessor(config, null, out, list, null);
                p.write(value, list.get(0));
                p.write(ClickHouseEmptyValue.INSTANCE, list.get(1));
                out.flush();
                Assert.assertEquals(bas.toByteArray(), new byte[] { 97, 97, 97, 9, 92, 78, 10 });
            }
        }
    }
}
