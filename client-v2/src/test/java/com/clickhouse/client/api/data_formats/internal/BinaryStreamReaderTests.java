package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.format.BinaryStreamUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.LocalDate;
import java.util.TimeZone;

public class BinaryStreamReaderTests {


    @Test(groups = {"unit"})
    public void testDateColumns() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream(1000);
        LocalDate inValue = LocalDate.of(2021, 1, 1);
        BinaryStreamUtils.writeDate(out, inValue);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        LocalDate outValue = new BinaryStreamReader(ClickHouseInputStream.of(in), null)
                .readValue(ClickHouseDataType.Date, TimeZone.getDefault());
        Assert.assertEquals(outValue, inValue);
    }
}
