package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.data_formats.internal.JsonParser;
import com.clickhouse.client.api.data_formats.internal.JsonParserFactory;
import com.clickhouse.data.ClickHouseDataType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public abstract class AbstractJSONEachRowFormatReaderTest {

    protected abstract String getProcessor();

    @Test
    public void testBasicParsing() throws Exception {
        String json = "{\"id\":1,\"name\":\"test\",\"active\":true}\n" +
                      "{\"id\":2,\"name\":\"clickhouse\",\"active\":false}";
        
        try (InputStream in = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
             JsonParser parser = JsonParserFactory.createParser(getProcessor(), in);
             JSONEachRowFormatReader reader = new JSONEachRowFormatReader(parser)) {
            
            // First row
            Assert.assertTrue(reader.hasNext());
            Map<String, Object> row1 = reader.next();
            Assert.assertNotNull(row1);
            Assert.assertEquals(reader.getInteger("id"), 1);
            Assert.assertEquals(reader.getString("name"), "test");
            Assert.assertEquals(reader.getBoolean("active"), true);
            
            // Second row
            Assert.assertTrue(reader.hasNext());
            Map<String, Object> row2 = reader.next();
            Assert.assertNotNull(row2);
            Assert.assertEquals(reader.getInteger("id"), 2);
            Assert.assertEquals(reader.getString("name"), "clickhouse");
            Assert.assertEquals(reader.getBoolean("active"), false);
            
            // No more rows
            Assert.assertNull(reader.next());
        }
    }

    @Test
    public void testSchemaInference() throws Exception {
        String json = "{\"col_int\":42,\"col_float\":3.14,\"col_bool\":true,\"col_str\":\"val\"}";
        
        try (InputStream in = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
             JsonParser parser = JsonParserFactory.createParser(getProcessor(), in);
             JSONEachRowFormatReader reader = new JSONEachRowFormatReader(parser)) {
            
            Assert.assertNotNull(reader.getSchema());
            Assert.assertEquals(reader.getSchema().getColumns().size(), 4);
            
            Assert.assertEquals(reader.getSchema().getColumnByIndex(1).getDataType(), ClickHouseDataType.Int64);
            Assert.assertEquals(reader.getSchema().getColumnByIndex(2).getDataType(), ClickHouseDataType.Float64);
            Assert.assertEquals(reader.getSchema().getColumnByIndex(3).getDataType(), ClickHouseDataType.Bool);
            Assert.assertEquals(reader.getSchema().getColumnByIndex(4).getDataType(), ClickHouseDataType.String);
        }
    }

    @Test
    public void testDataTypes() throws Exception {
        String json = "{\"b\":120,\"s\":30000,\"i\":1000000,\"l\":10000000000,\"f\":1.23,\"d\":1.23456789,\"bool\":true,\"str\":\"hello\"}";
        
        try (InputStream in = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
             JsonParser parser = JsonParserFactory.createParser(getProcessor(), in);
             JSONEachRowFormatReader reader = new JSONEachRowFormatReader(parser)) {
            
            reader.next();
            Assert.assertEquals(reader.getByte("b"), (byte) 120);
            Assert.assertEquals(reader.getShort("s"), (short) 30000);
            Assert.assertEquals(reader.getInteger("i"), 1000000);
            Assert.assertEquals(reader.getLong("l"), 10000000000L);
            Assert.assertEquals(reader.getFloat("f"), 1.23f, 0.001f);
            Assert.assertEquals(reader.getDouble("d"), 1.23456789d, 0.00000001d);
            Assert.assertEquals(reader.getBoolean("bool"), true);
            Assert.assertEquals(reader.getString("str"), "hello");
            
            Assert.assertEquals(reader.getBigInteger("l"), BigInteger.valueOf(10000000000L));
            Assert.assertEquals(reader.getBigDecimal("d").doubleValue(), 1.23456789d, 0.00000001d);
        }
    }

    @Test
    public void testEmptyData() throws Exception {
        String json = "";
        
        try (InputStream in = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
             JsonParser parser = JsonParserFactory.createParser(getProcessor(), in);
             JSONEachRowFormatReader reader = new JSONEachRowFormatReader(parser)) {
            
            Assert.assertFalse(reader.hasNext());
            Assert.assertNull(reader.next());
            Assert.assertEquals(reader.getSchema().getColumns().size(), 0);
        }
    }

    @Test
    public void testMixedNewlines() throws Exception {
        // JSONEachRow often has newlines between objects
        String json = "{\"id\":1}\n\n\r\n{\"id\":2}";
        
        try (InputStream in = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
             JsonParser parser = JsonParserFactory.createParser(getProcessor(), in);
             JSONEachRowFormatReader reader = new JSONEachRowFormatReader(parser)) {
            
            Assert.assertTrue(reader.hasNext());
            reader.next();
            Assert.assertEquals(reader.getInteger("id"), 1);
            
            reader.next();
            Assert.assertEquals(reader.getInteger("id"), 2);
            
            Assert.assertNull(reader.next());
        }
    }
}
