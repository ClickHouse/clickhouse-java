package com.clickhouse.client.internal;

import com.clickhouse.client.api.data_formats.internal.SerializerUtils;
import org.testng.annotations.Test;

import static org.junit.Assert.assertEquals;

public class SerializerUtilsTests {

    @Test
    public void testConvertToInteger() {
        int expected = 1640995199; // Unix timestamp for the given date
        assertEquals(expected, SerializerUtils.convertToInteger("1640995199").intValue());
        assertEquals(0, SerializerUtils.convertToInteger(false).intValue());
    }
}
