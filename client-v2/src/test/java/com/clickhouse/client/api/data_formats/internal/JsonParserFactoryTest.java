package com.clickhouse.client.api.data_formats.internal;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

@Test(groups = {"unit"})
public class JsonParserFactoryTest {

    private static InputStream emptyStream() {
        return new ByteArrayInputStream(new byte[0]);
    }

    @Test
    public void testCreateParserJackson() throws Exception {
        try (JsonParser parser = JsonParserFactory.createParser("JACKSON", emptyStream())) {
            Assert.assertTrue(parser instanceof JacksonJsonParser,
                    "Expected JacksonJsonParser but got " + parser.getClass().getName());
        }
    }

    @Test
    public void testCreateParserGson() throws Exception {
        try (JsonParser parser = JsonParserFactory.createParser("GSON", emptyStream())) {
            Assert.assertTrue(parser instanceof GsonJsonParser,
                    "Expected GsonJsonParser but got " + parser.getClass().getName());
        }
    }

    @Test
    public void testCreateParserIsCaseInsensitive() throws Exception {
        try (JsonParser jackson = JsonParserFactory.createParser("jackson", emptyStream());
             JsonParser gson = JsonParserFactory.createParser("Gson", emptyStream())) {
            Assert.assertTrue(jackson instanceof JacksonJsonParser);
            Assert.assertTrue(gson instanceof GsonJsonParser);
        }
    }

    @Test
    public void testCreateParserRejectsUnknownProcessor() {
        try {
            JsonParserFactory.createParser("FASTJSON", emptyStream());
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("FASTJSON"),
                    "Error message should mention the unsupported value: " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("JACKSON"),
                    "Error message should list JACKSON as supported: " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("GSON"),
                    "Error message should list GSON as supported: " + e.getMessage());
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testCreateParserRejectsEmptyType() {
        JsonParserFactory.createParser("", emptyStream());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testCreateParserRejectsNullType() {
        JsonParserFactory.createParser(null, emptyStream());
    }
}
