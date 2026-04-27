package com.clickhouse.client.api.data_formats.internal;

import com.fasterxml.jackson.core.JsonParseException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Test(groups = {"unit"})
public class JacksonJsonParserTest {

    private static InputStream input(String json) {
        return new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testReadsSingleRow() throws Exception {
        try (JacksonJsonParser parser = new JacksonJsonParser(
                input("{\"id\": 1, \"name\": \"a\"}"))) {
            Map<String, Object> row = parser.nextRow();
            Assert.assertNotNull(row);
            Assert.assertEquals(((Number) row.get("id")).intValue(), 1);
            Assert.assertEquals(row.get("name"), "a");

            // After the only row, EOF must be reported as null.
            Assert.assertNull(parser.nextRow());
        }
    }

    @Test
    public void testReadsMultipleRowsSeparatedByWhitespace() throws Exception {
        // JSONEachRow output is a sequence of JSON objects separated by
        // newlines. Jackson skips whitespace between tokens, so the parser
        // must transparently advance to each subsequent object.
        String body = "{\"id\":1}\n{\"id\":2}\n  {\"id\":3}\n";
        try (JacksonJsonParser parser = new JacksonJsonParser(input(body))) {
            for (int expected : Arrays.asList(1, 2, 3)) {
                Map<String, Object> row = parser.nextRow();
                Assert.assertNotNull(row, "row " + expected + " should not be null");
                Assert.assertEquals(((Number) row.get("id")).intValue(), expected);
            }
            Assert.assertNull(parser.nextRow());
        }
    }

    @Test
    public void testEmptyInputReturnsNull() throws Exception {
        try (JacksonJsonParser parser = new JacksonJsonParser(input(""))) {
            Assert.assertNull(parser.nextRow());
        }
    }

    @Test
    public void testWhitespaceOnlyInputReturnsNull() throws Exception {
        try (JacksonJsonParser parser = new JacksonJsonParser(input("   \n\n  "))) {
            Assert.assertNull(parser.nextRow());
        }
    }

    @Test
    public void testRepeatedNextRowAfterExhaustionRemainsNull() throws Exception {
        try (JacksonJsonParser parser = new JacksonJsonParser(input("{\"id\":1}"))) {
            Assert.assertNotNull(parser.nextRow());
            Assert.assertNull(parser.nextRow());
            Assert.assertNull(parser.nextRow());
        }
    }

    @Test(expectedExceptions = JsonParseException.class)
    public void testMalformedInputPropagatesParseException() throws Exception {
        try (JacksonJsonParser parser = new JacksonJsonParser(input("{not valid json"))) {
            parser.nextRow();
        }
    }

    @Test
    public void testCloseClosesUnderlyingStream() throws Exception {
        AtomicBoolean closed = new AtomicBoolean(false);
        InputStream stream = new ByteArrayInputStream("{\"id\":1}".getBytes(StandardCharsets.UTF_8)) {
            @Override
            public void close() throws IOException {
                closed.set(true);
                super.close();
            }
        };

        JacksonJsonParser parser = new JacksonJsonParser(stream);
        parser.close();
        Assert.assertTrue(closed.get(), "Underlying input stream should be closed");
    }

}
