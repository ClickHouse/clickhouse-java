package com.clickhouse.client.api.data_formats.internal;

import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ProcessParserTest {

    /**
     * A non-numeric summary value must be skipped (and logged at DEBUG) while the remaining numeric
     * fields still parse: a single malformed field must not drop the rest of the summary.
     */
    @Test
    public void testParseSkipsNonNumericValuesAndKeepsTheRest() {
        Map<String, Long> parsed = ProcessParser.parse(
                "{\"read_rows\":\"100\",\"bogus\":\"not_a_number\",\"read_bytes\":\"2048\"}");

        assertEquals(parsed.get("read_rows"), Long.valueOf(100L),
                "a numeric field before the bad one must parse");
        assertEquals(parsed.get("read_bytes"), Long.valueOf(2048L),
                "a numeric field after the bad one must still parse");
        assertFalse(parsed.containsKey("bogus"),
                "the non-numeric field must be skipped, not stored");
    }

    @Test
    public void testParseReadsAllNumericFields() {
        Map<String, Long> parsed = ProcessParser.parse("{\"read_rows\":\"7\",\"written_rows\":\"3\"}");
        assertEquals(parsed.get("read_rows"), Long.valueOf(7L));
        assertEquals(parsed.get("written_rows"), Long.valueOf(3L));
    }

    @Test
    public void testParseEmptyObjectReturnsEmptyMap() {
        assertTrue(ProcessParser.parse("{}").isEmpty(),
                "an empty summary object must yield an empty map");
    }
}
