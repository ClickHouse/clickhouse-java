package ru.yandex.clickhouse.domain;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ClickHouseFormatTest {

    @Test
    public void testNull() {
        assertFalse(ClickHouseFormat.containsFormat(null));
    }

    @Test
    public void testEmpty() {
        assertFalse(ClickHouseFormat.containsFormat(" \t \r\n"));
    }

    @Test
    public void testTrailingWhitespace() {
        assertFalse(ClickHouseFormat.containsFormat("Phantasy  "));
        assertTrue(ClickHouseFormat.containsFormat("TabSeparatedWithNamesAndTypes "));
        assertTrue(ClickHouseFormat.containsFormat("TabSeparatedWithNamesAndTypes \t \n"));
    }

    @Test
    public void testTrailingSemicolon() {
        assertFalse(ClickHouseFormat.containsFormat("Phantasy  ;"));
        assertTrue(ClickHouseFormat.containsFormat("TabSeparatedWithNamesAndTypes ; "));
        assertTrue(ClickHouseFormat.containsFormat("TabSeparatedWithNamesAndTypes ;"));
        assertTrue(ClickHouseFormat.containsFormat("TabSeparatedWithNamesAndTypes \t ; \n"));
    }

    @Test
    public void testAllFormats() {
        for (ClickHouseFormat format : ClickHouseFormat.values()) {
            assertTrue(ClickHouseFormat.containsFormat(format.name()));
        }
    }

}
