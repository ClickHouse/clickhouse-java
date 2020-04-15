package ru.yandex.clickhouse.util.apache;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Unit tests for {@link StringUtils}.
 */
public class StringUtilsTest {

    @Test
    public void testIsBlank() {
        assertTrue(StringUtils.isBlank(""));
        assertTrue(StringUtils.isBlank(" "));
        assertTrue(StringUtils.isBlank(null));

        assertFalse(StringUtils.isBlank("not_blank"));
    }

    @Test
    public void testStartWith() {
        assertTrue(StringUtils.startsWithIgnoreCase("blabla", "bla"));
        assertTrue(StringUtils.startsWithIgnoreCase("bla", "bla"));

        assertFalse(StringUtils.startsWithIgnoreCase("hello", "bla"));
    }

    @Test
    public void testRetainUnquoted() {
        assertEquals(StringUtils.retainUnquoted("select'something'from",'\''),"selectfrom");
    }
}
