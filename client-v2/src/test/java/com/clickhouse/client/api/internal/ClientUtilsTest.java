package com.clickhouse.client.api.internal;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

@Test(groups = {"unit"})
public class ClientUtilsTest {

    // ---------- isBlank ----------

    @Test
    public void isBlank_returnsTrueForNull() {
        assertTrue(ClientUtils.isBlank(null));
    }

    @Test
    public void isBlank_returnsTrueForEmpty() {
        assertTrue(ClientUtils.isBlank(""));
    }

    @Test
    public void isBlank_returnsTrueForWhitespaceOnly() {
        assertTrue(ClientUtils.isBlank(" "));
        assertTrue(ClientUtils.isBlank("\t"));
        assertTrue(ClientUtils.isBlank("\n"));
        assertTrue(ClientUtils.isBlank("  \t \n "));
    }

    @Test
    public void isBlank_returnsFalseForNonBlank() {
        assertFalse(ClientUtils.isBlank("a"));
        assertFalse(ClientUtils.isBlank(" a "));
        assertFalse(ClientUtils.isBlank("hello world"));
    }

    // ---------- defaultIfBlank ----------

    @Test
    public void defaultIfBlank_returnsDefaultWhenNull() {
        assertEquals(ClientUtils.defaultIfBlank(null, "fallback"), "fallback");
    }

    @Test
    public void defaultIfBlank_returnsDefaultWhenEmpty() {
        assertEquals(ClientUtils.defaultIfBlank("", "fallback"), "fallback");
    }

    @Test
    public void defaultIfBlank_returnsDefaultWhenWhitespace() {
        assertEquals(ClientUtils.defaultIfBlank("   ", "fallback"), "fallback");
        assertEquals(ClientUtils.defaultIfBlank("\t\n", "fallback"), "fallback");
    }

    @Test
    public void defaultIfBlank_returnsValueWhenNonBlank() {
        String value = "hello";
        assertSame(ClientUtils.defaultIfBlank(value, "fallback"), value);
    }

    @Test
    public void defaultIfBlank_returnsValueWithSurroundingWhitespace() {
        assertEquals(ClientUtils.defaultIfBlank(" x ", "fallback"), " x ");
    }

    @Test
    public void defaultIfBlank_returnsNullDefaultWhenBothBlank() {
        assertNull(ClientUtils.defaultIfBlank(null, null));
        assertNull(ClientUtils.defaultIfBlank("  ", null));
    }

    // ---------- truncate ----------

    @Test
    public void truncate_returnsNullWhenValueIsNull() {
        assertNull(ClientUtils.truncate(null, 10));
        assertNull(ClientUtils.truncate(null, 0));
    }

    @Test
    public void truncate_returnsSameInstanceWhenAlreadyShortEnough() {
        String value = "abc";
        assertSame(ClientUtils.truncate(value, 3), value);
        assertSame(ClientUtils.truncate(value, 10), value);
    }

    @Test
    public void truncate_shortensLongerString() {
        assertEquals(ClientUtils.truncate("abcdef", 3), "abc");
        assertEquals(ClientUtils.truncate("abcdef", 5), "abcde");
        assertEquals(ClientUtils.truncate("abcdef", 0), "");
    }

    @Test
    public void truncate_returnsEmptyStringForEmptyInput() {
        assertEquals(ClientUtils.truncate("", 0), "");
        assertEquals(ClientUtils.truncate("", 5), "");
    }

    @Test
    public void truncate_throwsOnNegativeMaxLength() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> ClientUtils.truncate("abc", -1));
        assertTrue(ex.getMessage().contains("-1"));
    }
}
