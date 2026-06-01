package com.clickhouse.client.api.internal;

import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

@Test(groups = {"unit"})
public class MapUtilsTest {

    // ---------- applyLong ----------

    @Test
    public void applyLong_appliesConsumerWhenKeyPresent() {
        Map<String, String> map = new HashMap<>();
        map.put("k", "42");
        AtomicLong sink = new AtomicLong(-1);

        MapUtils.applyLong(map, "k", sink::set);

        assertEquals(sink.get(), 42L);
    }

    @Test
    public void applyLong_isNoOpWhenKeyAbsent() {
        Map<String, String> map = new HashMap<>();
        AtomicReference<Long> sink = new AtomicReference<>(null);

        MapUtils.applyLong(map, "missing", sink::set);

        assertNull(sink.get());
    }

    @Test
    public void applyLong_isNoOpWhenValueIsNull() {
        Map<String, String> map = new HashMap<>();
        map.put("k", null);
        AtomicReference<Long> sink = new AtomicReference<>(null);

        MapUtils.applyLong(map, "k", sink::set);

        assertNull(sink.get());
    }

    @Test
    public void applyLong_throwsRuntimeOnInvalidNumber() {
        Map<String, String> map = new HashMap<>();
        map.put("k", "not-a-number");

        RuntimeException ex = expectThrows(RuntimeException.class,
                () -> MapUtils.applyLong(map, "k", v -> {}));
        assertTrue(ex.getMessage().contains("k"));
        assertTrue(ex.getMessage().contains("not-a-number"));
        assertTrue(ex.getCause() instanceof NumberFormatException);
    }

    // ---------- applyInt ----------

    @Test
    public void applyInt_appliesConsumerWhenKeyPresent() {
        Map<String, String> map = new HashMap<>();
        map.put("k", "7");
        AtomicReference<Integer> sink = new AtomicReference<>(null);

        MapUtils.applyInt(map, "k", sink::set);

        assertEquals(sink.get(), Integer.valueOf(7));
    }

    @Test
    public void applyInt_isNoOpWhenKeyAbsent() {
        Map<String, String> map = new HashMap<>();
        AtomicReference<Integer> sink = new AtomicReference<>(null);

        MapUtils.applyInt(map, "missing", sink::set);

        assertNull(sink.get());
    }

    @Test
    public void applyInt_throwsRuntimeOnInvalidNumber() {
        Map<String, String> map = new HashMap<>();
        map.put("k", "abc");

        RuntimeException ex = expectThrows(RuntimeException.class,
                () -> MapUtils.applyInt(map, "k", v -> {}));
        assertTrue(ex.getMessage().contains("k"));
        assertTrue(ex.getMessage().contains("abc"));
        assertTrue(ex.getCause() instanceof NumberFormatException);
    }

    // ---------- getInt ----------

    @Test
    public void getInt_returnsParsedValue() {
        Map<String, String> map = new HashMap<>();
        map.put("k", "123");

        assertEquals(MapUtils.getInt(map, "k"), 123);
    }

    @Test
    public void getInt_returnsZeroWhenKeyAbsent() {
        assertEquals(MapUtils.getInt(new HashMap<>(), "missing"), 0);
    }

    @Test
    public void getInt_handlesNegativeAndBoundaryValues() {
        Map<String, String> map = new HashMap<>();
        map.put("max", String.valueOf(Integer.MAX_VALUE));
        map.put("min", String.valueOf(Integer.MIN_VALUE));
        map.put("neg", "-42");

        assertEquals(MapUtils.getInt(map, "max"), Integer.MAX_VALUE);
        assertEquals(MapUtils.getInt(map, "min"), Integer.MIN_VALUE);
        assertEquals(MapUtils.getInt(map, "neg"), -42);
    }

    @Test
    public void getInt_throwsRuntimeOnInvalidNumber() {
        Map<String, String> map = new HashMap<>();
        map.put("k", "1.5");

        RuntimeException ex = expectThrows(RuntimeException.class, () -> MapUtils.getInt(map, "k"));
        assertTrue(ex.getMessage().contains("k"));
        assertTrue(ex.getCause() instanceof NumberFormatException);
    }

    // ---------- getLong ----------

    @Test
    public void getLong_returnsParsedValue() {
        Map<String, String> map = new HashMap<>();
        map.put("k", "9876543210");

        assertEquals(MapUtils.getLong(map, "k"), 9876543210L);
    }

    @Test
    public void getLong_returnsZeroWhenKeyAbsent() {
        assertEquals(MapUtils.getLong(new HashMap<>(), "missing"), 0L);
    }

    @Test
    public void getLong_handlesBoundaryValues() {
        Map<String, String> map = new HashMap<>();
        map.put("max", String.valueOf(Long.MAX_VALUE));
        map.put("min", String.valueOf(Long.MIN_VALUE));

        assertEquals(MapUtils.getLong(map, "max"), Long.MAX_VALUE);
        assertEquals(MapUtils.getLong(map, "min"), Long.MIN_VALUE);
    }

    @Test
    public void getLong_throwsRuntimeOnInvalidNumber() {
        Map<String, String> map = new HashMap<>();
        map.put("k", "foo");

        RuntimeException ex = expectThrows(RuntimeException.class, () -> MapUtils.getLong(map, "k"));
        assertTrue(ex.getMessage().contains("k"));
        assertTrue(ex.getCause() instanceof NumberFormatException);
    }

    // ---------- getFlag(map, key) ----------

    @Test
    public void getFlag_returnsTrueIgnoringCase() {
        Map<String, String> map = new HashMap<>();
        map.put("a", "true");
        map.put("b", "TRUE");
        map.put("c", "True");

        assertTrue(MapUtils.getFlag(map, "a"));
        assertTrue(MapUtils.getFlag(map, "b"));
        assertTrue(MapUtils.getFlag(map, "c"));
    }

    @Test
    public void getFlag_returnsFalseIgnoringCase() {
        Map<String, String> map = new HashMap<>();
        map.put("a", "false");
        map.put("b", "FALSE");
        map.put("c", "False");

        assertFalse(MapUtils.getFlag(map, "a"));
        assertFalse(MapUtils.getFlag(map, "b"));
        assertFalse(MapUtils.getFlag(map, "c"));
    }

    @Test
    public void getFlag_throwsNpeWhenKeyMissing() {
        NullPointerException ex = expectThrows(NullPointerException.class,
                () -> MapUtils.getFlag(new HashMap<>(), "missing"));
        assertTrue(ex.getMessage().contains("missing"));
    }

    @Test
    public void getFlag_throwsOnInvalidValue() {
        Map<String, String> map = new HashMap<>();
        map.put("k", "yes");

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> MapUtils.getFlag(map, "k"));
        assertTrue(ex.getMessage().contains("k"));
        assertTrue(ex.getMessage().contains("yes"));
    }

    // ---------- getFlag(map, key, defaultValue) ----------

    @Test
    public void getFlagWithDefault_returnsDefaultWhenKeyMissing() {
        assertTrue(MapUtils.getFlag(new HashMap<String, Object>(), "missing", true));
        assertFalse(MapUtils.getFlag(new HashMap<String, Object>(), "missing", false));
    }

    @Test
    public void getFlagWithDefault_returnsDefaultWhenValueIsNull() {
        Map<String, Object> map = new HashMap<>();
        map.put("k", null);

        assertTrue(MapUtils.getFlag(map, "k", true));
        assertFalse(MapUtils.getFlag(map, "k", false));
    }

    @Test
    public void getFlagWithDefault_unwrapsBooleanValue() {
        Map<String, Object> map = new HashMap<>();
        map.put("t", Boolean.TRUE);
        map.put("f", Boolean.FALSE);

        assertTrue(MapUtils.getFlag(map, "t", false));
        assertFalse(MapUtils.getFlag(map, "f", true));
    }

    @Test
    public void getFlagWithDefault_acceptsStringTrueFalseOneZero() {
        Map<String, Object> map = new HashMap<>();
        map.put("a", "true");
        map.put("b", "TRUE");
        map.put("c", "1");
        map.put("d", "false");
        map.put("e", "FALSE");
        map.put("f", "0");

        assertTrue(MapUtils.getFlag(map, "a", false));
        assertTrue(MapUtils.getFlag(map, "b", false));
        assertTrue(MapUtils.getFlag(map, "c", false));
        assertFalse(MapUtils.getFlag(map, "d", true));
        assertFalse(MapUtils.getFlag(map, "e", true));
        assertFalse(MapUtils.getFlag(map, "f", true));
    }

    @Test
    public void getFlagWithDefault_throwsOnInvalidString() {
        Map<String, Object> map = new HashMap<>();
        map.put("k", "yes");

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> MapUtils.getFlag(map, "k", false));
        assertTrue(ex.getMessage().contains("k"));
        assertTrue(ex.getMessage().contains("yes"));
    }

    @Test
    public void getFlagWithDefault_throwsOnUnsupportedType() {
        Map<String, Object> map = new HashMap<>();
        map.put("k", 1);

        assertThrows(IllegalArgumentException.class, () -> MapUtils.getFlag(map, "k", false));
    }

    // ---------- getFlag(p1, p2, key) ----------

    @Test
    public void getFlagTwoMaps_prefersFirstMap() {
        Map<String, Object> p1 = new HashMap<>();
        Map<String, Object> p2 = new HashMap<>();
        p1.put("k", "true");
        p2.put("k", "false");

        assertTrue(MapUtils.getFlag(p1, p2, "k"));
    }

    @Test
    public void getFlagTwoMaps_fallsBackToSecondMapWhenAbsentInFirst() {
        Map<String, Object> p1 = new HashMap<>();
        Map<String, Object> p2 = new HashMap<>();
        p2.put("k", "true");

        assertTrue(MapUtils.getFlag(p1, p2, "k"));
    }

    @Test
    public void getFlagTwoMaps_fallsBackToSecondMapWhenNullInFirst() {
        Map<String, Object> p1 = new HashMap<>();
        Map<String, Object> p2 = new HashMap<>();
        p1.put("k", null);
        p2.put("k", "false");

        assertFalse(MapUtils.getFlag(p1, p2, "k"));
    }

    @Test
    public void getFlagTwoMaps_unwrapsBooleanValue() {
        Map<String, Object> p1 = new HashMap<>();
        Map<String, Object> p2 = new HashMap<>();
        p1.put("k", Boolean.TRUE);

        assertTrue(MapUtils.getFlag(p1, p2, "k"));
    }

    @Test
    public void getFlagTwoMaps_throwsNpeWhenMissingFromBoth() {
        NullPointerException ex = expectThrows(NullPointerException.class,
                () -> MapUtils.getFlag(new HashMap<>(), new HashMap<>(), "missing"));
        assertTrue(ex.getMessage().contains("missing"));
    }

    @Test
    public void getFlagTwoMaps_throwsOnInvalidStringValue() {
        Map<String, Object> p1 = new HashMap<>();
        Map<String, Object> p2 = new HashMap<>();
        p1.put("k", "maybe");

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> MapUtils.getFlag(p1, p2, "k"));
        assertTrue(ex.getMessage().contains("k"));
        assertTrue(ex.getMessage().contains("maybe"));
    }

    @Test
    public void getFlagTwoMaps_throwsOnUnsupportedType() {
        Map<String, Object> p1 = new HashMap<>();
        Map<String, Object> p2 = new HashMap<>();
        p1.put("k", 42);

        assertThrows(IllegalArgumentException.class, () -> MapUtils.getFlag(p1, p2, "k"));
    }

    @Test
    public void getFlagTwoMaps_emptyMapsThrowNpe() {
        assertThrows(NullPointerException.class,
                () -> MapUtils.getFlag(Collections.emptyMap(), Collections.emptyMap(), "k"));
    }
}
