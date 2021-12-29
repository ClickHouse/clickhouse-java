package com.clickhouse.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseNodeSelectorTest {
    @SuppressWarnings({ "unchecked", "varargs" })
    private <T> List<T> listOf(T... values) {
        List<T> list = new ArrayList<>(values.length);
        for (T value : values) {
            list.add(value);
        }
        return Collections.unmodifiableList(list);
    }

    @SuppressWarnings({ "unchecked", "varargs" })
    private <T> Set<T> setOf(T... values) {
        Set<T> set = new HashSet<>();
        for (T value : values) {
            set.add(value);
        }
        return Collections.unmodifiableSet(set);
    }

    @Test(groups = { "unit" })
    public void testBuilder() {
        Assert.assertTrue(ClickHouseNodeSelector.of((Collection<ClickHouseProtocol>) null,
                (Collection<String>) null) == ClickHouseNodeSelector.EMPTY);
        Assert.assertTrue(
                ClickHouseNodeSelector.of(new ArrayList<ClickHouseProtocol>(), null) == ClickHouseNodeSelector.EMPTY);
        Assert.assertTrue(ClickHouseNodeSelector.of(null, new ArrayList<String>()) == ClickHouseNodeSelector.EMPTY);
        Assert.assertTrue(ClickHouseNodeSelector.of(new ArrayList<ClickHouseProtocol>(),
                new ArrayList<String>()) == ClickHouseNodeSelector.EMPTY);
    }

    @Test(groups = { "unit" })
    public void testGetPreferredProtocols() {
        Assert.assertEquals(ClickHouseNodeSelector.of(listOf((ClickHouseProtocol) null), null).getPreferredProtocols(),
                Collections.emptyList());
        Assert.assertEquals(ClickHouseNodeSelector.of(listOf(ClickHouseProtocol.ANY), null).getPreferredProtocols(),
                Collections.emptyList());
        Assert.assertEquals(ClickHouseNodeSelector.of(listOf(ClickHouseProtocol.HTTP, ClickHouseProtocol.ANY), null)
                .getPreferredProtocols(), Collections.emptyList());

        Assert.assertEquals(ClickHouseNodeSelector
                .of(listOf(ClickHouseProtocol.HTTP, ClickHouseProtocol.GRPC, ClickHouseProtocol.HTTP), null)
                .getPreferredProtocols(), listOf(ClickHouseProtocol.HTTP, ClickHouseProtocol.GRPC));
    }

    @Test(groups = { "unit" })
    public void testGetPreferredTags() {
        Assert.assertEquals(ClickHouseNodeSelector.of(null, listOf((String) null)).getPreferredTags(),
                Collections.emptySet());
        Assert.assertEquals(ClickHouseNodeSelector.of(null, listOf("")).getPreferredTags(), Collections.emptySet());

        Assert.assertEquals(ClickHouseNodeSelector.of(null, listOf("A", "C", "D", "C", "B")).getPreferredTags(),
                setOf("A", "C", "D", "B"));
        Assert.assertEquals(ClickHouseNodeSelector.of(null, setOf("A", "C", "D", "C", "B")).getPreferredTags(),
                setOf("A", "C", "D", "B"));
    }

    @Test(groups = { "unit" })
    public void testMatchAnyOfPreferredProtocols() {
        ClickHouseNodeSelector selector = ClickHouseNodeSelector.of(listOf((ClickHouseProtocol) null), null);
        for (ClickHouseProtocol p : ClickHouseProtocol.values()) {
            Assert.assertTrue(selector.matchAnyOfPreferredProtocols(p));
        }

        selector = ClickHouseNodeSelector.of(listOf(ClickHouseProtocol.ANY), null);
        for (ClickHouseProtocol p : ClickHouseProtocol.values()) {
            Assert.assertTrue(selector.matchAnyOfPreferredProtocols(p));
        }

        for (ClickHouseProtocol protocol : ClickHouseProtocol.values()) {
            if (protocol == ClickHouseProtocol.ANY) {
                continue;
            }

            selector = ClickHouseNodeSelector.of(listOf(protocol), null);
            for (ClickHouseProtocol p : ClickHouseProtocol.values()) {
                if (p == ClickHouseProtocol.ANY || p == protocol) {
                    Assert.assertTrue(selector.matchAnyOfPreferredProtocols(p));
                } else {
                    Assert.assertFalse(selector.matchAnyOfPreferredProtocols(p));
                }
            }
        }
    }

    @Test(groups = { "unit" })
    public void testMatchAllPreferredTags() {
        List<String> tags = listOf((String) null);
        ClickHouseNodeSelector selector = ClickHouseNodeSelector.of(null, tags);
        Assert.assertTrue(selector.matchAllPreferredTags(Collections.emptyList()));

        selector = ClickHouseNodeSelector.of(null, tags = listOf((String) null, ""));
        Assert.assertTrue(selector.matchAllPreferredTags(Collections.emptyList()));

        selector = ClickHouseNodeSelector.of(null, tags = listOf("1", "3", "2"));
        Assert.assertTrue(selector.matchAllPreferredTags(tags));
        Assert.assertTrue(selector.matchAllPreferredTags(listOf("2", "2", "1", "3", "3")));
    }

    @Test(groups = { "unit" })
    public void testMatchAnyOfPreferredTags() {
        List<String> tags = listOf((String) null);
        ClickHouseNodeSelector selector = ClickHouseNodeSelector.of(null, tags);
        Assert.assertTrue(selector.matchAnyOfPreferredTags(Collections.emptyList()));

        selector = ClickHouseNodeSelector.of(null, tags = listOf((String) null, ""));
        Assert.assertTrue(selector.matchAnyOfPreferredTags(Collections.emptyList()));

        selector = ClickHouseNodeSelector.of(null, tags = listOf("1", "3", "2"));
        for (String tag : tags) {
            Assert.assertTrue(selector.matchAnyOfPreferredTags(Collections.singleton(tag)));
        }
        Assert.assertTrue(selector.matchAnyOfPreferredTags(listOf("v", "3", "5")));
    }
}
