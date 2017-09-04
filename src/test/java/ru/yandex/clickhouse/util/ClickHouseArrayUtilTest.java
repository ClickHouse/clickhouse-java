package ru.yandex.clickhouse.util;

import java.util.ArrayList;
import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public class ClickHouseArrayUtilTest {
    @Test
    public void testArrayToString() throws Exception {
        Assert.assertEquals(
            ClickHouseArrayUtil.arrayToString(new String[]{"a", "b"}),
            "['a','b']"
        );

        Assert.assertEquals(
            ClickHouseArrayUtil.arrayToString(new String[]{"a", "'b\t"}),
            "['a','\\'b\\t']"
        );

        Assert.assertEquals(
            ClickHouseArrayUtil.arrayToString(new Integer[]{21, 42}),
            "[21,42]"
        );

        Assert.assertEquals(
            ClickHouseArrayUtil.arrayToString(new int[]{21, 42}),
            "[21,42]"
        );

        Assert.assertEquals(
                ClickHouseArrayUtil.arrayToString(new double[]{0.1, 1.2}),
                "[0.1,1.2]"
        );

        Assert.assertEquals(
            ClickHouseArrayUtil.arrayToString(new char[]{'a', 'b'}),
            "['a','b']"
        );
    }

    @Test
    public void testCollectionToString() throws Exception {
        Assert.assertEquals(
                ClickHouseArrayUtil.toString(new ArrayList<Object>(Arrays.asList("a", "b"))),
                "['a','b']"
        );

        Assert.assertEquals(
                ClickHouseArrayUtil.toString(new ArrayList<Object>(Arrays.asList("a", "'b\t"))),
                "['a','\\'b\\t']"
        );

        Assert.assertEquals(
                ClickHouseArrayUtil.toString(new ArrayList<Object>(Arrays.asList(21, 42))),
                "[21,42]"
        );

        Assert.assertEquals(
                ClickHouseArrayUtil.toString(new ArrayList<Object>(Arrays.asList(21, 42))),
                "[21,42]"
        );

        Assert.assertEquals(
                ClickHouseArrayUtil.toString(new ArrayList<Object>(Arrays.asList(0.1, 1.2))),
                "[0.1,1.2]"
        );

        Assert.assertEquals(
                ClickHouseArrayUtil.toString(new ArrayList<Object>(Arrays.asList('a', 'b'))),
                "['a','b']"
        );
    }
}