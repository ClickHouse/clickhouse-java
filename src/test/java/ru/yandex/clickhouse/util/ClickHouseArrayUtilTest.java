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

        Assert.assertEquals(
            ClickHouseArrayUtil.arrayToString(new String[][]{{"a", "b"},{"c", "d"}}),
            "[['a','b'],['c','d']]"
        );

        Assert.assertEquals(
            ClickHouseArrayUtil.arrayToString(new String[][]{{"a", "'b\t"},{"c", "'d\t"}}),
            "[['a','\\'b\\t'],['c','\\'d\\t']]"
        );

        Assert.assertEquals(
            ClickHouseArrayUtil.arrayToString(new Integer[][]{{21, 42},{63, 84}}),
            "[[21,42],[63,84]]"
        );

        Assert.assertEquals(
            ClickHouseArrayUtil.arrayToString(new double[][]{{0.1, 1.2}, {0.2, 2.2}}),
            "[[0.1,1.2],[0.2,2.2]]"
        );

        Assert.assertEquals(
            ClickHouseArrayUtil.arrayToString(new int[][]{{1, 2}, {3, 4}}),
            "[[1,2],[3,4]]"
        );

        Assert.assertEquals(
            ClickHouseArrayUtil.arrayToString(new char[][]{{'a', 'b'}, {'c', 'd'}}),
            "[['a','b'],['c','d']]"
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

        ArrayList<Object> arrayOfArrays = new ArrayList<Object>();
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList(1, 2)));
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList(3, 4)));
        Assert.assertEquals(
            ClickHouseArrayUtil.toString(arrayOfArrays),
            "[[1,2],[3,4]]"
        );

        arrayOfArrays = new ArrayList<Object>();
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList(1.1, 2.4)));
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList(3.9, 4.16)));
        Assert.assertEquals(
            ClickHouseArrayUtil.toString(arrayOfArrays),
            "[[1.1,2.4],[3.9,4.16]]"
        );

        arrayOfArrays = new ArrayList<Object>();
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList("a", "b")));
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList("c", "'d\t")));
        Assert.assertEquals(
            ClickHouseArrayUtil.toString(arrayOfArrays),
            "[['a','b'],['c','\\'d\\t']]"
        );

        arrayOfArrays = new ArrayList<Object>();
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList('a', 'b')));
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList('c', 'd')));
        Assert.assertEquals(
            ClickHouseArrayUtil.toString(arrayOfArrays),
            "[['a','b'],['c','d']]"
        );
    }
}
