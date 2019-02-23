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

        // https://github.com/yandex/clickhouse-jdbc/issues/283
        Assert.assertEquals(
                ClickHouseArrayUtil.arrayToString(new String[]{"\\xEF\\xBC", "\\x3C\\x22"}), // quote == true
                "['\\\\xEF\\\\xBC','\\\\x3C\\\\x22']"
        );
        Assert.assertEquals(
                ClickHouseArrayUtil.arrayToString(new String[]{"\\xEF\\xBC", "\\x3C\\x22"}, false),
                "['\\xEF\\xBC','\\x3C\\x22']"
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
                ClickHouseArrayUtil.toString(new String[] {"a", "b"}),
                "['a','b']"
        );

        Assert.assertEquals(
                ClickHouseArrayUtil.toString(new String[] {"a", "'b\t"}),
                "['a','\\'b\\t']"
        );

        // https://github.com/yandex/clickhouse-jdbc/issues/283
        Assert.assertEquals(
                ClickHouseArrayUtil.toString(new String[] {"\\xEF\\xBC", "\\x3C\\x22"}), // quote == true
                "['\\\\xEF\\\\xBC','\\\\x3C\\\\x22']"
        );
        Assert.assertEquals(
                ClickHouseArrayUtil.toString(new String[] {"\\xEF\\xBC", "\\x3C\\x22"}, false),
                "['\\xEF\\xBC','\\x3C\\x22']"
        );

        Assert.assertEquals(
                ClickHouseArrayUtil.toString(new Integer[] {21, 42}),
                "[21,42]"
        );

        Assert.assertEquals(
                ClickHouseArrayUtil.toString(new Integer[] {21, 42}),
                "[21,42]"
        );

        Assert.assertEquals(
                ClickHouseArrayUtil.toString(new Double[] {0.1, 1.2}),
                "[0.1,1.2]"
        );

        Assert.assertEquals(
                ClickHouseArrayUtil.toString(new Character[]{'a', 'b'}),
                "['a','b']"
        );

        ArrayList<Object> arrayOfArrays = new ArrayList<Object>();
        arrayOfArrays.add(new Integer[] {1, 2});
        arrayOfArrays.add(new Integer[] {3, 4});
        Assert.assertEquals(
            ClickHouseArrayUtil.toString(arrayOfArrays.toArray()),
            "[[1,2],[3,4]]"
        );

        arrayOfArrays = new ArrayList<Object>();
        arrayOfArrays.add(new Double[] {1.1, 2.4});
        arrayOfArrays.add(new Double[] {3.9, 4.16});
        Assert.assertEquals(
            ClickHouseArrayUtil.toString(arrayOfArrays.toArray()),
            "[[1.1,2.4],[3.9,4.16]]"
        );

        arrayOfArrays = new ArrayList<Object>();
        arrayOfArrays.add(new String[] {"a", "b"});
        arrayOfArrays.add(new String[] {"c", "'d\t"});
        Assert.assertEquals(
            ClickHouseArrayUtil.toString(arrayOfArrays.toArray()),
            "[['a','b'],['c','\\'d\\t']]"
        );

        arrayOfArrays = new ArrayList<Object>();
        arrayOfArrays.add(new Character[] {'a', 'b'});
        arrayOfArrays.add(new Character[] {'c', 'd'});
        Assert.assertEquals(
            ClickHouseArrayUtil.toString(arrayOfArrays.toArray()),
            "[['a','b'],['c','d']]"
        );

        Assert.assertEquals(
            ClickHouseArrayUtil.toString(new Integer[] {21, null}),
            "[21,NULL]"
        );

        Assert.assertEquals(
            ClickHouseArrayUtil.toString(new Integer[] {null, 42}),
            "[NULL,42]"
        );

        Assert.assertEquals(
            ClickHouseArrayUtil.toString(new String[] {"a", null}),
            "['a',NULL]"
        );

        Assert.assertEquals(
            ClickHouseArrayUtil.toString(new String[] {null, "b"}),
            "[NULL,'b']"
        );

        arrayOfArrays = new ArrayList<Object>();
        arrayOfArrays.add(new Character[] {null, 'b'});
        arrayOfArrays.add(new Character[] {'c', 'd'});
        Assert.assertEquals(
            ClickHouseArrayUtil.toString(arrayOfArrays.toArray()),
            "[[NULL,'b'],['c','d']]"
        );

        arrayOfArrays = new ArrayList<Object>();
        arrayOfArrays.add(new Character[] {null, 'b'});
        arrayOfArrays.add(new Character[] {'c', null});
        Assert.assertEquals(
            ClickHouseArrayUtil.toString(arrayOfArrays.toArray()),
            "[[NULL,'b'],['c',NULL]]"
        );
    }
}
