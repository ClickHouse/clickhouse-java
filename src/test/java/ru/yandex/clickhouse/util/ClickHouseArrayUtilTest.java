package ru.yandex.clickhouse.util;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 24/09/16
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
            ClickHouseArrayUtil.arrayToString(new char[]{'a', 'b'}),
            "['a','b']"
        );
    }

}