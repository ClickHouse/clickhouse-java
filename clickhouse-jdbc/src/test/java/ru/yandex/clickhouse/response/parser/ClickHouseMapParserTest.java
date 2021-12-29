package ru.yandex.clickhouse.response.parser;

import java.math.BigInteger;
import java.util.Map;
import java.util.TimeZone;

import org.testng.annotations.Test;

import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;
import ru.yandex.clickhouse.util.Utils;

import static org.testng.Assert.assertEquals;

public class ClickHouseMapParserTest {
    @Test(groups = "unit")
    public void testReadPart() throws Exception {
        ClickHouseMapParser parser = ClickHouseMapParser.getInstance();
        String s;
        StringBuilder sb = new StringBuilder();
        int index = parser.readPart(ClickHouseDataType.String, s = " '' ", 0, s.length(), sb, ':');
        assertEquals(sb.toString(), "");

        sb.setLength(0);
        index = parser.readPart(ClickHouseDataType.String, s = "''", 0, s.length(), sb, ':');
        assertEquals(sb.toString(), "");

        sb.setLength(0);
        index = parser.readPart(ClickHouseDataType.String, s = "'':123", 0, s.length(), sb, ':');
        assertEquals(sb.toString(), "");

        sb.setLength(0);
        index = parser.readPart(ClickHouseDataType.String, s = "''}", 0, s.length(), sb, ':');
        assertEquals(sb.toString(), "");

        sb.setLength(0);
        index = parser.readPart(ClickHouseDataType.String, s = " '1''\\'' : 1", 0, s.length(), sb, ':');
        assertEquals(sb.toString(), "1''\\'");

        sb.setLength(0);
        index = parser.readPart(ClickHouseDataType.Int16, s = " 123 ", 0, s.length(), sb, ':');
        assertEquals(sb.toString(), s.trim());

        sb.setLength(0);
        index = parser.readPart(ClickHouseDataType.Int16, s = "123", 0, s.length(), sb, ':');
        assertEquals(sb.toString(), s.trim());

        sb.setLength(0);
        index = parser.readPart(ClickHouseDataType.Int16, s = "123:'aa'", 0, s.length(), sb, ':');
        assertEquals(sb.toString(), "123");

        sb.setLength(0);
        index = parser.readPart(ClickHouseDataType.Int16, s = "123}", 0, s.length(), sb, ':');
        assertEquals(sb.toString(), "123");

        // now complex data types like Array, Tuple and Map
        sb.setLength(0);
        index = parser.readPart(ClickHouseDataType.Array, s = "[]", 0, s.length(), sb, ',');
        assertEquals(sb.toString(), s);

        sb.setLength(0);
        index = parser.readPart(ClickHouseDataType.Array, s = "[1,2,3]", 0, s.length(), sb, ',');
        assertEquals(sb.toString(), s);

        sb.setLength(0);
        index = parser.readPart(ClickHouseDataType.Array, s = "[1,2,3],", 0, s.length(), sb, ',');
        assertEquals(sb.toString(), "[1,2,3]");

        sb.setLength(0);
        index = parser.readPart(ClickHouseDataType.Tuple, s = "()", 0, s.length(), sb, ',');
        assertEquals(sb.toString(), s);

        sb.setLength(0);
        index = parser.readPart(ClickHouseDataType.Tuple, s = "(1,2,3)", 0, s.length(), sb, ',');
        assertEquals(sb.toString(), s);

        sb.setLength(0);
        index = parser.readPart(ClickHouseDataType.Tuple, s = "(1,2,3),", 0, s.length(), sb, ',');
        assertEquals(sb.toString(), "(1,2,3)");

        sb.setLength(0);
        index = parser.readPart(ClickHouseDataType.Map, s = "{}", 0, s.length(), sb, ',');
        assertEquals(sb.toString(), s);

        sb.setLength(0);
        index = parser.readPart(ClickHouseDataType.Map, s = "{1:2,3:4}", 0, s.length(), sb, ',');
        assertEquals(sb.toString(), s);

        sb.setLength(0);
        index = parser.readPart(ClickHouseDataType.Map, s = "{1:2,3:4},", 0, s.length(), sb, ',');
        assertEquals(sb.toString(), "{1:2,3:4}");

        // mixed
        sb.setLength(0);
        index = parser.readPart(ClickHouseDataType.Array, s = "['a,b', '[1,2]']", 0, s.length(), sb, ',');
        assertEquals(sb.toString(), s);

        sb.setLength(0);
        index = parser.readPart(ClickHouseDataType.Array, s = "[[[[[]]]]]", 0, s.length(), sb, ',');
        assertEquals(sb.toString(), s);
    }

    @Test(groups = "unit")
    public void testParse() throws Exception {
        ClickHouseMapParser parser = ClickHouseMapParser.getInstance();
        Map<?, ?> result = parser.parse(ByteFragment.fromString("{'a': 1, 'a''\\\\\\'b':2}"),
                ClickHouseColumnInfo.parse("Map(String, Int8)", "dunno", TimeZone.getTimeZone("Asia/Chongqing")),
                TimeZone.getDefault());
        assertEquals(result, Utils.mapOf("a", 1, "a''\\'b", 2));

        result = parser.parse(ByteFragment.fromString("{'a': '1', 'a''\\\\\\'b':'1''\\\\\\'2'}"),
                ClickHouseColumnInfo.parse("Map(String, String)", "dunno", TimeZone.getTimeZone("Asia/Chongqing")),
                TimeZone.getDefault());
        assertEquals(result, Utils.mapOf("a", "1", "a''\\'b", "1''\\'2"));

        result = parser.parse(ByteFragment.fromString("{123: '1a1', -456:'331'}"),
                ClickHouseColumnInfo.parse("Map(Int16, String)", "dunno", TimeZone.getTimeZone("Asia/Chongqing")),
                TimeZone.getDefault());
        assertEquals(result, Utils.mapOf(123, "1a1", -456, "331"));

        result = parser.parse(
                ByteFragment
                        .fromString("{123: 1111111111111111111111111111111111111, -456:222222222222222222222222222}"),
                ClickHouseColumnInfo.parse("Map(Int16, UInt256)", "dunno", TimeZone.getTimeZone("Asia/Chongqing")),
                TimeZone.getDefault());
        assertEquals(result, Utils.mapOf(123, new BigInteger("1111111111111111111111111111111111111"), -456,
                new BigInteger("222222222222222222222222222")));
    }
}
