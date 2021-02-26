package ru.yandex.clickhouse.response.parser;

import java.util.UUID;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.response.ByteFragment;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

public class ClickHouseUUIDParserTest {

    private ClickHouseValueParser<UUID> parser;

    @BeforeClass
    public void setUp() throws Exception {
        parser = ClickHouseValueParser.getParser(UUID.class);
    }

    @Test
    public void testNullValue() throws Exception {
        assertNull(parser.parse(
            ByteFragment.fromString("\\N"), null, null));
    }

    @Test
    public void testEmptyValue() throws Exception {
        assertNull(parser.parse(
            ByteFragment.fromString(""), null, null));
    }

    @Test
    public void testSimpleUUID() throws Exception {
        UUID uuid = UUID.randomUUID();
        assertEquals(
            parser.parse(
                ByteFragment.fromString(uuid.toString()), null, null),
            uuid);
        assertNotEquals(
            parser.parse(
                ByteFragment.fromString(uuid.toString()), null, null),
            UUID.randomUUID());
    }

    @Test
    public void testBrokenUUID() throws Exception {
        try {
            parser.parse(
                ByteFragment.fromString("BROKEN"), null, null);
            fail();
        } catch (ClickHouseException che) {
            // expected
        }
    }

}
