package ru.yandex.clickhouse;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by zhur on 14/03/16.
 */
public class CHDataSourceTest {
    @Test
    public void testConstructor() throws Exception {
        CHDataSource ds = new CHDataSource("jdbc:clickhouse://localhost:1234/ppc");
        assertEquals("localhost", ds.getHost());
        assertEquals(1234, ds.getPort());
        assertEquals("ppc", ds.getDatabase());

        CHDataSource ds2 = new CHDataSource("jdbc:clickhouse://clh.company.com:5324");
        assertEquals("clh.company.com", ds2.getHost());
        assertEquals(5324, ds2.getPort());
        assertEquals("default", ds2.getDatabase());

        try {
            new CHDataSource(null);
            fail("CHDataSource with null argument must fail");
        } catch (IllegalArgumentException ex) {
            // pass, it's ok
        }

        try {
            new CHDataSource("jdbc:mysql://localhost:2342");
            fail("CHDataSource with incorrect args must fail");
        } catch (IllegalArgumentException ex) {
            // pass, it's ok
        }

        try {
            new CHDataSource("jdbc:clickhouse://localhost:wer");
            fail("CHDataSource with incorrect args must fail");
        } catch (IllegalArgumentException ex) {
            // pass, it's ok
        }
    }

}